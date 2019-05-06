package com.farmerworking.leveldb.in.java.data.structure.table;

import com.farmerworking.leveldb.in.java.api.*;
import com.farmerworking.leveldb.in.java.common.ByteUtils;
import com.farmerworking.leveldb.in.java.common.ICRC32C;
import com.farmerworking.leveldb.in.java.common.ICoding;
import com.farmerworking.leveldb.in.java.data.structure.block.IBlockReader;
import com.farmerworking.leveldb.in.java.data.structure.block.IFilterBlockReader;
import com.farmerworking.leveldb.in.java.data.structure.two.level.iterator.IndexTransfer;
import com.farmerworking.leveldb.in.java.data.structure.two.level.iterator.TwoLevelIterator;
import com.farmerworking.leveldb.in.java.file.RandomAccessFile;
import javafx.util.Pair;
import org.xerial.snappy.Snappy;

import java.io.IOException;


public class TableReader implements ITableReader {
    private Status status = Status.Corruption("init invalid status");
    private Options options;
    private RandomAccessFile file;
    private BlockHandle metaIndexHandle;
    private IBlockReader indexBlockReader;
    private IFilterBlockReader filter;
    private String filterData;
    private long cacheId;

    public Status open(Options options, RandomAccessFile file, long size) {
        if (size < Footer.kEncodedLength) {
            status = Status.Corruption("file is too short to be an sstable");
            return status;
        }

        Pair<Status, String> pair = file.read(size - Footer.kEncodedLength, Footer.kEncodedLength);
        status = pair.getKey();
        if (status.isNotOk()) {
            return status;
        }

        Footer footer = new Footer();
        status = footer.decodeFrom(pair.getValue().toCharArray());
        if (status.isNotOk()) {
            return status;
        }

        ReadOptions readOptions = new ReadOptions();
        if (options.isParanoidChecks()) {
            readOptions.setVerifyChecksums(true);
        }
        pair = readBlock(file, readOptions, footer.getIndexBlockHandle());
        status = pair.getKey();

        if (status.isOk()) {
            // We've successfully read the footer and the index block: we're ready to serve requests.
            this.options = options;
            this.file = file;
            this.metaIndexHandle = footer.getMetaIndexBlockHandle();
            this.indexBlockReader = IBlockReader.getDefaultImpl(pair.getValue());
            this.cacheId = options.getBlockCache() == null ? 0 : options.getBlockCache().newId();
            this.filterData = null;
            this.filter = null;
            this.readMeta();
        }

        return status;
    }

    @Override
    public Iterator<String, String> iterator(ReadOptions readOptions) {
        return new TwoLevelIterator(
                this.indexBlockReader.iterator(this.options.getComparator()),
                readOptions,
                new TableIndexTransfer(this.file, this.options, this.cacheId, null));
    }

    // for unit test only
    public Iterator<String, String> iterator(ReadOptions readOptions, Deleter deleter) {
        return new TwoLevelIterator(
                this.indexBlockReader.iterator(this.options.getComparator()),
                readOptions,
                new TableIndexTransfer(this.file, this.options, this.cacheId, deleter));
    }

    @Override
    public long approximateOffsetOf(String key) {
        Iterator<String, String> iter = this.indexBlockReader.iterator(this.options.getComparator());
        iter.seek(key);

        long result;
        if (iter.valid()) {
            BlockHandle blockHandle = new BlockHandle();
            Pair<Status, Integer> decodeStatus = blockHandle.decodeFrom(iter.value().toCharArray(), 0);
            if (decodeStatus.getKey().isOk()) {
                result = blockHandle.getOffset();
            } else {
                // Strange: we can't decode the block handle in the index block.
                // We'll just return the offset of the metaindex block, which is
                // close to the whole file size for this case.
                result = this.metaIndexHandle.getOffset();
            }
        } else {
            // key is past the last key in the file.  Approximate the offset
            // by returning the offset of the metaindex block (which is
            // right near the end of the file).
            result = this.metaIndexHandle.getOffset();
        }
        return result;
    }

    @Override
    public Status status() {
        return status;
    }

    private void readMeta() {
        if (this.options.getFilterPolicy() == null) {
            return; // Do not need any metadata
        }

        ReadOptions readOptions = new ReadOptions();
        if (this.options.isParanoidChecks()) {
            readOptions.setVerifyChecksums(true);
        }

        Pair<Status, String> pair = readBlock(this.file, readOptions, this.metaIndexHandle);
        if (pair.getKey().isNotOk()) {
            // Do not propagate errors since meta info is not needed for operation
            return;
        }

        IBlockReader iBlockReader = IBlockReader.getDefaultImpl(pair.getValue());
        Iterator<String, String> iter = iBlockReader.iterator(new BytewiseComparator());
        String filterName = "filter." + this.options.getFilterPolicy().name();
        iter.seek(filterName);
        if (iter.valid() && iter.key().equals(filterName)) {
            readFilter(iter.value());
        }
    }

    private void readFilter(String value) {
        BlockHandle filterHandle = new BlockHandle();
        Pair<Status, Integer> decodeStatus = filterHandle.decodeFrom(value.toCharArray(), 0);

        if (decodeStatus.getKey().isNotOk()) {
            return;
        }

        ReadOptions readOptions = new ReadOptions();
        if (this.options.isParanoidChecks()) {
            readOptions.setVerifyChecksums(true);
        }

        Pair<Status, String> pair = readBlock(file, readOptions, filterHandle);
        if (pair.getKey().isNotOk()) {
            return;
        }

        this.filterData = pair.getValue();
        this.filter = IFilterBlockReader.getDefaultImpl(this.options.getFilterPolicy(), this.filterData);
    }

    static Pair<Status, String> readBlock(RandomAccessFile file, ReadOptions options, BlockHandle blockHandle) {
        int size = blockHandle.getSize().intValue();
        Pair<Status, String> pair = file.read(blockHandle.getOffset(), size + TableBuilder.kBlockTrailerSize);
        if (pair.getKey().isNotOk()) {
            return pair;
        }

        if (pair.getValue().length() != size + TableBuilder.kBlockTrailerSize) {
            return new Pair<>(Status.Corruption("truncated block read"), null);
        }

        char[] chars = pair.getValue().toCharArray();
        if (options.isVerifyChecksums()) {
            int crc = ICRC32C.getInstance().unmask(ICoding.getInstance().decodeFixed32(chars, size + 1));
            byte[] bytes = ByteUtils.toByteArray(chars, 0, size + 1);
            int actual = ICRC32C.getInstance().value(bytes, 0, bytes.length);

            if (actual != crc) {
                return new Pair<>(Status.Corruption("block checksum mismatch"), null);
            }
        }

        CompressionType type = CompressionType.valueOf((int) chars[size]);
        switch (type) {
            case kNoCompression:
                return new Pair<>(Status.OK(), new String(chars, 0, size));
            case kSnappyCompression:
                byte[] bytes = ByteUtils.toByteArray(chars, 0, size);
                try {
                    byte[] uncompressedBytes = Snappy.uncompress(bytes);
                    return new Pair<>(Status.OK(), new String(ByteUtils.toCharArray(uncompressedBytes, 0, uncompressedBytes.length)));
                } catch (IOException e) {
                    return new Pair<>(Status.Corruption("corrupted compressed block contents"), null);
                }
            default:
                return new Pair<>(Status.Corruption("unknown compression type"), null);
        }
    }
}
