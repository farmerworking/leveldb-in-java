package com.farmerworking.leveldb.in.java.data.structure.table;

import com.farmerworking.leveldb.in.java.api.CompressionType;
import com.farmerworking.leveldb.in.java.api.Options;
import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.common.ByteUtils;
import com.farmerworking.leveldb.in.java.common.ICRC32C;
import com.farmerworking.leveldb.in.java.common.ICoding;
import com.farmerworking.leveldb.in.java.data.structure.block.IBlockBuilder;
import com.farmerworking.leveldb.in.java.data.structure.block.IFilterBlockBuilder;
import com.farmerworking.leveldb.in.java.file.WritableFile;

public class TableBuilder implements ITableBuilder {
    public static int kBlockTrailerSize = 1 + ICoding.getInstance().getFixed32Length();

    private WritableFile file;
    private boolean closed;
    private char[] lastKey;
    private long numEntries;
    private Status status;
    private boolean pendingIndexEntry;
    private BlockHandle pendingHandle;
    private long offset;

    /**
     * create using options
     */
    private IBlockBuilder dataBlock;
    private IBlockBuilder indexBlock;
    private IFilterBlockBuilder filterBlockBuilder;

    /**
     * copy from options
     */
    private Options options;
    private Options indexBlockOptions;

    public TableBuilder(Options options, WritableFile file) {
        this.options = new Options(options);
        this.indexBlockOptions = new Options(options);
        this.indexBlockOptions.setBlockRestartInterval(1);

        this.dataBlock = IBlockBuilder.getDefaultImpl(this.options);
        this.indexBlock= IBlockBuilder.getDefaultImpl(this.indexBlockOptions);
        this.filterBlockBuilder = IFilterBlockBuilder.getDefaultImpl(options.getFilterPolicy());

        this.file = file;
        this.closed = false;
        this.lastKey = null;
        this.numEntries = 0;
        this.pendingIndexEntry = false;
        this.status = Status.OK();
        this.pendingHandle = new BlockHandle();
        this.offset = 0;
    }

    @Override
    public Status changeOptions(Options options) {
        // Note: if more fields are added to Options, update
        // this function to catch changes that should not be allowed to
        // change in the middle of building a Table.
        if (options.getComparator() != this.options.getComparator()) {
            return Status.InvalidArgument("changing comparator while building table");
        }

        // Note that any live BlockBuilders point to rep_->options and therefore
        // will automatically pick up the updated options.
        this.options = new Options(options);
        this.indexBlockOptions = new Options(options);
        this.indexBlockOptions.setBlockRestartInterval(1);
        return Status.OK();
    }

    @Override
    public void add(String key, String value) {
        assert !this.closed;
        if (!ok()) return;
        char[] keyChars = key.toCharArray();
        if (this.numEntries > 0) {
            assert this.options.getComparator().compare(keyChars, lastKey) > 0;
        }

        if (this.pendingIndexEntry) {
            assert this.dataBlock.isEmpty();
            char[] seperator = this.options.getComparator().findShortestSeparator(lastKey, keyChars);
            StringBuilder builder = new StringBuilder();
            pendingHandle.encodeTo(builder);
            this.indexBlock.add(new String(seperator), builder.toString());
            this.pendingIndexEntry = false;
        }

        if (this.filterBlockBuilder != null) {
            this.filterBlockBuilder.addKey(key);
        }

        this.lastKey = keyChars;
        this.numEntries ++;
        this.dataBlock.add(key, value);

        if (this.dataBlock.memoryUsage() >= this.options.getBlockSize()) {
            flush();
        }
    }

    @Override
    public void flush() {
        assert !this.closed;
        if (!ok()) return;
        if (this.dataBlock.isEmpty()) return;
        assert !this.pendingIndexEntry;
        writeBlock(this.dataBlock, this.pendingHandle);

        if (ok()) {
            this.pendingIndexEntry = true;
            this.status = this.file.flush();
        }

        if (this.filterBlockBuilder != null) {
            this.filterBlockBuilder.startBlock(offset);
        }
    }

    // File format contains a sequence of blocks where each block has:
    //    block_data: uint8[n]
    //    type: uint8
    //    crc: uint32
    private void writeBlock(IBlockBuilder dataBlock, BlockHandle handle) {
        assert ok();
        // todo: support snappy compression
        writeRawBlock(dataBlock.finish(), CompressionType.kNoCompression, handle);
        dataBlock.reset();
    }

    private void writeRawBlock(String blockContent, CompressionType type, BlockHandle handle) {
        handle.setOffset(offset);
        handle.setSize((long) blockContent.length());
        this.status = this.file.append(blockContent);

        if (ok()) {
            char[] trailer = new char[kBlockTrailerSize];
            trailer[0] = (char) type.getValue();
            byte[] bytes = ByteUtils.toByteArray(blockContent, 0, blockContent.length());
            int crc = ICRC32C.getInstance().value(bytes, 0, bytes.length);
            crc = ICRC32C.getInstance().extend(crc, new byte[]{(byte) type.getValue()}, 0, 1);
            crc = ICRC32C.getInstance().mask(crc);
            ICoding.getInstance().encodeFixed32(trailer, 1, crc);
            this.status = this.file.append(new String(trailer));

            if (ok()) {
                this.offset += blockContent.length() + kBlockTrailerSize;
            }
        }
    }

    @Override
    public Status status() {
        return status;
    }

    @Override
    public Status finish() {
        flush();
        assert !this.closed;
        this.closed = true;

        BlockHandle filterBlockHandle = new BlockHandle();
        BlockHandle metaIndexBlockHandle = new BlockHandle();
        BlockHandle indexBlockHandle = new BlockHandle();

        if (ok() && this.filterBlockBuilder != null) {
            writeRawBlock(this.filterBlockBuilder.finish(), CompressionType.kNoCompression, filterBlockHandle);
        }

        if (ok()) {
            IBlockBuilder metaIndexBlock = IBlockBuilder.getDefaultImpl(this.options);
            if (this.filterBlockBuilder != null) {
                // Add mapping from "filter.Name" to location of filter data
                String key = "filter." + this.options.getFilterPolicy().name();

                StringBuilder builder = new StringBuilder();
                filterBlockHandle.encodeTo(builder);
                metaIndexBlock.add(key, builder.toString());
            }

            writeBlock(metaIndexBlock, metaIndexBlockHandle);
        }

        if (ok()) {
            if (this.pendingIndexEntry) {
                char[] successor = this.options.getComparator().findShortSuccessor(lastKey);
                StringBuilder builder = new StringBuilder();
                pendingHandle.encodeTo(builder);
                this.indexBlock.add(new String(successor), builder.toString());
                this.pendingIndexEntry = false;
            }

            writeBlock(this.indexBlock, indexBlockHandle);
        }

        if (ok()) {
            Footer footer = new Footer();
            footer.setMetaIndexBlockHandle(metaIndexBlockHandle);
            footer.setIndexBlockHandle(indexBlockHandle);

            StringBuilder builder = new StringBuilder();
            footer.encodeTo(builder);
            this.status = this.file.append(builder.toString());
            if (ok()) {
                this.offset += builder.length();
            }
        }

        return this.status;
    }

    @Override
    public void abandon() {
        assert !this.closed;
        this.closed = true;
    }

    @Override
    public long numEntries() {
        return numEntries;
    }

    @Override
    public long fileSize() {
        return offset;
    }

    private boolean ok() {
        return status.isOk();
    }
}
