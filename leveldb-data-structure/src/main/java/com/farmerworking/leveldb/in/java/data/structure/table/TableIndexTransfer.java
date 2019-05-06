package com.farmerworking.leveldb.in.java.data.structure.table;

import com.farmerworking.leveldb.in.java.api.*;
import com.farmerworking.leveldb.in.java.common.ICoding;
import com.farmerworking.leveldb.in.java.data.structure.block.EmptyIterator;
import com.farmerworking.leveldb.in.java.data.structure.block.IBlockReader;
import com.farmerworking.leveldb.in.java.data.structure.two.level.iterator.IndexTransfer;
import com.farmerworking.leveldb.in.java.file.RandomAccessFile;
import javafx.util.Pair;

public class TableIndexTransfer implements IndexTransfer {
    private final Options options;
    private final RandomAccessFile file;
    private final long cacheId;
    private final Deleter deleter;

    public TableIndexTransfer(RandomAccessFile file, Options options, long cacheId, Deleter deleter) {
        this.file = file;
        this.options = options;
        this.cacheId = cacheId;
        this.deleter = deleter;
    }

    @Override
    public Iterator<String, String> transfer(ReadOptions options, String value) {
        BlockHandle handle = new BlockHandle();
        Pair<Status, Integer> decodeStatus = handle.decodeFrom(value.toCharArray(), 0);
        Status status = decodeStatus.getKey();

        IBlockReader blockReader = null;
        CacheHandle cacheHandle = null;
        if (status.isOk()) {
            String cacheKey = null;
            if (this.options.getBlockCache() != null) {
                cacheKey = buildCacheKey(handle.getOffset());
                cacheHandle = this.options.getBlockCache().lookup(cacheKey);

                if (cacheHandle != null) {
                    blockReader = (IBlockReader) this.options.getBlockCache().value(cacheHandle);
                }
            }

            if (blockReader == null) { // no cache or cache miss
                Pair<Status, String> pair = TableReader.readBlock(file, options, handle);
                status = pair.getKey();
                if (status.isOk()) {
                    blockReader = IBlockReader.getDefaultImpl(pair.getValue());

                    if (this.options.getBlockCache() != null) {
                        cacheHandle = this.options.getBlockCache().insert(cacheKey, blockReader, blockReader.memoryUsage(), deleter);
                    }
                }
            }
        }

        Iterator<String, String> iter = null;
        if (blockReader != null) {
            iter = blockReader.iterator(this.options.getComparator());
            if (cacheHandle != null) {
                iter.registerCleanup(new CacheHandleReleaser(cacheHandle, this.options.getBlockCache()));
            }
        } else {
            iter = new EmptyIterator(status);
        }

        return iter;
    }

    class CacheHandleReleaser implements Runnable {
        private final Cache cache;
        private final CacheHandle cacheHandle;

        public CacheHandleReleaser(CacheHandle cacheHandle, Cache cache) {
            this.cacheHandle = cacheHandle;
            this.cache = cache;
        }

        @Override
        public void run() {
            cache.release(cacheHandle);
        }
    }

    private String buildCacheKey(long offset) {
        StringBuilder builder = new StringBuilder();
        ICoding.getInstance().putFixed64(builder, this.cacheId);
        ICoding.getInstance().putFixed64(builder, offset);
        return builder.toString();
    }
}
