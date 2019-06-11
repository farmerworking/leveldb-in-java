package com.farmerworking.leveldb.in.java.data.structure.cache;

import com.farmerworking.leveldb.in.java.api.*;
import com.farmerworking.leveldb.in.java.common.ICoding;
import com.farmerworking.leveldb.in.java.data.structure.block.EmptyIterator;
import com.farmerworking.leveldb.in.java.data.structure.table.CacheHandleReleaser;
import com.farmerworking.leveldb.in.java.data.structure.table.ITableReader;
import com.farmerworking.leveldb.in.java.file.FileName;
import com.farmerworking.leveldb.in.java.file.RandomAccessFile;
import javafx.util.Pair;

public class TableCache {
    final Options options;
    private final String dbname;
    ShardedLRUCache<Pair<RandomAccessFile, ITableReader>> cache;

    private int hit;
    private int miss;

    public TableCache(String dbname, Options options, int entries) {
        this.dbname = dbname;
        this.options = options;
        this.cache = new ShardedLRUCache<>(entries);

        this.hit = 0;
        this.miss = 0;
    }

    public Pair<Status, Pair<String, String>> get(ReadOptions readOptions, long fileNumber, long fileSize, String key) {
        Pair<Status, CacheHandle<Pair<RandomAccessFile, ITableReader>>> pair = findTable(fileNumber, fileSize);

        if (pair.getKey().isOk()) {
            ITableReader tableReader = cache.value(pair.getValue()).getValue();
            Pair<Status, Pair<String, String>> getPair = tableReader.internalGet(readOptions, key);
            cache.release(pair.getValue());
            return getPair;
        }

        return new Pair<>(pair.getKey(), null);
    }

    public void evict(long fileNumber) {
        String cacheKey = cacheKey(fileNumber);
        cache.erase(cacheKey);
    }

    public Iterator<String, String> iterator(ReadOptions readOptions, long fileNumber, long fileSize) {
        Pair<Status, CacheHandle<Pair<RandomAccessFile, ITableReader>>> pair = findTable(fileNumber, fileSize);
        if (pair.getKey().isNotOk()) {
            return new EmptyIterator(pair.getKey());
        } else {
            ITableReader tableReader = cache.value(pair.getValue()).getValue();
            Iterator<String, String> iter = tableReader.iterator(readOptions);
            iter.registerCleanup(new CacheHandleReleaser(pair.getValue(), cache));
            return iter;
        }
    }

    public Pair<Integer, Integer> stat() {
        return new Pair<>(hit, miss);
    }

    private String cacheKey(long fileNumber) {
        StringBuilder builder = new StringBuilder();
        ICoding.getInstance().putFixed64(builder, fileNumber);
        return builder.toString();
    }

    Pair<Status, CacheHandle<Pair<RandomAccessFile, ITableReader>>> findTable(long fileNumber, long fileSize) {
        String cacheKey = cacheKey(fileNumber);
        CacheHandle<Pair<RandomAccessFile, ITableReader>> handle = cache.lookup(cacheKey);

        if (handle != null) {
            hit ++;
            return new Pair<>(Status.OK(), handle);
        } else {
            miss ++;
            String tableFileName = FileName.tableFileName(dbname, fileNumber);
            Pair<Status, RandomAccessFile> pair = this.options.getEnv().newRandomAccessFile(tableFileName);

            Status status = pair.getKey();
            if (status.isNotOk()) {
                tableFileName = FileName.SSTTableFileName(dbname, fileNumber);
                pair = this.options.getEnv().newRandomAccessFile(tableFileName);

                if (pair.getKey().isOk()) {
                    status = pair.getKey();
                }
            }

            ITableReader tableReader = null;
            if (status.isOk()) {
                tableReader = newTableReader();
                status = tableReader.open(this.options, pair.getValue(), fileSize);
            }

            if (status.isNotOk()) {
                // We do not cache error results so that if the error is transient,
                // or somebody repairs the file, we recover automatically.
                return new Pair<>(status, null);
            } else {
                handle = cache.insert(cacheKey, new Pair<>(pair.getValue(), tableReader), 1, null);
                return new Pair<>(status, handle);
            }
        }
    }

    ITableReader newTableReader() {
        return ITableReader.getDefaultImpl();
    }
}
