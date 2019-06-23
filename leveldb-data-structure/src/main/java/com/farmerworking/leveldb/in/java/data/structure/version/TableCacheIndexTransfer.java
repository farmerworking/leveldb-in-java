package com.farmerworking.leveldb.in.java.data.structure.version;

import com.farmerworking.leveldb.in.java.api.Iterator;
import com.farmerworking.leveldb.in.java.api.ReadOptions;
import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.data.structure.block.EmptyIterator;
import com.farmerworking.leveldb.in.java.data.structure.cache.TableCache;
import com.farmerworking.leveldb.in.java.data.structure.two.level.iterator.IndexTransfer;
import javafx.util.Pair;

import java.nio.charset.StandardCharsets;

public class TableCacheIndexTransfer implements IndexTransfer<Pair<Long, Long>> {
    private final TableCache tableCache;

    public TableCacheIndexTransfer(TableCache tableCache) {
        this.tableCache = tableCache;
    }

    @Override
    public Iterator<String, String> transfer(ReadOptions options, Pair<Long, Long> value) {
        if (value == null || value.getKey() == null || value.getValue() == null) {
            return new EmptyIterator(
                    Status.Corruption("FileReader invoked with unexpected value"));
        } else {
            return tableCache.iterator(options, value.getKey(), value.getValue());
        }
    }
}
