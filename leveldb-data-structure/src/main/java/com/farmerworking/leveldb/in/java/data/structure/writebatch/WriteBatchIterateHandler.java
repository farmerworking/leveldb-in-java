package com.farmerworking.leveldb.in.java.data.structure.writebatch;

public interface WriteBatchIterateHandler {
    void put(String key, String value);

    void delete(String key);
}
