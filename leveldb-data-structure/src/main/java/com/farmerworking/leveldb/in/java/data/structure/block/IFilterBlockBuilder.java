package com.farmerworking.leveldb.in.java.data.structure.block;

public interface IFilterBlockBuilder {
    void startBlock(long blockOffset);

    void addKey(String key);

    String finish();
}
