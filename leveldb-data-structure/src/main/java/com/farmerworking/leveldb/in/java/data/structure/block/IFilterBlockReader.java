package com.farmerworking.leveldb.in.java.data.structure.block;

public interface IFilterBlockReader {
    boolean keyMayMatch(long blockOffset, String key);
}
