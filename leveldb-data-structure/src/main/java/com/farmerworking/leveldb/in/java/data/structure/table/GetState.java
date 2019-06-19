package com.farmerworking.leveldb.in.java.data.structure.table;

public enum GetState {
    kNotFound,
    kFound,
    kDeleted,
    kCorrupt;
}
