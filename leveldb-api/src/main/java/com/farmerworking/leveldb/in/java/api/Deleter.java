package com.farmerworking.leveldb.in.java.api;

public interface Deleter<T> {
    void delete(String key, T value);
}
