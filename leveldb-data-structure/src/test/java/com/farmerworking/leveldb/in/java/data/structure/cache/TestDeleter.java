package com.farmerworking.leveldb.in.java.data.structure.cache;

import com.farmerworking.leveldb.in.java.api.Deleter;

import java.util.Vector;

public class TestDeleter<V> implements Deleter<V> {
    public Vector<Integer> deletedKeys = new Vector<>();
    public Vector<V> deletedValues = new Vector<>();


    @Override
    public void delete(String key, V value) {
        deletedKeys.add(CacheTest.decodeKey(key));
        deletedValues.add(value);
    }
}
