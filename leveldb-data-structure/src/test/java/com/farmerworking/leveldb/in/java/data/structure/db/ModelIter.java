package com.farmerworking.leveldb.in.java.data.structure.db;

import java.util.Map;
import java.util.SortedMap;

import com.farmerworking.leveldb.in.java.api.Iterator;
import com.farmerworking.leveldb.in.java.api.Status;

public class ModelIter implements Iterator<String, String> {
    private SortedMap<String, String> map;
    private java.util.Iterator<String> keyIterator;
    private String key;

    public ModelIter(SortedMap<String, String> map) {
        this.map = map;
    }

    @Override
    public boolean valid() {
        return keyIterator != null;
    }

    @Override
    public void seekToFirst() {
        this.keyIterator = map.keySet().iterator();
        if (valid()) {
            next();
        }
    }

    @Override
    public void seekToLast() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void seek(String target) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void next() {
        if (keyIterator.hasNext()) {
            key = keyIterator.next();
        } else {
            keyIterator = null;
        }
    }

    @Override
    public void prev() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public String value() {
        return this.map.get(this.key);
    }

    @Override
    public Status status() {
        return Status.OK();
    }

    @Override
    public void close() {

    }

    @Override
    public void registerCleanup(Runnable runnable) {

    }
}
