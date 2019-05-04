package com.farmerworking.leveldb.in.java.data.structure.memory;

import com.farmerworking.leveldb.in.java.api.Iterator;
import com.farmerworking.leveldb.in.java.api.Status;

class KeyConvertingIterator implements Iterator<String, String> {
    private final Iterator<InternalKey, String> iterator;

    public KeyConvertingIterator(Iterator<InternalKey, String> iterator) {
        this.iterator = iterator;
    }

    @Override
    public boolean valid() {
        return iterator.valid();
    }

    @Override
    public void seekToFirst() {
        iterator.seekToFirst();
    }

    @Override
    public void seekToLast() {
        iterator.seekToLast();
    }

    @Override
    public void seek(String target) {
        iterator.seek(target);
    }

    @Override
    public void next() {
        iterator.next();
    }

    @Override
    public void prev() {
        iterator.prev();
    }

    @Override
    public String key() {
        return iterator.key().userKey;
    }

    @Override
    public String value() {
        return iterator.value();
    }

    @Override
    public Status status() {
        return iterator.status();
    }
}
