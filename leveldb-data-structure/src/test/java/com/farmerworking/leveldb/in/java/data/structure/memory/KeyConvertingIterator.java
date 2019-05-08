package com.farmerworking.leveldb.in.java.data.structure.memory;

import com.farmerworking.leveldb.in.java.api.Iterator;
import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.data.structure.iterator.AbstractIterator;

public class KeyConvertingIterator extends AbstractIterator<String, String> {
    private final Iterator<InternalKey, String> iterator;

    public KeyConvertingIterator(Iterator<InternalKey, String> iterator) {
        this.iterator = iterator;
    }

    @Override
    public boolean valid() {
        assert !this.closed;
        return iterator.valid();
    }

    @Override
    public void seekToFirst() {
        assert !this.closed;
        iterator.seekToFirst();
    }

    @Override
    public void seekToLast() {
        assert !this.closed;
        iterator.seekToLast();
    }

    @Override
    public void seek(String target) {
        assert !this.closed;
        iterator.seek(target);
    }

    @Override
    public void next() {
        assert !this.closed;
        iterator.next();
    }

    @Override
    public void prev() {
        assert !this.closed;
        iterator.prev();
    }

    @Override
    public String key() {
        assert !this.closed;
        return iterator.key().userKey;
    }

    @Override
    public String value() {
        assert !this.closed;
        return iterator.value();
    }

    @Override
    public Status status() {
        assert !this.closed;
        return iterator.status();
    }
}
