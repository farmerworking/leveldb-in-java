package com.farmerworking.leveldb.in.java.data.structure.block;

import com.farmerworking.leveldb.in.java.api.Iterator;
import com.farmerworking.leveldb.in.java.api.Status;

public class EmptyIterator implements Iterator {
    private Status status;

    public EmptyIterator(Status status) {
        this.status = status;
    }

    public EmptyIterator() {
        this.status = Status.OK();
    }

    @Override
    public boolean valid() {
        return false;
    }

    @Override
    public void seekToFirst() {
    }

    @Override
    public void seekToLast() {
    }

    @Override
    public void seek(String target) {
    }

    @Override
    public void next() {
        assert(false);
    }

    @Override
    public void prev() {
        assert(false);
    }

    @Override
    public String key() {
        assert(false);
        return null;
    }

    @Override
    public String value() {
        assert(false);
        return null;
    }

    @Override
    public Status status() {
        return status;
    }
}
