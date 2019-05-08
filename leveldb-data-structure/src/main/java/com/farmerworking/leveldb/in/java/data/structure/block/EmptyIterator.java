package com.farmerworking.leveldb.in.java.data.structure.block;

import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.data.structure.iterator.AbstractIterator;

public class EmptyIterator extends AbstractIterator {
    private Status status;

    public EmptyIterator(Status status) {
        this.status = status;
    }

    public EmptyIterator() {
        this.status = Status.OK();
    }

    @Override
    public boolean valid() {
        assert !this.closed;
        return false;
    }

    @Override
    public void seekToFirst() {
        assert !this.closed;
    }

    @Override
    public void seekToLast() {
        assert !this.closed;
    }

    @Override
    public void seek(String target) {
        assert !this.closed;
    }

    @Override
    public void next() {
        assert !this.closed;
        assert(false);
    }

    @Override
    public void prev() {
        assert !this.closed;
        assert(false);
    }

    @Override
    public String key() {
        assert !this.closed;
        assert(false);
        return null;
    }

    @Override
    public String value() {
        assert !this.closed;
        assert(false);
        return null;
    }

    @Override
    public Status status() {
        assert !this.closed;
        return status;
    }
}
