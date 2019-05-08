package com.farmerworking.leveldb.in.java.data.structure.memory;

import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.data.structure.iterator.AbstractIterator;
import com.farmerworking.leveldb.in.java.data.structure.skiplist.ISkipList;
import com.farmerworking.leveldb.in.java.data.structure.skiplist.ISkipListIterator;

public class MemtableIterator extends AbstractIterator<InternalKey, String> {
    private ISkipListIterator<MemtableEntry> iter;

    public MemtableIterator(ISkipList<MemtableEntry> skipList) {
        this.iter = skipList.iterator();
    }

    @Override
    public boolean valid() {
        assert !this.closed;
        return iter.valid();
    }

    @Override
    public void seekToFirst() {
        assert !this.closed;
        iter.seekToFirst();
    }

    @Override
    public void seekToLast() {
        assert !this.closed;
        iter.seekToLast();
    }

    @Override
    public void seek(String target) {
        assert !this.closed;
        MemtableEntry seek = new MemtableEntry(InternalKey.kMaxSequenceNumber, ValueType.kTypeValue, target, null);
        iter.seek(seek);
    }

    @Override
    public void next() {
        assert !this.closed;
        iter.next();
    }

    @Override
    public void prev() {
        assert !this.closed;
        iter.prev();
    }

    @Override
    public InternalKey key() {
        assert !this.closed;
        return iter.key().internalKey;
    }

    @Override
    public String value() {
        assert !this.closed;
        return iter.key().value;
    }

    @Override
    public Status status() {
        assert !this.closed;
        return Status.OK();
    }
}
