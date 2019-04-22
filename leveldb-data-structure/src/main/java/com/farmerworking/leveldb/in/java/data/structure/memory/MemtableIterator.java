package com.farmerworking.leveldb.in.java.data.structure.memory;

import com.farmerworking.leveldb.in.java.common.Status;
import com.farmerworking.leveldb.in.java.data.structure.Iterator;
import com.farmerworking.leveldb.in.java.data.structure.skiplist.ISkipList;
import com.farmerworking.leveldb.in.java.data.structure.skiplist.ISkipListIterator;

public class MemtableIterator implements Iterator {
    private ISkipListIterator<MemtableEntry> iter;

    public MemtableIterator(ISkipList<MemtableEntry> skipList) {
        this.iter = skipList.iterator();
    }

    @Override
    public boolean valid() {
        return iter.valid();
    }

    @Override
    public void seekToFirst() {
        iter.seekToFirst();
    }

    @Override
    public void seekToLast() {
        iter.seekToLast();
    }

    @Override
    public void seek(String target) {
        MemtableEntry seek = new MemtableEntry(InternalKey.kMaxSequenceNumber, ValueType.kTypeValue, target, null);
        iter.seek(seek);
    }

    @Override
    public void next() {
        iter.next();
    }

    @Override
    public void prev() {
        iter.prev();
    }

    @Override
    public String key() {
        return iter.key().internalKey.userKey;
    }

    @Override
    public String value() {
        return iter.key().value;
    }

    @Override
    public Status status() {
        return Status.OK();
    }
}
