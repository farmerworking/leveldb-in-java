package com.farmerworking.leveldb.in.java.data.structure.skiplist;

import java.util.concurrent.ConcurrentSkipListSet;

// note: c++ leveldb skiplist's next and prev is O(1), this version is O(logn)
public class JDKSkipListIterator<T> implements ISkipListIterator<T> {
    private ConcurrentSkipListSet<T> skipList;
    private T current;

    public JDKSkipListIterator(ConcurrentSkipListSet<T> skipList) {
        this.skipList = skipList;
        this.current = null;
    }

    @Override
    public boolean valid() {
        return current != null;
    }

    @Override
    public T key() {
        assert valid();
        return current;
    }

    @Override
    public void next() {
        assert valid();
        current = skipList.higher(current);
    }

    @Override
    public void prev() {
        assert valid();
        current = skipList.lower(current);
    }

    @Override
    public void seekToFirst() {
        if (!skipList.isEmpty()) {
            current = skipList.first();
        }
    }

    @Override
    public void seekToLast() {
        if (!skipList.isEmpty()) {
            current = skipList.last();
        }
    }

    @Override
    public void seek(T target) {
        current = skipList.ceiling(target);
    }
}
