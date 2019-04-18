package com.farmerworking.leveldb.in.java.data.structure.memory;

import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListSet;

public class JDKSkipList<T> implements ISkipList<T> {
    private ConcurrentSkipListSet<T> internalSkipList;

    public JDKSkipList(Comparator<T> comparator) {
        this.internalSkipList = new ConcurrentSkipListSet<>(comparator);
    }

    @Override
    public void insert(T key) {
        this.internalSkipList.add(key);
    }

    @Override
    public boolean contains(T key) {
        return this.internalSkipList.contains(key);
    }

    @Override
    public ISkipListIterator<T> iterator() {
        return new JDKSkipListIterator<>(internalSkipList);
    }
}
