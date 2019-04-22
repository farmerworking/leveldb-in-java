package com.farmerworking.leveldb.in.java.data.structure.skiplist;

import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

public class JDKSkipList<T> implements ISkipList<T> {
    private ConcurrentSkipListSet<T> internalSkipList;
    private AtomicInteger memoryUsage;

    public JDKSkipList(Comparator<T> comparator) {
        this.internalSkipList = new ConcurrentSkipListSet<>(comparator);
        this.memoryUsage = new AtomicInteger(0);
    }

    @Override
    public void insert(T key) {
        this.internalSkipList.add(key);

        if (key instanceof String) {
            memoryUsage.addAndGet(((String) key).length());
        }
    }

    @Override
    public boolean contains(T key) {
        return this.internalSkipList.contains(key);
    }

    @Override
    public ISkipListIterator<T> iterator() {
        return new JDKSkipListIterator<>(internalSkipList);
    }

    @Override
    public int approximateMemoryUsage() {
        return memoryUsage.get();
    }
}
