package com.farmerworking.leveldb.in.java.data.structure.iterator;

import com.farmerworking.leveldb.in.java.api.Iterator;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractIterator<K, V> implements Iterator<K, V> {
    protected List<Runnable> cleanupRunnables = new ArrayList<>();
    protected volatile boolean closed = false;

    @Override
    public void close() {
        assert !this.closed;
        closed = true;

        if (!cleanupRunnables.isEmpty()) {
            for(Runnable runnable : cleanupRunnables) {
                runnable.run();
            }
        }
    }

    @Override
    public void registerCleanup(Runnable runnable) {
        assert !this.closed;
        assert runnable != null;
        this.cleanupRunnables.add(runnable);
    }
}
