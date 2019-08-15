package com.farmerworking.leveldb.in.java.data.structure.utils;

import com.farmerworking.leveldb.in.java.api.Comparator;
import com.farmerworking.leveldb.in.java.api.Iterator;
import com.farmerworking.leveldb.in.java.api.Status;
import javafx.util.Pair;

import java.util.List;

public class SimpleIterator implements Iterator<String, String> {
    private Integer index;
    private List<Pair<String, String>> data;
    private Comparator comparator;

    public SimpleIterator(List<Pair<String, String>> data, Comparator comparator) {
        assert data != null;
        this.index = null;
        this.data = data;
        this.comparator = comparator;
    }

    @Override
    public boolean valid() {
        return index != null;
    }

    @Override
    public void seekToFirst() {
        if (data.size() > 0) {
            this.index = 0;
        }
    }

    @Override
    public void seekToLast() {
        if (data.size() > 0) {
            this.index = data.size() - 1;
        }
    }

    @Override
    public void seek(String target) {
        this.index = null;
        for (int i = 0; i < data.size(); i++) {
            Pair<String, String> pair = data.get(i);
            if (comparator.compare(pair.getKey().toCharArray(), target.toCharArray()) >= 0) {
                this.index = i;
                break;
            }
        }
    }

    @Override
    public void next() {
        this.index ++;
        if (this.index >= data.size()) {
            this.index = null;
        }
    }

    @Override
    public void prev() {
        this.index --;
        if (this.index < 0) {
            this.index = null;
        }
    }

    @Override
    public String key() {
        return this.data.get(this.index).getKey();
    }

    @Override
    public String value() {
        return this.data.get(this.index).getValue();
    }

    @Override
    public Status status() {
        return Status.OK();
    }

    @Override
    public void close() {
    }

    @Override
    public void registerCleanup(Runnable runnable) {
    }
}
