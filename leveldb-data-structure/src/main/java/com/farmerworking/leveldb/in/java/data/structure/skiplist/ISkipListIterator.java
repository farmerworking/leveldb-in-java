package com.farmerworking.leveldb.in.java.data.structure.skiplist;

public interface ISkipListIterator<T> {
    boolean valid();

    T key();

    void next();

    void prev();

    void seekToFirst();

    void seekToLast();

    void seek(T target);
}
