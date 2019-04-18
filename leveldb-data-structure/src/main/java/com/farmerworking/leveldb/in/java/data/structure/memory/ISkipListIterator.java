package com.farmerworking.leveldb.in.java.data.structure.memory;

public interface ISkipListIterator<T> {
    boolean valid();

    T key();

    void next();

    void prev();

    void seekToFirst();

    void seekToLast();

    void seek(T target);
}
