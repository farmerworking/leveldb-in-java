package com.farmerworking.leveldb.in.java.data.structure.skiplist;

public interface ISkipList<T> {
    // Insert key into the list.
    // REQUIRES: nothing that compares equal to key is currently in the list.
    void insert(T key);

    // Returns true iff an entry that compares equal to key is in the list.
    boolean contains(T key);

    ISkipListIterator<T> iterator();
}
