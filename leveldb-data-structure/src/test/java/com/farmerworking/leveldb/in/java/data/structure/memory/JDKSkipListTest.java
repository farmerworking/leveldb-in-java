package com.farmerworking.leveldb.in.java.data.structure.memory;

import java.util.Comparator;

public class JDKSkipListTest extends ISkipListConcurrentTest {
    @Override
    protected ISkipList getImpl(Comparator comparator) {
        return new JDKSkipList(comparator);
    }
}
