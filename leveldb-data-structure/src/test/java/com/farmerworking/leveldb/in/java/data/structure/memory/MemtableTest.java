package com.farmerworking.leveldb.in.java.data.structure.memory;

import com.farmerworking.leveldb.in.java.data.structure.BytewiseComparator;

public class MemtableTest extends IMemtableTest {
    @Override
    protected IMemtable getImpl() {
        InternalKeyComparator comparator = new InternalKeyComparator(new BytewiseComparator());
        return new Memtable(comparator);
    }
}
