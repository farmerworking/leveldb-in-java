package com.farmerworking.leveldb.in.java.data.structure.iterator;

import com.farmerworking.leveldb.in.java.api.BytewiseComparator;
import com.farmerworking.leveldb.in.java.api.Iterator;
import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKeyComparator;
import com.farmerworking.leveldb.in.java.data.structure.memory.MemtableEntryComparator;
import com.farmerworking.leveldb.in.java.data.structure.memory.MemtableIterator;
import com.farmerworking.leveldb.in.java.data.structure.skiplist.JDKSkipList;

public class MemtableIteratorTest extends AbstractIteratorTest {
    @Override
    protected Iterator getImpl() {
        return new MemtableIterator(
                new JDKSkipList<>(
                        new MemtableEntryComparator(
                                new InternalKeyComparator(new BytewiseComparator()))));
    }
}
