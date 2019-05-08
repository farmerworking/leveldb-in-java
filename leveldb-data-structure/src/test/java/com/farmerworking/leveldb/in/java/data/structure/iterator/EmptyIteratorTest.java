package com.farmerworking.leveldb.in.java.data.structure.iterator;

import com.farmerworking.leveldb.in.java.api.Iterator;
import com.farmerworking.leveldb.in.java.data.structure.block.EmptyIterator;

public class EmptyIteratorTest extends AbstractIteratorTest {
    @Override
    protected Iterator getImpl() {
        return new EmptyIterator();
    }
}
