package com.farmerworking.leveldb.in.java.data.structure.iterator;

import com.farmerworking.leveldb.in.java.api.Iterator;
import com.farmerworking.leveldb.in.java.data.structure.memory.KeyConvertingIterator;

public class KeyConvertingIteratorTest extends AbstractIteratorTest {
    @Override
    protected Iterator getImpl() {
        return new KeyConvertingIterator(null);
    }
}
