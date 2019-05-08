package com.farmerworking.leveldb.in.java.data.structure.iterator;

import com.farmerworking.leveldb.in.java.api.Iterator;
import com.farmerworking.leveldb.in.java.data.structure.block.BlockIterator;

public class BlockIteratorTest extends AbstractIteratorTest{
    @Override
    protected Iterator getImpl() {
        return new BlockIterator(null, null, 0 ,1);
    }
}
