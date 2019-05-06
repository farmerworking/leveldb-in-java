package com.farmerworking.leveldb.in.java.data.structure.block;

import com.farmerworking.leveldb.in.java.api.Comparator;
import com.farmerworking.leveldb.in.java.api.Iterator;
import com.farmerworking.leveldb.in.java.data.structure.skiplist.Sizable;

public interface IBlockReader extends Sizable {
    Iterator<String, String> iterator(Comparator comparator);

    public static IBlockReader getDefaultImpl(String blockContent) {
        return new BlockReader(blockContent);
    }
}
