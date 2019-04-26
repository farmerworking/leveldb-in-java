package com.farmerworking.leveldb.in.java.data.structure.block;

import com.farmerworking.leveldb.in.java.api.Comparator;
import com.farmerworking.leveldb.in.java.api.Iterator;

public interface IBlockReader {
    Iterator<String, String> iterator(Comparator comparator);
}
