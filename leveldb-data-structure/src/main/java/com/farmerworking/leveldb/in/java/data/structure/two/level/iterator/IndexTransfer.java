package com.farmerworking.leveldb.in.java.data.structure.two.level.iterator;

import com.farmerworking.leveldb.in.java.api.Iterator;
import com.farmerworking.leveldb.in.java.api.ReadOptions;

public interface IndexTransfer {
    Iterator<String, String> transfer(ReadOptions options, String value);
}
