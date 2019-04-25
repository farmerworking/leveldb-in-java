package com.farmerworking.leveldb.in.java.data.structure.writebatch;

import com.farmerworking.leveldb.in.java.data.structure.memory.IMemtable;
import com.farmerworking.leveldb.in.java.data.structure.memory.ValueType;

public class MemTableInserter implements WriteBatchIterateHandler{
    private long sequence;
    private IMemtable memtable;

    public MemTableInserter(long sequence, IMemtable memtable) {
        this.sequence = sequence;
        this.memtable = memtable;
    }

    @Override
    public void put(String key, String value) {
        memtable.add(sequence, ValueType.kTypeValue, key, value);
        sequence ++;
    }

    @Override
    public void delete(String key) {
        memtable.add(sequence, ValueType.kTypeDeletion, key, "");
        sequence ++;
    }
}
