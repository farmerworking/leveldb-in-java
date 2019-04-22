package com.farmerworking.leveldb.in.java.data.structure.memory;

import com.farmerworking.leveldb.in.java.data.structure.skiplist.Sizable;

public class MemtableEntry implements Sizable {
    final InternalKey internalKey;
    final String value;

    public MemtableEntry(long sequence, ValueType type, String key, String value) {
        this.internalKey = new InternalKey(key, sequence, type);
        this.value = value;
    }

    @Override
    public int memoryUsage() {
        return internalKey.memoryUsage() + value.length();
    }
}
