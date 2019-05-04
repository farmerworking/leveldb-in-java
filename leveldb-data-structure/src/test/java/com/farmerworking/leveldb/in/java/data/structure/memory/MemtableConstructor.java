package com.farmerworking.leveldb.in.java.data.structure.memory;

import com.farmerworking.leveldb.in.java.api.Comparator;
import com.farmerworking.leveldb.in.java.api.Iterator;
import com.farmerworking.leveldb.in.java.api.Options;
import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.data.structure.harness.Constructor;
import com.farmerworking.leveldb.in.java.data.structure.harness.StringComparator;

import java.util.Map;
import java.util.TreeMap;

class MemtableConstructor extends Constructor {
    private IMemtable memtable;

    public MemtableConstructor(Comparator comparator) {
        super(comparator);
    }

    @Override
    public Iterator<String, String> iterator() {
        return new KeyConvertingIterator(memtable.iterator());
    }

    @Override
    public Status finishImpl(Options options, Map<String, String> data) {
        memtable = IMemtable.getDefaultImpl(comparator);
        int seq = 1;
        for(String key : data.keySet()) {
            memtable.add(seq++, ValueType.kTypeValue, key, data.get(key));
        }
        return Status.OK();
    }
}
