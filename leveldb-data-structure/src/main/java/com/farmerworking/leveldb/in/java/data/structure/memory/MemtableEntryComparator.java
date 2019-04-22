package com.farmerworking.leveldb.in.java.data.structure.memory;

import java.util.Comparator;

public class MemtableEntryComparator implements Comparator<MemtableEntry> {
    InternalKeyComparator comparator;

    public MemtableEntryComparator(InternalKeyComparator comparator) {
        this.comparator = comparator;
    }

    @Override
    public int compare(MemtableEntry o1, MemtableEntry o2) {
        return comparator.compare(o1.internalKey, o2.internalKey);
    }
}
