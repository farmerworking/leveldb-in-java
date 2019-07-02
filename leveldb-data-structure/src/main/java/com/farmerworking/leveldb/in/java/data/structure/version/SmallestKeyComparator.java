package com.farmerworking.leveldb.in.java.data.structure.version;

import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKeyComparator;

import java.util.Comparator;

public class SmallestKeyComparator implements Comparator<FileMetaData> {
    private InternalKeyComparator internalKeyComparator;

    public SmallestKeyComparator(InternalKeyComparator internalKeyComparator) {
        this.internalKeyComparator = internalKeyComparator;
    }

    @Override
    public int compare(FileMetaData o1, FileMetaData o2) {
        int r = internalKeyComparator.compare(o1.getSmallest(), o2.getSmallest());

        if (r != 0) {
            return r;
        } else {
            return (int) (o1.getFileNumber() - o2.getFileNumber());
        }
    }
}
