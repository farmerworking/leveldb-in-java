package com.farmerworking.leveldb.in.java.data.structure.version;

import java.util.Comparator;

public class NewestFileComparator implements Comparator<FileMetaData> {
    @Override
    public int compare(FileMetaData o1, FileMetaData o2) {
        if (o1.getFileNumber() == o2.getFileNumber()) {
            return 0;
        }

        return o1.getFileNumber() > o2.getFileNumber() ? -1 : 1;
    }
}
