package com.farmerworking.leveldb.in.java.data.structure.harness;

import java.util.Comparator;

public class StringComparator implements Comparator<String> {
    private final com.farmerworking.leveldb.in.java.api.Comparator comparator;

    public StringComparator(com.farmerworking.leveldb.in.java.api.Comparator comparator) {
        this.comparator = comparator;
    }

    @Override
    public int compare(String o1, String o2) {
        return comparator.compare(o1.toCharArray(), o2.toCharArray());
    }
}
