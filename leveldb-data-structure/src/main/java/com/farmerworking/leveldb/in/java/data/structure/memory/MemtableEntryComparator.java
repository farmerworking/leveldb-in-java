package com.farmerworking.leveldb.in.java.data.structure.memory;

import com.farmerworking.leveldb.in.java.common.ICoding;
import javafx.util.Pair;

import java.util.Comparator;

public class MemtableEntryComparator implements Comparator<char[]> {
    private static ICoding coding = ICoding.getInstance();

    InternalKeyComparator comparator;

    public MemtableEntryComparator(InternalKeyComparator comparator) {
        this.comparator = comparator;
    }

    @Override
    public int compare(char[] o1, char[] o2) {
        Pair<char[], Integer> pair1 = coding.getLengthPrefixedChars(o1, 0);
        Pair<char[], Integer> pair2 = coding.getLengthPrefixedChars(o2, 0);

        return comparator.compare(pair1.getKey(), pair2.getKey());
    }
}
