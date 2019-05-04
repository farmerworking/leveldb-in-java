package com.farmerworking.leveldb.in.java.data.structure.harness;

import com.farmerworking.leveldb.in.java.api.BytewiseComparator;
import com.farmerworking.leveldb.in.java.api.Comparator;

class ReverseKeyComparator implements Comparator {
    private Comparator comparator;

    public ReverseKeyComparator() {
        comparator = new BytewiseComparator();
    }

    @Override
    public String name() {
        return "leveldb.ReverseBytewiseComparator";

    }

    @Override
    public int compare(char[] a, char[] b) {
        return comparator.compare(reverse(a), reverse(b));
    }

    @Override
    public char[] findShortestSeparator(char[] a, char[] b) {
        char[] a1 = reverse(a);
        char[] b1 = reverse(b);
        return reverse(comparator.findShortestSeparator(a1, b1));
    }

    @Override
    public char[] findShortSuccessor(char[] a) {
        char[] a1 = reverse(a);
        return reverse(comparator.findShortSuccessor(a1));
    }

    private char[] reverse(char[] chars) {
        char[] result = new char[chars.length];

        for (int i = chars.length - 1, j = 0; i >= 0; i--, j++) {
            result[j] = chars[i];
        }

        return result;
    }
}
