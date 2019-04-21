package com.farmerworking.leveldb.in.java.data.structure;

public interface Comparator {
    String name();

    int compare(char[] a, char[] b);

    char[] findShortestSeparator(char[] a, char[] b);

    char[] findShortSuccessor(char[] a);
}
