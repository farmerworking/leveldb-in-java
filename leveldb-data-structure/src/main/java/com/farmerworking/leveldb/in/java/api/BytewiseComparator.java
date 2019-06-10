package com.farmerworking.leveldb.in.java.api;

import java.util.Arrays;

public class BytewiseComparator implements Comparator {
    public static char UNSIGNED_CHAR_MAX_VALUE = 255;
    public static char UNSIGNED_CHAR_MIN_VALUE = 0;

    @Override
    public String name() {
        return "leveldb.BytewiseComparator";
    }

    @Override
    public int compare(char[] o1, char[] o2) {
        int n = Math.min(o1.length, o2.length);
        for (int i = 0, j = 0; i < n; i++, j++) {
            int cmp = compare(o1[i], o2[j]);
            if (cmp != 0)
                return cmp;
        }
        return o1.length - o2.length;
    }

    @Override
    public char[] findShortestSeparator(char[] a, char[] b) {
        int minLength = Math.min(a.length, b.length);
        int diffIndex = 0;
        while(diffIndex < minLength && a[diffIndex] == b[diffIndex]) {
            diffIndex ++;
        }

        if (diffIndex >= minLength) {
            // Do not shorten if one string is a prefix of the other
            return a;
        } else {
            char diffChar = a[diffIndex];
            if (compare(diffChar, UNSIGNED_CHAR_MAX_VALUE) < 0 && compare((char)(diffChar + 1), b[diffIndex]) < 0) {
                char[] result = Arrays.copyOf(a, diffIndex + 1);
                result[diffIndex] = (char) (diffChar + 1);
                assert compare(result, b) < 0;
                return result;
            } else {
                return a;
            }
        }
    }

    @Override
    public char[] findShortSuccessor(char[] chars) {
        for (int i = 0; i < chars.length; i++) {
            char b = chars[i];

            if (compare(b, UNSIGNED_CHAR_MAX_VALUE) < 0) {
                b = (char) (b + 1);

                char[] result = Arrays.copyOf(chars, i + 1);
                result[i] = b;
                return result;
            }
        }

        return chars;
    }

    private int compare(char c1, char c2) {
        return c1 - c2;
    }
}
