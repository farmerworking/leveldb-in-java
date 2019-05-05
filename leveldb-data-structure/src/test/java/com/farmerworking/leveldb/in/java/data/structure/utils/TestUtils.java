package com.farmerworking.leveldb.in.java.data.structure.utils;

import java.util.Random;

public class TestUtils {
    // Make sure to generate a wide variety of characters so we
    // test the boundary conditions for short-key optimizations.
    private static char[] kTestChars = new char[]{
            0, 1, 'a', 'b', 'c', 'd', 'e', 253, 254, 255
    };

    public static String randomKey(int len) {
        Random random = new Random();
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < len; i++) {
            result.append(kTestChars[random.nextInt(kTestChars.length)]);
        }

        return result.toString();
    }

    public static String randomString(int len) {
        Random random = new Random();
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < len; i++) {
            builder.append((char)(
                    (int)' ' + random.nextInt(95)
            ));   // ' ' .. '~'
        }
        return builder.toString();
    }

    public static String compressibleString(double compressed_fraction, int len) {
        int raw = (int)(len * compressed_fraction);
        if (raw < 1) raw = 1;
        String s = randomString(raw);

        // Duplicate the random data until we have filled "len" bytes
        StringBuilder builder = new StringBuilder();
        while (builder.length() < len) {
            builder.append(s);
        }
        return builder.substring(0, len);
    }
}
