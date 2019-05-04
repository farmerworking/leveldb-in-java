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
}
