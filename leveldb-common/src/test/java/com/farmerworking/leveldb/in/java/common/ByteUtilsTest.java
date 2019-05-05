package com.farmerworking.leveldb.in.java.common;

import org.junit.Test;

import static org.junit.Assert.*;

public class ByteUtilsTest {
    @Test
    public void testCharByteArrayConvert() {
        char[] chars = {0, 255, 1, 2, 3, 126, 127, 128};
        byte[] bytes = ByteUtils.toByteArray(chars, 0, chars.length);
        char[] newChars = ByteUtils.toCharArray(bytes, 0, bytes.length);
        assertArrayEquals(chars, newChars);
    }
}