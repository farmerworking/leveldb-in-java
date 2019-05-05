package com.farmerworking.leveldb.in.java.common;

// c++ use 1 byte to represent char
public class ByteUtils {
    public static byte[] toByteArray(char[] chars, int offset, int length) {
        byte[] bytes = new byte[length];
        for (int i = offset, j = 0; i < offset + length; i++, j++) {
            bytes[j] = (byte) chars[i];
        }
        return bytes;
    }

    public static byte[] toByteArray(String chars, int offset, int length) {
        byte[] bytes = new byte[length];
        for (int i = offset, j = 0; i < offset + length; i++, j++) {
            bytes[j] = (byte) chars.charAt(i);
        }
        return bytes;
    }

    public static char[] toCharArray(byte[] bytes, int offset, int length) {
        char[] chars = new char[length];
        for (int i = offset, j = 0; i < offset + length; i++, j++) {
            chars[j] = (char) bytes[i];
        }
        return chars;
    }
}
