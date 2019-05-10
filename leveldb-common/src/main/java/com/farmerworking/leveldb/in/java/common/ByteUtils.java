package com.farmerworking.leveldb.in.java.common;

// c++ use 1 byte to represent char
public class ByteUtils {
    public static byte[] toByteArray(char[] chars) {
        return toByteArray(chars, 0, chars.length);
    }

    public static byte[] toByteArray(char[] chars, int offset, int length) {
        byte[] bytes = new byte[length];
        for (int i = offset, j = 0; i < offset + length; i++, j++) {
            bytes[j] = (byte) chars[i];
        }
        return bytes;
    }

    public static byte[] toByteArray(String s) {
        return toByteArray(s, 0, s.length());
    }

    public static byte[] toByteArray(String s, int offset, int length) {
        byte[] bytes = new byte[length];
        for (int i = offset, j = 0; i < offset + length; i++, j++) {
            bytes[j] = (byte) s.charAt(i);
        }
        return bytes;
    }

    public static char[] toCharArray(byte[] bytes) {
        return toCharArray(bytes, 0, bytes.length);
    }

    public static char[] toCharArray(byte[] bytes, int offset, int length) {
        char[] chars = new char[length];
        for (int i = offset, j = 0; i < offset + length; i++, j++) {
            chars[j] = (char)Byte.toUnsignedInt(bytes[i]);
        }
        return chars;
    }
}
