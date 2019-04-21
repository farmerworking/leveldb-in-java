package com.farmerworking.leveldb.in.java.common;

import javafx.util.Pair;

public interface ICoding {
    ICoding instance = getDefaultImpl();

    void encodeFixed32(char[] buffer, int offset, int value);

    int decodeFixed32(char[] buffer, int offset);

    void putFixed32(StringBuilder buffer, int value);

    void encodeFixed64(char[] buffer, int offset, long value);

    long decodeFixed64(char[] buffer, int offset);

    void putFixed64(StringBuilder buffer, long value);

    /**
     * @return value encode length
     */
    int encodeVarint32(char[] buffer, int offset, int value);

    /**
     * @return a pair. pair's left is decoded value. pair's right is offset just past decoded value
     */
    Pair<Integer, Integer> decodeVarint32(char[] buffer, int offset);

    Pair<Integer, Integer> decodeVarint32(char[] buffer, int offset, int length);

    void putVarint32(StringBuilder builder, int value);

    /**
     * @return value encode length
     */
    int encodeVarint64(char[] buffer, int offset, long value);

    /**
     * @return a pair. pair's left is decoded value. pair's right is offset just past decoded value
     */
    Pair<Long, Integer> decodeVarint64(char[] buffer, int offset);

    Pair<Long, Integer> decodeVarint64(char[] buffer, int offset, int length);

    void putVarint64(StringBuilder builder, long value);

    int varintLength(long value);

    int varintLength(int value);

    void putLengthPrefixedString(StringBuilder buffer, String value);

    /**
     * @return a pair. pair's left is length prefixed string. pair's right is offset just past the string
     */
    Pair<String, Integer> getLengthPrefixedString(char[] buffer, int offset);

    Pair<String, Integer> getLengthPrefixedString(char[] buffer, int offset, int length);

    static ICoding getDefaultImpl() {
        return new Coding();
    }

    static ICoding getInstance() {
        return instance;
    }
}
