package com.farmerworking.leveldb.in.java.data.structure;

import javafx.util.Pair;

public interface ICoding {
    void encodeFixed32(char[] buffer, int offset, int value);

    int decodeFixed32(char[] buffer, int offset);

    void putFixed32(StringBuilder buffer, int value);

    void encodeFixed64(char[] buffer, int offset, long value);

    long decodeFixed64(char[] buffer, int offset);

    void putFixed64(StringBuilder buffer, long value);
}
