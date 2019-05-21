package com.farmerworking.leveldb.in.java.data.structure.memory;

import com.farmerworking.leveldb.in.java.data.structure.skiplist.Sizable;

import java.util.Objects;

public class InternalKey implements Sizable {
    // We leave eight bits empty at the bottom so a type and sequence#
    // can be packed together into 64-bits.
    public static long kMaxSequenceNumber = (Long.MAX_VALUE >> 8) - 1;

    public final String userKey;
    final char[] userKeyChar;
    public final long sequence;
    public final ValueType type;

    public InternalKey(String userKey, long sequence, ValueType type) {
        this.userKey = userKey;
        this.userKeyChar = userKey.toCharArray();
        this.sequence = sequence;
        this.type = type;
    }

    public InternalKey(char[] userKeyChar, long sequence, ValueType type) {
        this.userKey = new String(userKeyChar);
        this.userKeyChar = userKeyChar;
        this.sequence = sequence;
        this.type = type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InternalKey that = (InternalKey) o;
        return sequence == that.sequence &&
                Objects.equals(userKey, that.userKey) &&
                type == that.type;
    }

    @Override
    public int memoryUsage() {
        return userKey.length() + userKeyChar.length + 8 + 4;
    }
}
