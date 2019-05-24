package com.farmerworking.leveldb.in.java.data.structure.memory;

import com.farmerworking.leveldb.in.java.common.ICoding;
import com.farmerworking.leveldb.in.java.data.structure.skiplist.Sizable;

import java.util.Objects;

import static com.farmerworking.leveldb.in.java.data.structure.memory.ValueType.kValueTypeForSeek;

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

    public String encode() {
        assert(this.sequence <= kMaxSequenceNumber);
        assert(this.type.getValue()  <= kValueTypeForSeek);

        StringBuilder builder = new StringBuilder();
        builder.append(userKey);
        ICoding.getInstance().putFixed64(builder, (this.sequence << 8) | this.type.getValue());
        return builder.toString();
    }

    public static InternalKey decode(String s) {
        int length = ICoding.getInstance().getFixed64Length();
        assert s.length() >= length;
        char[] buffer = s.toCharArray();
        String userKey = new String(buffer, 0, s.length() - length);
        long num = ICoding.getInstance().decodeFixed64(buffer, s.length() - length);
        return new InternalKey(userKey, num >>> 8, ValueType.valueOf((int) (num & 0xff)));
    }
}
