package com.farmerworking.leveldb.in.java.data.structure.memory;

import com.farmerworking.leveldb.in.java.common.ICoding;
import javafx.util.Pair;

import java.util.Arrays;

import static com.farmerworking.leveldb.in.java.data.structure.memory.ValueType.kTypeValue;

public class InternalKey {
    // We leave eight bits empty at the bottom so a type and sequence#
    // can be packed together into 64-bits.
    public static long kMaxSequenceNumber = (Long.MAX_VALUE >> 8) - 1;
    private static ICoding coding = ICoding.getInstance();

    private char[] rep;

    public InternalKey() {
        this.rep = new char[0];
    }


    public InternalKey(String userKey, long sequence, ValueType valueType) {
        init(userKey.toCharArray(), sequence, valueType);
    }

    public InternalKey(char[] userKeyChar, long sequence, ValueType valueType) {
        init(userKeyChar, sequence, valueType);
    }

    public InternalKey(String userKey, long sequence) {
        init(userKey.toCharArray(), sequence, kTypeValue);
    }

    private void init(char[] userKey, long sequence, ValueType valueType) {
        this.rep = new char[userKey.length + coding.getFixed64Length()];
        System.arraycopy(userKey, 0, this.rep, 0, userKey.length);
        coding.encodeFixed64(this.rep, userKey.length, packSequenceAndType(sequence, valueType));
    }

    public void decodeFrom(String s) {
        this.rep = s.toCharArray();
    }

    public String encode() {
        assert this.rep != null;
        return new String(this.rep);
    }

    public char[] getRep() {
        return rep;
    }

    public String userKey() {
        return new String(extractUserKey(this.rep));
    }

    public ValueType type() {
        return extractValueType(this.rep);
    }

    public long sequence() {
        return extractSequence(this.rep);
    }

    public void clear() {
        this.rep = new char[0];
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InternalKey that = (InternalKey) o;
        return Arrays.equals(rep, that.rep);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(rep);
    }

    public ParsedInternalKey convert() {
        ParsedInternalKey result = new ParsedInternalKey();
        result.setUserKey(this.userKey());
        result.setSequence(this.sequence());
        result.setValueType(this.type());
        return result;
    }

    public static long packSequenceAndType(long sequence, ValueType valueType) {
        assert sequence <= kMaxSequenceNumber;
        return (sequence << 8) | valueType.getValue();
    }

    public static Pair<Boolean, ParsedInternalKey> parseInternalKey(String s) {
        if (s.length() < coding.getFixed64Length()) {
            return new Pair<>(false, null);
        }
        long num = coding.decodeFixed64(s.toCharArray(), s.length() - coding.getFixed64Length());
        int c = (int) num & 0xff;

        ParsedInternalKey result = new ParsedInternalKey();
        result.setSequence(num >>> 8);
        result.setUserKey(s.substring(0, s.length() - coding.getFixed64Length()));
        result.setValueType(ValueType.valueOf(c));

        return new Pair<>(c <= kTypeValue.getValue(), result);
    }

    public static String extractUserKey(String s) {
        return new String(extractUserKey(s.toCharArray()));
    }

    public static char[] extractUserKey(char[] s) {
        assert s.length >= coding.getFixed64Length();
        return Arrays.copyOfRange(s, 0, s.length - coding.getFixed64Length());
    }

    public static ValueType extractValueType(char[] s) {
        assert s.length >= coding.getFixed64Length();
        long num = coding.decodeFixed64(s,  s.length - coding.getFixed64Length());
        int c = (int) num & 0xff;
        return ValueType.valueOf(c);
    }

    public static long extractSequence(char[] s) {
        assert s.length >= coding.getFixed64Length();
        long num = coding.decodeFixed64(s,  s.length - coding.getFixed64Length());
        return num >>> 8;
    }
}
