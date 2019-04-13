package com.farmerworking.leveldb.in.java.data.structure;

public class Coding implements ICoding {
    static final int FIXED_32_LENGTH = 4;
    static final int FIXED_64_LENGTH = 8;

    @Override
    public void encodeFixed32(char[] buffer, int offset, int value) {
        encodeFixedInternal(buffer, offset, value, FIXED_32_LENGTH);
    }

    @Override
    public int decodeFixed32(char[] buffer, int offset) {
        return (int) decodeFixedInternal(buffer, offset, FIXED_32_LENGTH);
    }

    @Override
    public void putFixed32(StringBuilder buffer, int value) {
        putFixedInternal(buffer, Integer.toUnsignedLong(value), FIXED_32_LENGTH);
    }

    @Override
    public void encodeFixed64(char[] buffer, int offset, long value) {
        encodeFixedInternal(buffer, offset, value, FIXED_64_LENGTH);
    }

    private void encodeFixedInternal(char[] buffer, int offset, long value, int fixedLength) {
        assert buffer.length + 1 > offset + fixedLength;

        for (int i = 0; i < fixedLength; i++) {
            buffer[offset + i] = (char) ((value >>> (i * 8)) & 0xff);
        }
    }

    @Override
    public long decodeFixed64(char[] buffer, int offset) {
        return decodeFixedInternal(buffer, offset, FIXED_64_LENGTH);
    }

    private long decodeFixedInternal(char[] buffer, int offset, int fixedLength) {
        assert buffer.length + 1 > offset + fixedLength;

        long result = 0;
        for (int i = 0; i < fixedLength; i++) {
            result = result | (long)buffer[offset + i] << (i * 8);
        }
        return result;
    }

    @Override
    public void putFixed64(StringBuilder buffer, long value) {
        putFixedInternal(buffer, value, FIXED_64_LENGTH);
    }

    private void putFixedInternal(StringBuilder buffer, long value, int fixedLength) {
        char[] chars = new char[fixedLength];
        encodeFixedInternal(chars, 0, value, fixedLength);
        buffer.append(chars);
    }
}
