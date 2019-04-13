package com.farmerworking.leveldb.in.java.data.structure;

import javafx.util.Pair;

public class Coding implements ICoding {
    static final int FIXED_32_LENGTH = 4;
    static final int FIXED_64_LENGTH = 8;

    static final int VAR_INT_32_MAX_LENGTH = 5;
    static final int VAR_INT_64_MAX_LENGTH = 10;

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

    @Override
    public int encodeVarint32(char[] buffer, int offset, int value) {
        return encodeVarint64(buffer, offset, Integer.toUnsignedLong(value));
    }

    @Override
    public Pair<Integer, Integer> decodeVarint32(char[] buffer, int offset) {
        return decodeVarint32(buffer, offset, buffer.length);
    }

    @Override
    public Pair<Integer, Integer> decodeVarint32(char[] buffer, int offset, int length) {
        Pair<Long, Integer> pair = decodeVarintInternal(buffer, offset, length, 7 * (VAR_INT_32_MAX_LENGTH - 1));

        if (pair != null) {
            return new Pair<>(pair.getKey().intValue(), pair.getValue());
        } else {
            return null;
        }
    }

    @Override
    public void putVarint32(StringBuilder builder, int value) {
        putVarintInternal(builder, Integer.toUnsignedLong(value), VAR_INT_32_MAX_LENGTH);
    }


    /**
     * binary representation:
     * 128: 10000000
     * 127: 01111111
     *
     * (value & 127) | 128 --- start with 1 and append with value's last 7 bits
     *
     * the 8th bit represent whether it's the end
     * 0: no more, the end
     * 1: has more
     */
    @Override
    public int encodeVarint64(char[] buffer, int offset, long value) {
        int offsetBefore = offset;

        int B = 128;
        while (Long.compareUnsigned(value, B) >= 0) {
            buffer[offset ++] = (char)((value & (B-1)) | B);
            value = value >>> 7;
        }
        buffer[offset ++] = (char) value;
        return offset - offsetBefore;
    }

    @Override
    public Pair<Long, Integer> decodeVarint64(char[] buffer, int offset) {
        return decodeVarint64(buffer, offset, buffer.length);
    }

    @Override
    public Pair<Long, Integer> decodeVarint64(char[] buffer, int offset, int length) {
        return decodeVarintInternal(buffer, offset, length, 7 * (VAR_INT_64_MAX_LENGTH - 1));
    }

    @Override
    public void putVarint64(StringBuilder builder, long value) {
        putVarintInternal(builder, value, VAR_INT_64_MAX_LENGTH);
    }

    @Override
    public int varintLength(long value) {
        int len = 1;
        while (Long.compareUnsigned(value, 128) >= 0) {
            value = value >>> 7;
            len++;
        }
        return len;
    }

    @Override
    public int varintLength(int value) {
        return varintLength(Integer.toUnsignedLong(value));
    }

    @Override
    public void putLengthPrefixedString(StringBuilder buffer, String value) {
        putVarint32(buffer, value.length());
        buffer.append(value);
    }

    @Override
    public Pair<String, Integer> getLengthPrefixedString(char[] buffer, int offset) {
        return getLengthPrefixedString(buffer, offset, buffer.length);
    }

    @Override
    public Pair<String, Integer> getLengthPrefixedString(char[] buffer, int offset, int length) {
        Pair<Integer, Integer> p = decodeVarint32(buffer, offset);
        if (p == null) return null; // due to overflow or truncation

        int strLength = p.getKey();
        int strStartOffset = p.getValue();

        if (strLength + strStartOffset > length) return null; // buffer is truncated
        return new Pair<>(
                new String(buffer, strStartOffset, strLength),
                strStartOffset + strLength
        );
    }

    private Pair<Long, Integer> decodeVarintInternal(char[] buffer, int offset, int limit, int shiftLimit) {
        long result = 0L;
        for (int shift = 0; shift <= shiftLimit && offset < limit; shift += 7) {
            long byteValue = buffer[offset];
            offset++;
            if ((byteValue & 128) > 0) {
                // More bytes are present
                result |= ((byteValue & 127) << shift);
            } else {
                result |= (byteValue << shift);
                return new Pair<>(result, offset);
            }
        }

        return null;
    }

    private void putVarintInternal(StringBuilder builder, long value, int varintMaxLength) {
        char[] buffer = new char[varintMaxLength];
        int length = encodeVarint64(buffer, 0, value);
        builder.append(buffer, 0, length);
    }
}
