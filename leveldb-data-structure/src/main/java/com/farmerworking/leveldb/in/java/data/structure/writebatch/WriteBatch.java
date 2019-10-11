package com.farmerworking.leveldb.in.java.data.structure.writebatch;

import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.common.ICoding;
import com.farmerworking.leveldb.in.java.data.structure.memory.ValueType;
import javafx.util.Pair;

import java.util.Arrays;

public class WriteBatch {
    private static ICoding coding = ICoding.getInstance();
    public static int kHeaderSize = coding.getFixed32Length() + coding.getFixed64Length();

    private char[] buffer;
    private StringBuilder builder;

    public WriteBatch() {
        this.buffer = new char[kHeaderSize];
        this.builder = new StringBuilder();
    }

    public long getSequence() {
        return coding.decodeFixed64(buffer, 0);
    }

    public void setSequence(long sequence) {
        coding.encodeFixed64(buffer, 0, sequence);
    }

    public int getCount() {
        return coding.decodeFixed32(buffer, coding.getFixed64Length());
    }

    public void setCount(int count) {
        coding.encodeFixed32(buffer, coding.getFixed64Length(), count);
    }

    public void put(String key, String value) {
        setCount(getCount() + 1);
        this.builder.append((char) ValueType.kTypeValue.getValue());
        coding.putLengthPrefixedString(this.builder, key);
        coding.putLengthPrefixedString(this.builder, value);
    }

    public void delete(String key) {
        setCount(getCount() + 1);
        this.builder.append((char) ValueType.kTypeDeletion.getValue());
        coding.putLengthPrefixedString(this.builder, key);
    }

    public void append(WriteBatch writeBatch) {
        setCount(getCount() + writeBatch.getCount());
        this.builder.append(writeBatch.getBuilder());
    }

    public StringBuilder getBuilder() {
        return builder;
    }

    public void clear() {
        this.builder = new StringBuilder();
        this.buffer = new char[this.buffer.length];
    }

    public int approximateSize() {
        return this.buffer.length + this.builder.length();
    }

    public Status iterate(WriteBatchIterateHandler handler) {
        char[] chars = encode();
        if (chars.length < kHeaderSize) {
            return Status.Corruption("malformed WriteBatch (too small)");
        }

        int found = 0;
        int index = kHeaderSize;
        while (index < chars.length) {
            found ++;
            ValueType valueType = ValueType.valueOf((int) chars[index]);
            index += 1;

            if (valueType == null) {
                return Status.Corruption("unknown WriteBatch tag");
            } else if (valueType.equals(ValueType.kTypeValue)) {
                Pair<String, Integer> pair = getLengthPrefixedString(chars, index);
                if (pair == null) {
                    return Status.Corruption("bad WriteBatch Put");
                } else {
                    index = pair.getValue();
                    String key = pair.getKey();
                    pair = getLengthPrefixedString(chars, index);
                    if (pair == null) {
                        return Status.Corruption("bad WriteBatch Put");
                    } else {
                        String value = pair.getKey();
                        index = pair.getValue();
                        handler.put(key, value);
                    }
                }
            } else {
                Pair<String, Integer> pair = getLengthPrefixedString(chars, index);
                if (pair == null) {
                    return Status.Corruption("bad WriteBatch Delete");
                } else {
                    index = pair.getValue();
                    handler.delete(pair.getKey());
                }
            }
        }

        if (found != this.getCount()) {
            return Status.Corruption("WriteBatch has wrong count");
        } else {
            return Status.OK();
        }
    }

    Pair<String, Integer> getLengthPrefixedString(char[] chars, int index) {
        return coding.getLengthPrefixedString(chars, index);
    }

    public void decode(char[] content) {
        assert content.length >= coding.getFixed64Length() + coding.getFixed32Length();
        this.buffer = Arrays.copyOfRange(content, 0, kHeaderSize);
        this.builder = new StringBuilder(String.valueOf(Arrays.copyOfRange(content, kHeaderSize, content.length)));
    }

    public char[] encode() {
        char[] chars = new char[this.buffer.length + this.builder.length()];
        StringBuilder tmp = new StringBuilder().append(this.buffer).append(this.builder);
        tmp.getChars(0, tmp.length(), chars, 0);
        return chars;
    }
}
