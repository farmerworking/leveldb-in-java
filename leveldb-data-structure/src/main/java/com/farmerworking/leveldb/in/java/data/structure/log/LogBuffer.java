package com.farmerworking.leveldb.in.java.data.structure.log;

public class LogBuffer {
    private StringBuffer buffer;
    private int bufferOffset;

    public LogBuffer() {
        this.buffer = new StringBuffer(RecordType.kBlockSize);
        this.bufferOffset = 0;
    }

    void clear() {
        this.buffer.setLength(0);
        this.bufferOffset = 0;
    }

    int remain() {
        return this.buffer.length() - bufferOffset;
    }

    void set(String str) {
        clear();
        this.buffer.append(str);
    }

    void seek(int size) {
        bufferOffset += size;
    }

    char[] getChars(int offset, int size) {
        char[] result = new char[size];
        offset = bufferOffset + offset;
        this.buffer.getChars(offset, offset + size, result, 0);
        return result;
    }
}
