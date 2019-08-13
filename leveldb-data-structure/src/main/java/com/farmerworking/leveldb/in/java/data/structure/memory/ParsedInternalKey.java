package com.farmerworking.leveldb.in.java.data.structure.memory;

public class ParsedInternalKey {
    private String userKey;
    private long sequence;
    private ValueType valueType;

    private char[] userKeyChar;

    public String getUserKey() {
        return userKey;
    }

    public void setUserKey(String userKey) {
        this.userKey = userKey;
        this.userKeyChar = userKey.toCharArray();
    }

    public long getSequence() {
        return sequence;
    }

    public void setSequence(long sequence) {
        this.sequence = sequence;
    }

    public ValueType getValueType() {
        return valueType;
    }

    public void setValueType(ValueType valueType) {
        this.valueType = valueType;
    }

    public char[] getUserKeyChar() {
        return userKeyChar;
    }

    public void setUserKeyChar(char[] userKeyChar) {
        this.userKeyChar = userKeyChar;
    }
}
