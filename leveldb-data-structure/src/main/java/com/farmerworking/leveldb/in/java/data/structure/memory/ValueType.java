package com.farmerworking.leveldb.in.java.data.structure.memory;

public enum ValueType {
    kTypeDeletion(0), kTypeValue(1);

    private int value;

    ValueType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static ValueType valueOf(int value) {
        if (value == 0) {
            return kTypeDeletion;
        } else if (value == 1) {
            return kTypeValue;
        } else {
            return null;
        }
    }
}
