package com.farmerworking.leveldb.in.java.common;

public interface IHash {
    static IHash instance = getDefaultImpl();

    int hash(char[] data, int seed);

    static IHash getDefaultImpl() {
        return new Hash();
    }

    static IHash getInstance() {
        return instance;
    }
}
