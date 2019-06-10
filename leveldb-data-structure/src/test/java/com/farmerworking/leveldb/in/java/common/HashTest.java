package com.farmerworking.leveldb.in.java.common;

public class HashTest extends IHashTest {
    @Override
    protected IHash getImpl() {
        return new Hash();
    }
}
