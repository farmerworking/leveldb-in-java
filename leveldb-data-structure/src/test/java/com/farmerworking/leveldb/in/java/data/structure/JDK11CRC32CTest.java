package com.farmerworking.leveldb.in.java.data.structure;

public class JDK11CRC32CTest extends ICRC32CTest {
    @Override
    protected ICRC32C getImpl() {
        return new JDK11CRC32C();
    }
}
