package com.farmerworking.leveldb.in.java.common;

public class CodingTest extends ICodingTest {
    @Override
    protected ICoding getImpl() {
        return new Coding();
    }
}
