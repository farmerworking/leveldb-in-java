package com.farmerworking.leveldb.in.java.data.structure;

public class CodingTest extends ICodingTest {
    @Override
    protected ICoding getImpl() {
        return new Coding();
    }
}
