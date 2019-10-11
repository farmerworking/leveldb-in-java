package com.farmerworking.leveldb.in.java.data.structure.db;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.Data;

@Data
public class MultiThreadState {
    public static int kNumThread = 4;
    public static int kTestSeconds = 10;
    public static int kNumKeys = 1000;

    private DBTest dbTest;
    private AtomicBoolean stop = new AtomicBoolean(false);

    private AtomicInteger[] counter = new AtomicInteger[]{
        new AtomicInteger(0),
        new AtomicInteger(0),
        new AtomicInteger(0),
        new AtomicInteger(0)
    };

    private AtomicBoolean[] threadDone = new AtomicBoolean[]{
        new AtomicBoolean(false),
        new AtomicBoolean(false),
        new AtomicBoolean(false),
        new AtomicBoolean(false)
    };
}
