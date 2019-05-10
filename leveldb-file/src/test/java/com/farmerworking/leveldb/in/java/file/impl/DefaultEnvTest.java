package com.farmerworking.leveldb.in.java.file.impl;

import com.farmerworking.leveldb.in.java.file.Env;
import com.farmerworking.leveldb.in.java.file.EnvTest;

public class DefaultEnvTest extends EnvTest {
    @Override
    protected Env getImpl() {
        return new DefaultEnv();
    }
}
