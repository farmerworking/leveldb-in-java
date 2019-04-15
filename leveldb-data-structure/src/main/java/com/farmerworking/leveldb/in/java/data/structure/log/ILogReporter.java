package com.farmerworking.leveldb.in.java.data.structure.log;

import com.farmerworking.leveldb.in.java.common.Status;

public interface ILogReporter {
    void corruption(long bytes, Status status);
}
