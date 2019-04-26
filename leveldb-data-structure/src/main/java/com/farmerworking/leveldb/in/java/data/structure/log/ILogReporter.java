package com.farmerworking.leveldb.in.java.data.structure.log;

import com.farmerworking.leveldb.in.java.api.Status;

public interface ILogReporter {
    void corruption(long bytes, Status status);
}
