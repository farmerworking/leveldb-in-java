package com.farmerworking.leveldb.in.java.data.structure.db;

import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.data.structure.writebatch.WriteBatch;
import lombok.Data;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

@Data
public class Writer {
    private Status status;
    private WriteBatch batch;
    private boolean sync;
    private boolean done;
    private Condition condition;

    public Writer(Lock lock) {
        this.condition = lock.newCondition();
    }

    public boolean isNotDone() {
        return !done;
    }

    public boolean isNotSync() {
        return !isSync();
    }
}
