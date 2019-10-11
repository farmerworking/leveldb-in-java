package com.farmerworking.leveldb.in.java.data.structure.db;

import lombok.Data;

@Data
public class SnapshotImpl implements Snapshot{
    private long sequence;

    public SnapshotImpl(long sequence) {
        this.sequence = sequence;
    }
}
