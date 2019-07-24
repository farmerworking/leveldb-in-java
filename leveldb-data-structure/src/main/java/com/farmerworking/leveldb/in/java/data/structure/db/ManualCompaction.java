package com.farmerworking.leveldb.in.java.data.structure.db;

import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKey;
import lombok.Data;

@Data
public class ManualCompaction {
    private int level;
    private boolean done;
    private InternalKey begin;
    private InternalKey end;
    private InternalKey tmpStorage;
}
