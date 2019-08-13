package com.farmerworking.leveldb.in.java.data.structure.db;

import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKey;
import lombok.Data;

@Data
public class Output {
    private long number;
    private long fileSize;
    private InternalKey smallest = new InternalKey();
    private InternalKey largest = new InternalKey();

    public Output(long number) {
        this.number = number;
    }
}
