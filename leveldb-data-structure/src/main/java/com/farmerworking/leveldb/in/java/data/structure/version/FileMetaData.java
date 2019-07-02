package com.farmerworking.leveldb.in.java.data.structure.version;

import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKey;
import lombok.Data;

@Data
public class FileMetaData {
    private int refs = 0;
    private int allowedSeeks = 1 << 30; // Seeks allowed until compaction
    private long fileNumber;
    private long fileSize = 0; // File size in bytes
    private InternalKey smallest; // Smallest internal key served by table
    private InternalKey largest;  // Largest internal key served by table

    public FileMetaData(long fileNumber, long fileSize, InternalKey smallest, InternalKey largest) {
        this.fileNumber = fileNumber;
        this.fileSize = fileSize;
        this.smallest = smallest;
        this.largest = largest;
    }

    public FileMetaData(FileMetaData metaData) {
        this.refs = metaData.refs;
        this.allowedSeeks = metaData.allowedSeeks;
        this.fileNumber = metaData.fileNumber;
        this.fileSize = metaData.fileSize;
        this.smallest = metaData.smallest;
        this.largest = metaData.largest;
    }
}
