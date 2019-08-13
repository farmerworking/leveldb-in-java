package com.farmerworking.leveldb.in.java.data.structure.version;

import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKey;
import lombok.Data;

import java.util.Objects;

@Data
public class FileMetaData {
    private int refs = 0;
    private int allowedSeeks = 1 << 30; // Seeks allowed until compaction
    private long fileNumber;
    private long fileSize = 0; // File size in bytes
    private InternalKey smallest = new InternalKey(); // Smallest internal key served by table
    private InternalKey largest = new InternalKey();  // Largest internal key served by table

    public FileMetaData() {
    }

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FileMetaData metaData = (FileMetaData) o;
        return fileNumber == metaData.fileNumber &&
                fileSize == metaData.fileSize &&
                Objects.equals(smallest, metaData.smallest) &&
                Objects.equals(largest, metaData.largest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileNumber, fileSize, smallest, largest);
    }
}
