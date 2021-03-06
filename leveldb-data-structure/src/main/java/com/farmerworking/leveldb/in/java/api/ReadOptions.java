package com.farmerworking.leveldb.in.java.api;

import com.farmerworking.leveldb.in.java.data.structure.db.Snapshot;
import lombok.Data;

@Data
public class ReadOptions {
    // If true, all data read from underlying storage will be
    // verified against corresponding checksums.
    // Default: false
    boolean verifyChecksums = false;

    Snapshot snapshot = null;
}
