package com.farmerworking.leveldb.in.java.api;

import lombok.Data;

@Data
public class ReadOptions {
    // If true, all data read from underlying storage will be
    // verified against corresponding checksums.
    // Default: false
    boolean verifyChecksums = false;

    Long snapshot = null;
}
