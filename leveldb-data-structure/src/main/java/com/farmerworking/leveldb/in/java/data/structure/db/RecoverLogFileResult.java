package com.farmerworking.leveldb.in.java.data.structure.db;

import com.farmerworking.leveldb.in.java.api.Status;
import lombok.Data;

@Data
class RecoverLogFileResult {
    private Status status;
    private boolean saveManifest;
    private long maxSequence;

    public RecoverLogFileResult(Status status) {
        this.status = status;
    }

    public RecoverLogFileResult(Status status, boolean saveManifest, long maxSequence) {
        this.status = status;
        this.saveManifest = saveManifest;
        this.maxSequence = maxSequence;
    }
}
