package com.farmerworking.leveldb.in.java.data.structure.db;

import com.farmerworking.leveldb.in.java.api.Options;
import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.data.structure.log.ILogReporter;
import com.farmerworking.leveldb.in.java.file.Env;

class LogReporter implements ILogReporter {
    private Options.Logger infoLog;
    private String fileName;
    private Status status;

    public LogReporter(Options.Logger infoLog, String filename, Status status) {
        this.infoLog = infoLog;
        this.fileName = filename;
        this.status = status;
    }

    @Override
    public void corruption(long bytes, Status status) {
        Options.Logger.log(this.infoLog, String.format("%s%s: dropping %d bytes; %s",
                (this.status == null ? "(ignoring error) " : "")
                , this.fileName, (int)bytes, status.toString()));

        if (this.status != null && this.status.isOk()) {
            this.status.setMessage(status.getMessage());
            this.status.setCode(status.getCode());
        }
    }
}
