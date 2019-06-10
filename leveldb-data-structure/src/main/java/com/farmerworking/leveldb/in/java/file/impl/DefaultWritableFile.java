package com.farmerworking.leveldb.in.java.file.impl;

import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.common.ByteUtils;
import com.farmerworking.leveldb.in.java.file.WritableFile;

import java.io.FileOutputStream;
import java.io.IOException;

public class DefaultWritableFile implements WritableFile {
    private final FileOutputStream fileOutputStream;

    public DefaultWritableFile(FileOutputStream fileOutputStream) {
        this.fileOutputStream = fileOutputStream;
    }

    @Override
    public Status append(String data) {
        try {
            fileOutputStream.write(ByteUtils.toByteArray(data));
            return Status.OK();
        } catch (IOException e) {
            return Status.IOError(e.getMessage());
        }
    }

    @Override
    public Status close() {
        try {
            fileOutputStream.close();
            return Status.OK();
        } catch (IOException e) {
            return Status.IOError(e.getMessage());
        }
    }

    @Override
    public Status flush() {
        try {
            fileOutputStream.flush();
            return Status.OK();
        } catch (IOException e) {
            return Status.IOError(e.getMessage());
        }
    }

    @Override
    public Status sync() {
        try {
            fileOutputStream.getFD().sync();
            return Status.OK();
        } catch (IOException e) {
            return Status.IOError(e.getMessage());
        }
    }
}
