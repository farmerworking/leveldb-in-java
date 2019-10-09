package com.farmerworking.leveldb.in.java.file.impl;

import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.common.ByteUtils;
import com.farmerworking.leveldb.in.java.file.RandomAccessFile;
import javafx.util.Pair;

import java.io.IOException;

public class DefaultRandomAccessFile implements RandomAccessFile {
    private final java.io.RandomAccessFile randomAccessFile;

    public DefaultRandomAccessFile(java.io.RandomAccessFile randomAccessFile) {
        this.randomAccessFile = randomAccessFile;
    }

    @Override
    public synchronized Pair<Status, String> read(long offset, int n) {
        try {
            randomAccessFile.seek(offset);

            byte[] bytes = new byte[n];
            randomAccessFile.read(bytes);
            return new Pair<>(Status.OK(), new String(ByteUtils.toCharArray(bytes)));
        } catch (IOException e) {
            return new Pair<>(Status.IOError(e.getMessage()), null);
        }
    }
}
