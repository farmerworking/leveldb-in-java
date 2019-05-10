package com.farmerworking.leveldb.in.java.file.impl;

import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.common.ByteUtils;
import com.farmerworking.leveldb.in.java.file.SequentialFile;
import javafx.util.Pair;

import java.io.FileInputStream;
import java.io.IOException;

public class DefaultSequentialFile implements SequentialFile {
    private final FileInputStream fileInputStream;

    public DefaultSequentialFile(FileInputStream fileInputStream) {
        this.fileInputStream = fileInputStream;
    }

    @Override
    public Pair<Status, String> read(int n) {
        try {
            byte[] bytes = new byte[n];
            int count = this.fileInputStream.read(bytes);
            if (count == -1) {
                return new Pair<>(Status.OK(), "");
            } else {
                return new Pair<>(Status.OK(), new String(ByteUtils.toCharArray(bytes, 0, count)));
            }
        } catch (IOException e) {
            return new Pair<>(Status.IOError(e.getMessage()), null);
        }
    }

    @Override
    public Status skip(long n) {
        try {
            this.fileInputStream.skip(n);
            return Status.OK();
        } catch (IOException e) {
            return Status.IOError(e.getMessage());
        }
    }
}
