package com.farmerworking.leveldb.in.java.data.structure.utils;

import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.file.WritableFile;

public class StringDest implements WritableFile {
    StringBuilder builder = new StringBuilder();

    @Override
    public Status append(String data) {
        builder.append(data);
        return Status.OK();
    }

    public void replace(int index, char newChar) {
        builder.setCharAt(index, newChar);
    }

    public int getLength() {
        return builder.length();
    }

    public String getContent() {
        return builder.toString();
    }

    public void setContent(String content) {
        this.builder = new StringBuilder(content);
    }

    @Override
    public Status flush() { return Status.OK(); }
    @Override
    public Status close() { return Status.OK(); }
    @Override
    public Status sync() { return Status.OK(); }
}
