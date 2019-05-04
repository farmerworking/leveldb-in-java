package com.farmerworking.leveldb.in.java.data.structure.utils;

import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.file.RandomAccessFile;
import javafx.util.Pair;

public class StringRandomAccessSource implements RandomAccessFile {
    private final String content;

    public StringRandomAccessSource(String content) {
        this.content = content;
    }

    @Override
    public Pair<Status, String> read(long offset, int n) {
        if (offset > content.length()) {
            return new Pair<>(Status.InvalidArgument("invalid Read offset"), null);
        }
        if (offset + n > content.length()) {
            n = (int) (content.length() - offset);
        }

        String result = content.substring((int)offset, (int)offset + n);
        return new Pair<>(Status.OK(), result);
    }

    public String getContent() {
        return content;
    }
}
