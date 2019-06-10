package com.farmerworking.leveldb.in.java.file;

import com.farmerworking.leveldb.in.java.api.Status;
import javafx.util.Pair;

public interface Env {
    Pair<Status, WritableFile> newWritableFile(String filename) ;

    Pair<Status, WritableFile> newAppendableFile(String filename) ;

    Pair<Status, RandomAccessFile> newRandomAccessFile(String filename);

    Pair<Status, SequentialFile> newSequentialFile(String filename);

    Pair<Status, String> getTestDirectory();

    Pair<Status, Boolean> delete(String filename);

    Pair<Status, Boolean> isFileExists(String filename);
}
