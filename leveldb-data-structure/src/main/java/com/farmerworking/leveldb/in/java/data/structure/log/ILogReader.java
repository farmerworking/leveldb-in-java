package com.farmerworking.leveldb.in.java.data.structure.log;

import javafx.util.Pair;

public interface ILogReader {
    long lastRecordOffset();

    Pair<Boolean, String> readRecord();
}
