package com.farmerworking.leveldb.in.java.data.structure.db;

import java.util.SortedMap;
import java.util.TreeMap;

import lombok.Data;

@Data
public class ModelSnapshot implements Snapshot {
    private SortedMap<String, String> map;

    public ModelSnapshot(SortedMap<String, String> map) {
        this.map = new TreeMap<>(map);
    }
}
