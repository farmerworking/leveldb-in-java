package com.farmerworking.leveldb.in.java.data.structure.table;

import com.farmerworking.leveldb.in.java.api.Comparator;
import lombok.Data;

@Data
public class GetSaver {
    // provided
    private Comparator userComparator;
    private String userKey;

    // assigned
    private GetState state = GetState.kNotFound;
    private String value;

    public GetSaver(String userKey, Comparator userComparator) {
        this.userKey = userKey;
        this.userComparator = userComparator;
    }
}
