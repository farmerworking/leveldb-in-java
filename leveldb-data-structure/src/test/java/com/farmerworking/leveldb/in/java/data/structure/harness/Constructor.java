package com.farmerworking.leveldb.in.java.data.structure.harness;

import com.farmerworking.leveldb.in.java.api.Comparator;
import com.farmerworking.leveldb.in.java.api.Iterator;
import com.farmerworking.leveldb.in.java.api.Options;
import com.farmerworking.leveldb.in.java.api.Status;

import java.util.*;

public abstract class Constructor {
    protected TreeMap<String, String> data;
    protected final Comparator comparator;

    public Constructor(Comparator comparator) {
        this.comparator = comparator;
        this.data = new TreeMap<>(new StringComparator(comparator));
    }

    public TreeMap<String, String> getData() {
        return data;
    }

    public abstract Iterator<String, String> iterator();

    public abstract Status finishImpl(Options options, Map<String, String> data);

    public void add(String key, String value) {
        getData().put(key, value);
    }

    List<String> finish(Options options) {
        List<String> result = new ArrayList<>(getData().keySet());
        Status status = finishImpl(options, getData());
        assert status.isOk();
        return result;
    }
}
