package com.farmerworking.leveldb.in.java.data.structure.db;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import com.farmerworking.leveldb.in.java.api.Iterator;
import com.farmerworking.leveldb.in.java.api.Options;
import com.farmerworking.leveldb.in.java.api.ReadOptions;
import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.api.WriteOptions;
import com.farmerworking.leveldb.in.java.data.structure.writebatch.WriteBatch;
import com.farmerworking.leveldb.in.java.data.structure.writebatch.WriteBatchIterateHandler;
import javafx.util.Pair;

public class ModelDB implements DB {
    private final Options options;
    private SortedMap<String, String> map;

    public ModelDB(Options currentOptions) {
        this.options = currentOptions;
        this.map = new TreeMap<>(new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return options.getComparator().compare(o1.toCharArray(), o2.toCharArray());
            }
        });
    }

    class ModelDBHandler implements WriteBatchIterateHandler {
        private SortedMap<String, String> map;

        public ModelDBHandler(SortedMap<String, String> map) {
            this.map = map;
        }

        @Override
        public void put(String key, String value) {
            map.put(key, value);
        }

        @Override
        public void delete(String key) {
            map.remove(key);
        }
    }

    @Override
    public Status write(WriteOptions writeOptions, WriteBatch batch) {
        return batch.iterate(new ModelDBHandler(this.map));
    }

    @Override
    public Iterator<String, String> iterator(ReadOptions readOptions) {
        if (readOptions.getSnapshot() == null) {
            return new ModelIter(new TreeMap<>(map));
        } else {
            SortedMap<String, String> state = ((ModelSnapshot)readOptions.getSnapshot()).getMap();
            return new ModelIter(state);
        }
    }

    @Override
    public Pair<Status, String> get(ReadOptions readOptions, String key) {
        assert false; // not implemented
        return null;
    }

    @Override
    public Status put(WriteOptions writeOptions, String key, String value) {
        WriteBatch batch = new WriteBatch();
        batch.put(key, value);
        return write(writeOptions, batch);
    }

    @Override
    public Status delete(WriteOptions writeOptions, String key) {
        WriteBatch batch = new WriteBatch();
        batch.delete(key);
        return write(writeOptions, batch);
    }

    @Override
    public Pair<Boolean, String> getProperty(String property) {
        return null;
    }

    @Override
    public int numLevelFiles(int level) {
        return 0;
    }

    @Override
    public Snapshot getSnapshot() {
        ModelSnapshot snapshot = new ModelSnapshot(this.map);
        return snapshot;
    }

    @Override
    public void releaseSnapshot(Snapshot snapshot) {

    }

    @Override
    public void compactRange(String begin, String end) {

    }

    @Override
    public void close() {

    }

    @Override
    public Status TEST_compactMemtable() {
        return null;
    }

    @Override
    public void TEST_compactRange(int level, String begin, String end) {

    }

    @Override
    public Iterator<String, String> TEST_newInternalIterator() {
        return null;
    }

    @Override
    public long TEST_maxNextLevelOverlappingBytes() {
        return 0;
    }

    @Override
    public List<Long> getApproximateSizes(List<Pair<String, String>> range, int n) {
        return null;
    }
}
