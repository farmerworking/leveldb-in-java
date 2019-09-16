package com.farmerworking.leveldb.in.java.data.structure.db;

import com.farmerworking.leveldb.in.java.api.*;
import com.farmerworking.leveldb.in.java.data.structure.harness.Constructor;
import com.farmerworking.leveldb.in.java.data.structure.writebatch.WriteBatch;
import com.farmerworking.leveldb.in.java.file.impl.DefaultEnv;
import javafx.util.Pair;
import static org.junit.Assert.*;

import java.util.Map;

public class DBConstructor extends Constructor {
    private static long number = 0;
    private DB db;

    public DBConstructor(Comparator comparator) {
        super(comparator);
        this.db = null;
        newDB();
    }

    @Override
    public Iterator<String, String> iterator() {
        return this.db.iterator(new ReadOptions());
    }

    @Override
    public Status finishImpl(Options options, Map<String, String> data) {
        for (String key : data.keySet()) {
            WriteBatch batch = new WriteBatch();
            batch.put(key, data.get(key));
            Status status = db.write(new WriteOptions(), batch);
            assertTrue(status.isOk());
        }
        return Status.OK();
    }

    private void newDB() {
        Pair<Status, String> pair = new DefaultEnv().getTestDirectory();
        String name = pair.getValue() + "/table_testdb" + (number++);

        Options options = new Options();
        options.setComparator(comparator);
        Status status = DB.destroyDB(name, options);
        assertTrue(status.isOk());

        options.setCreateIfMissing(true);
        options.setErrorIfExists(true);
        options.setWriteBufferSize(10000); // Something small to force merging
        Pair<Status, DB> open = DB.open(options, name);
        status = open.getKey();
        this.db = open.getValue();
        assertTrue(status.isOk());
    }
}
