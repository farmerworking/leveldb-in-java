package com.farmerworking.leveldb.in.java.data.structure.memory;

import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.api.Iterator;
import com.farmerworking.leveldb.in.java.data.structure.writebatch.MemTableInserter;
import com.farmerworking.leveldb.in.java.data.structure.writebatch.WriteBatch;
import javafx.util.Pair;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import static org.junit.Assert.*;

public abstract class IMemtableTest {
    protected abstract IMemtable getImpl();

    private void testValue(IMemtable memtable, String userKey, long sequence, String value) {
        Pair<Boolean, Pair<Status, String>> result = memtable.get(userKey, sequence);
        assertEquals(true, result.getKey());
        assertTrue(result.getValue().getKey().isOk());
        assertEquals(value, result.getValue().getValue());
    }

    @Test
    public void testEmpty() {
        IMemtable memtable = getImpl();
        Pair<Boolean, Pair<Status, String>> result = memtable.get("empty", 1L);
        assertEquals(false, result.getKey());
    }

    @Test
    public void testNotFound() {
        IMemtable memtable = getImpl();
        memtable.add(1L, ValueType.kTypeDeletion, "empty", "value");
        Pair<Boolean, Pair<Status, String>> result = memtable.get("empty", 1L);
        assertEquals(true, result.getKey());
        assertTrue(result.getValue().getKey().IsNotFound());
    }

    @Test
    public void testAddGet() {
        IMemtable memtable = getImpl();
        memtable.add(1L, ValueType.kTypeValue, "", "value");
        memtable.add(1L, ValueType.kTypeValue, "key", "");

        memtable.add(1L, ValueType.kTypeValue, "same key1", "one");
        memtable.add(1L, ValueType.kTypeValue, "same key2", "two");

        memtable.add(1L, ValueType.kTypeValue, "same key", "1");
        memtable.add(2L, ValueType.kTypeValue, "same key", "2");
        memtable.add(3L, ValueType.kTypeValue, "same key", "3");

        memtable.add(1L, ValueType.kTypeValue, StringUtils.repeat("largekey", 100), "largekey");
        memtable.add(1L, ValueType.kTypeValue, "largevalue", StringUtils.repeat("largevalue", 100));
        memtable.add(1L, ValueType.kTypeValue, StringUtils.repeat("largekeyvalue", 100), StringUtils.repeat("largekeyvalue", 100));

        // empty key
        testValue(memtable, "", 1L, "value");
        // empty value
        testValue(memtable, "key", 1L, "");
        // same key, same sequence, different value -- update
        testValue(memtable, "same key1", 1L, "one");
        testValue(memtable, "same key2", 1L, "two");
        // same key, different sequence, different value
        testValue(memtable, "same key", 1L, "1");
        testValue(memtable, "same key", 2L, "2");
        testValue(memtable, "same key", 3L, "3");
        // large key or value
        testValue(memtable, StringUtils.repeat("largekey", 100), 1L, "largekey");
        testValue(memtable, "largevalue", 1L, StringUtils.repeat("largevalue", 100));
        testValue(memtable, StringUtils.repeat("largekeyvalue", 100), 1L, StringUtils.repeat("largekeyvalue", 100));
    }

    @Test
    public void testMemoryUsage() {
        IMemtable memtable = getImpl();
        long before = memtable.approximateMemoryUsage();
        memtable.add(1L, ValueType.kTypeValue, StringUtils.repeat("largekey", 100), "largekey");
        memtable.add(1L, ValueType.kTypeValue, "largevalue", StringUtils.repeat("largevalue", 100));
        memtable.add(1L, ValueType.kTypeValue, StringUtils.repeat("largekeyvalue", 100), StringUtils.repeat("largekeyvalue", 100));
        assertTrue(memtable.approximateMemoryUsage() > before);
    }

    @Test
    public void testIteratorEmpty() {
        IMemtable memtable = getImpl();
        Iterator iter = memtable.iterator();
        iter.seekToFirst();
        assertFalse(iter.valid());
    }

    @Test
    public void testIterator() {
        IMemtable memtable = getImpl();
        String[] values = new String[]{
                "", "2", "3", "4", "5", "one", "two", StringUtils.repeat("largevalue", 100)
        };
        for (int i = 0; i < values.length; i++) {
            memtable.add(1L, ValueType.kTypeValue, i + "", values[i]);
        }

        Iterator<String, String> iter = memtable.iterator();
        iter.seekToFirst();
        assertTrue(iter.valid());

        for (int i = 0; i < values.length; i++) {
            assertEquals(String.valueOf(i), InternalKey.extractUserKey(iter.key()));
            assertEquals(values[i], iter.value());
            iter.next();
        }
        assertFalse(iter.valid());

        iter.seek(new InternalKey("1", 1L, ValueType.kTypeValue).encode());
        assertTrue(iter.valid());
        assertEquals("2", iter.value());

        iter.next();
        assertTrue(iter.valid());
        assertEquals("3", iter.value());

        iter.next();
        assertTrue(iter.valid());
        assertEquals("4", iter.value());
    }

    @Test
    public void testSimple() {
        IMemtable memtable = getImpl();
        WriteBatch batch = new WriteBatch();
        batch.setSequence(100);
        batch.put("k1", "v1");
        batch.put("k2", "v2");
        batch.put("k3", "v3");
        batch.put("largekey", "vlarge");

        MemTableInserter memTableInserter = new MemTableInserter(batch.getSequence(), memtable);
        Status status = batch.iterate(memTableInserter);
        assertTrue(status.isOk());

        Iterator<String, String> iter = memtable.iterator();
        iter.seekToFirst();
        while (iter.valid()) {
            System.out.println(String.format("key: '%s' -> '%s'", InternalKey.extractUserKey(iter.key()), iter.value()));
            iter.next();
        }
    }
}