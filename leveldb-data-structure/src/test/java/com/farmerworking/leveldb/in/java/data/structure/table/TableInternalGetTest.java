package com.farmerworking.leveldb.in.java.data.structure.table;

import com.farmerworking.leveldb.in.java.api.*;
import com.farmerworking.leveldb.in.java.common.TestUtils;
import com.farmerworking.leveldb.in.java.data.structure.block.BlockReader;
import com.farmerworking.leveldb.in.java.data.structure.block.EmptyIterator;
import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKey;
import com.farmerworking.leveldb.in.java.data.structure.memory.ValueType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class TableInternalGetTest {
    abstract TableConstructor getTableConstructor(Comparator userComparator, Options options);

    @Test
    public void testInternalGetFoundCase() {
        Options options = new Options();
        Comparator userComparator = new BytewiseComparator();
        TableConstructor constructor = getTableConstructor(userComparator, options);
        long sequence = 1L;

        List<InternalKey> internalKeys = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            InternalKey key = new InternalKey(TestUtils.randomKey(5), sequence ++, ValueType.kTypeValue);
            internalKeys.add(key);
            constructor.add(key.encode(), TestUtils.randomString(10));
        }

        constructor.finish(options);
        Map<String, String> data = constructor.getData();

        ITableReader tableReader = constructor.getItableReader();
        ReadOptions readOptions = new ReadOptions();
        for(InternalKey key : internalKeys) {
            GetSaver saver = new GetSaver(key.userKey(), userComparator);
            Status status = tableReader.internalGet(readOptions, key.encode(), saver);
            assertTrue(status.isOk());
            assertEquals(GetState.kFound, saver.getState());
            assertEquals(key.userKey(), saver.getUserKey());
            assertEquals(data.get(key.encode()), saver.getValue());
        }
    }

    @Test
    public void testInternalGetCorruptCase() {
        Options options = new Options();
        Comparator userComparator = new BytewiseComparator();
        TableConstructor constructor = getTableConstructor(userComparator, options);
        constructor.add("a", TestUtils.randomString(10));

        constructor.finish(options);

        ITableReader tableReader = constructor.getItableReader();
        ReadOptions readOptions = new ReadOptions();

        GetSaver saver = new GetSaver("a", userComparator);
        Status status = tableReader.internalGet(readOptions, "a", saver);
        assertTrue(status.isOk());
        assertEquals(GetState.kCorrupt, saver.getState());
    }

    @Test
    public void testInternalGetDeletedCase() {
        Options options = new Options();
        Comparator userComparator = new BytewiseComparator();
        TableConstructor constructor = getTableConstructor(userComparator, options);
        String key = new InternalKey("a", 1L, ValueType.kTypeDeletion).encode();
        constructor.add(key, TestUtils.randomString(10));

        constructor.finish(options);

        ITableReader tableReader = constructor.getItableReader();
        ReadOptions readOptions = new ReadOptions();

        GetSaver saver = new GetSaver("a", userComparator);
        Status status = tableReader.internalGet(readOptions, key, saver);
        assertTrue(status.isOk());
        assertEquals(GetState.kDeleted, saver.getState());
    }

    @Test
    public void testInternalGetNotFoundWhereAllKeysAreSmaller() {
        Options options = new Options();
        Comparator userComparator = new BytewiseComparator();
        TableConstructor constructor = getTableConstructor(userComparator, options);
        long sequence = 1L;

        List<InternalKey> internalKeys = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            InternalKey key = new InternalKey(TestUtils.randomKey(5), sequence ++, ValueType.kTypeValue);
            internalKeys.add(key);
            constructor.add(key.encode(), TestUtils.randomString(10));
        }

        constructor.finish(options);
        Map<String, String> data = constructor.getData();

        ITableReader tableReader = constructor.getItableReader();
        ReadOptions readOptions = new ReadOptions();
        for(InternalKey key : internalKeys) {
            GetSaver saver = new GetSaver(key.userKey(), userComparator);
            Status status = tableReader.internalGet(readOptions, key.encode(), saver);
            assertTrue(status.isOk());
            assertEquals(GetState.kFound, saver.getState());
            assertEquals(key.userKey(), saver.getUserKey());
            assertEquals(data.get(key.encode()), saver.getValue());
        }

        String key = new String(new char[]{255, 255, 255, 255, 255, 255});
        GetSaver saver = new GetSaver(key, userComparator);
        Status status = tableReader.internalGet(readOptions, new InternalKey(key, 1L, ValueType.kTypeValue).encode(), saver);
        assertTrue(status.isOk());
        assertEquals(GetState.kNotFound, saver.getState());
    }

    @Test
    public void testInternalGetWhereExistKeyLarger() {
        Options options = new Options();
        Comparator userComparator = new BytewiseComparator();
        TableConstructor constructor = getTableConstructor(userComparator, options);
        long sequence = 1L;

        List<InternalKey> internalKeys = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            InternalKey key = new InternalKey(TestUtils.randomKey(5), sequence ++, ValueType.kTypeValue);
            internalKeys.add(key);
            constructor.add(key.encode(), TestUtils.randomString(10));
        }

        constructor.finish(options);
        Map<String, String> data = constructor.getData();

        ITableReader tableReader = constructor.getItableReader();
        ReadOptions readOptions = new ReadOptions();
        for(InternalKey key : internalKeys) {
            GetSaver saver = new GetSaver(key.userKey(), userComparator);
            Status status = tableReader.internalGet(readOptions, key.encode(), saver);
            assertTrue(status.isOk());
            assertEquals(GetState.kFound, saver.getState());
            assertEquals(key.userKey(), saver.getUserKey());
            assertEquals(data.get(key.encode()), saver.getValue());
        }

        String key = new String(new char[]{0, 0, 0});
        GetSaver saver = new GetSaver(key, userComparator);
        Status status = tableReader.internalGet(readOptions, new InternalKey(key, 1L, ValueType.kTypeValue).encode(), saver);
        assertTrue(status.isOk());
        assertEquals(GetState.kNotFound, saver.getState());
    }

    @Test
    public void testInternalGetErrorStatus() {
        Options options = new Options();
        Comparator userComparator = new BytewiseComparator();
        TableConstructor constructor = getTableConstructor(userComparator, options);
        long sequence = 1L;

        List<InternalKey> internalKeys = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            InternalKey key = new InternalKey(TestUtils.randomKey(5), sequence ++, ValueType.kTypeValue);
            internalKeys.add(key);
            constructor.add(key.encode(), TestUtils.randomString(10));
        }

        List<String> keys = constructor.finish(options);
        Map<String, String> data = constructor.getData();

        TableReader tableReader = (TableReader) constructor.getItableReader();

        ReadOptions readOptions = new ReadOptions();
        for(InternalKey key : internalKeys) {
            GetSaver saver = new GetSaver(key.userKey(), userComparator);
            Status status = tableReader.internalGet(readOptions, key.encode(), saver);
            assertTrue(status.isOk());
            assertEquals(GetState.kFound, saver.getState());
            assertEquals(key.userKey(), saver.getUserKey());
            assertEquals(data.get(key.encode()), saver.getValue());
        }

        String key = keys.get(0);

        // data error
        TableIndexTransfer mockTransfer = mock(TableIndexTransfer.class);
        when(mockTransfer.transfer(any(ReadOptions.class), anyString())).thenReturn(new EmptyIterator(Status.Corruption("force data error")));
        tableReader.indexTransfer = mockTransfer;

        Status status = tableReader.internalGet(readOptions, key, null);
        assertTrue(status.IsCorruption());
        assertEquals("force data error", status.getMessage());

        // index error
        BlockReader mockReader = mock(BlockReader.class);
        when(mockReader.iterator(any(Comparator.class))).thenReturn(new EmptyIterator(Status.Corruption("force index error")));
        tableReader.indexBlockReader = mockReader;

        status = tableReader.internalGet(readOptions, key, null);
        assertTrue(status.IsCorruption());
        assertEquals("force index error", status.getMessage());
    }
}
