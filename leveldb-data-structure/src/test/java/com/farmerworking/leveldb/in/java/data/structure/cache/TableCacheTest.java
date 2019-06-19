package com.farmerworking.leveldb.in.java.data.structure.cache;

import com.farmerworking.leveldb.in.java.api.*;
import com.farmerworking.leveldb.in.java.common.TestUtils;
import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKey;
import com.farmerworking.leveldb.in.java.data.structure.memory.ValueType;
import com.farmerworking.leveldb.in.java.data.structure.table.GetSaver;
import com.farmerworking.leveldb.in.java.data.structure.table.GetState;
import com.farmerworking.leveldb.in.java.data.structure.table.ITableBuilder;
import com.farmerworking.leveldb.in.java.data.structure.table.ITableReader;
import com.farmerworking.leveldb.in.java.file.Env;
import com.farmerworking.leveldb.in.java.file.FileName;
import com.farmerworking.leveldb.in.java.file.RandomAccessFile;
import com.farmerworking.leveldb.in.java.file.WritableFile;
import javafx.util.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class TableCacheTest {
    TableCache tableCache;
    ITableBuilder builder;
    Options options;
    String dbname;
    String fileName;

    String userKey;
    String key;
    String value;

    Long fileNumber;

    Comparator userComparator;

    @Before
    public void setUp() throws Exception {
        userComparator = new BytewiseComparator();

        options = new Options();
        Pair<Status, String> pair = options.getEnv().getTestDirectory();
        assertTrue(pair.getKey().isOk());
        dbname = pair.getValue();

        fileNumber = 1L;
        // table builder
        fileName = FileName.tableFileName(dbname, fileNumber);
        Pair<Status, WritableFile> filePair = options.getEnv().newWritableFile(fileName);
        assertTrue(filePair.getKey().isOk());
        builder = ITableBuilder.getDefaultImpl(options, filePair.getValue());

        userKey = TestUtils.randomKey(5);
        key = new InternalKey(userKey, 1L, ValueType.kTypeValue).encode();
        value = TestUtils.randomString(10);
        builder.add(key, value);

        Status status = builder.finish();
        assertTrue(status.isOk());

        // table cache
        tableCache = new TableCache(dbname, options, 1024);
    }

    @After
    public void tearDown() throws Exception {
        Pair<Status, Boolean> deletePair = options.getEnv().delete(fileName);
        assertTrue(deletePair.getKey().isOk());
        assertTrue(deletePair.getValue());
    }

    @Test
    public void testSimpleGet() {
        Pair<Integer, Integer> statPair = tableCache.stat();
        assertEquals(0, statPair.getKey().intValue());
        assertEquals(0, statPair.getValue().intValue());

        // get exist key
        GetSaver saver = new GetSaver(userKey, userComparator);
        Status status = tableCache.get(new ReadOptions(), fileNumber, builder.fileSize(), key, saver);
        assertTrue(status.isOk());
        assertEquals(GetState.kFound, saver.getState());
        assertEquals(value, saver.getValue());

        // get non-exist key
        saver = new GetSaver("two", userComparator);
        status = tableCache.get(new ReadOptions(), fileNumber, builder.fileSize(), new InternalKey("two", 1L, ValueType.kTypeValue).encode(), saver);
        assertTrue(status.isOk());
        assertEquals(GetState.kNotFound, saver.getState());
    }

    @Test
    public void testEvict() {
        Pair<Integer, Integer> statPair = tableCache.stat();
        assertEquals(0, statPair.getKey().intValue());
        assertEquals(0, statPair.getValue().intValue());

        Status status = tableCache.get(new ReadOptions(), fileNumber, builder.fileSize(), "three", new GetSaver("three", new BytewiseComparator()));
        statPair = tableCache.stat();
        assertEquals(0, statPair.getKey().intValue());
        assertEquals(1, statPair.getValue().intValue());

        status = tableCache.get(new ReadOptions(), fileNumber, builder.fileSize(), "three", new GetSaver("three", new BytewiseComparator()));
        statPair = tableCache.stat();
        assertEquals(1, statPair.getKey().intValue());
        assertEquals(1, statPair.getValue().intValue());

        tableCache.evict(fileNumber);
        status = tableCache.get(new ReadOptions(), fileNumber, builder.fileSize(), "four", new GetSaver("three", new BytewiseComparator()));
        statPair = tableCache.stat();
        assertEquals(1, statPair.getKey().intValue());
        assertEquals(2, statPair.getValue().intValue());

        status = tableCache.get(new ReadOptions(), fileNumber, builder.fileSize(), "five", new GetSaver("three", new BytewiseComparator()));
        statPair = tableCache.stat();
        assertEquals(2, statPair.getKey().intValue());
        assertEquals(2, statPair.getValue().intValue());
    }

    @Test
    public void testGetHandleRelease() {
        Pair<Status, CacheHandle<Pair<RandomAccessFile, ITableReader>>> findPair = tableCache.findTable(fileNumber, builder.fileSize());
        assertTrue(findPair.getKey().isOk());
        LRUCacheNode<Pair<RandomAccessFile, ITableReader>> handle = (LRUCacheNode<Pair<RandomAccessFile, ITableReader>>) findPair.getValue();
        int before = handle.getPins();
        Status status = tableCache.get(new ReadOptions(), fileNumber, builder.fileSize(), "six", new GetSaver("six", new BytewiseComparator()));
        assertEquals(before, handle.getPins());
    }

    @Test
    public void testGetWhenInternalGetError() {
        // pre check
        GetSaver saver = new GetSaver(userKey, userComparator);
        Status status = tableCache.get(new ReadOptions(), fileNumber, builder.fileSize(), key, saver);
        assertTrue(status.isOk());
        assertEquals(GetState.kFound, saver.getState());
        assertEquals(value, saver.getValue());

        // cause internal get error
        ITableReader mockTableReader = mock(ITableReader.class);
        when(mockTableReader.internalGet(any(ReadOptions.class), anyString(), any(GetSaver.class))).thenReturn(Status.Corruption("internal get force error"));

        ShardedLRUCache<Pair<RandomAccessFile, ITableReader>> spyCache = spy(tableCache.cache);
        doReturn(new Pair<>(null, mockTableReader)).when(spyCache).value(any(CacheHandle.class));
        tableCache.cache = spyCache;

        // verify
        saver = new GetSaver(userKey, userComparator);
        status = tableCache.get(new ReadOptions(), fileNumber, builder.fileSize(), key, saver);
        assertTrue(status.IsCorruption());
        assertEquals("internal get force error", status.getMessage());
    }

    @Test
    public void testGetFindTableError() {
        // pre check
        GetSaver saver = new GetSaver(userKey, userComparator);
        Status status = tableCache.get(new ReadOptions(), fileNumber, builder.fileSize(), key, saver);
        assertTrue(status.isOk());
        assertEquals(GetState.kFound, saver.getState());
        assertEquals(value, saver.getValue());

        // cause find table error
        TableCache spyTableCache = spy(tableCache);
        doReturn(new Pair<>(Status.Corruption("find table force error"), null)).when(spyTableCache).findTable(anyLong(), anyLong());

        // verify
        saver = new GetSaver(userKey, userComparator);
        status = spyTableCache.get(new ReadOptions(), fileNumber, builder.fileSize(), key, saver);
        assertTrue(status.IsCorruption());
        assertEquals("find table force error", status.getMessage());
    }

    @Test
    public void testSimpleIterator() {
        Iterator<String, String> iter = tableCache.iterator(new ReadOptions(), fileNumber, builder.fileSize());

        assertFalse(iter.valid());
        iter.seekToFirst();
        assertTrue(iter.valid());

        assertEquals(key, iter.key());
        assertEquals(value, iter.value());

        iter.next();
        assertFalse(iter.valid());
    }

    @Test
    public void testIteratorCleanup() {
        Pair<Status, CacheHandle<Pair<RandomAccessFile, ITableReader>>> findPair = tableCache.findTable(fileNumber, builder.fileSize());
        assertTrue(findPair.getKey().isOk());

        LRUCacheNode<Pair<RandomAccessFile, ITableReader>> handle = (LRUCacheNode<Pair<RandomAccessFile, ITableReader>>) findPair.getValue();
        int before = handle.getPins();
        Iterator<String, String> iter = tableCache.iterator(new ReadOptions(), fileNumber, builder.fileSize());
        assertEquals(before + 1, handle.getPins());
        iter.close();
        assertEquals(before, handle.getPins());
    }

    @Test
    public void testIteratorFindTableError() {
        // pre check
        Iterator<String, String> iter = tableCache.iterator(new ReadOptions(), fileNumber, builder.fileSize());
        assertTrue(iter.status().isOk());

        // cause find table error
        TableCache spyTableCache = spy(tableCache);
        doReturn(new Pair<>(Status.Corruption("find table force error"), null)).when(spyTableCache).findTable(anyLong(), anyLong());

        // pre check
        iter = spyTableCache.iterator(new ReadOptions(), fileNumber, builder.fileSize());
        assertTrue(iter.status().IsCorruption());
        assertEquals("find table force error", iter.status().getMessage());
    }

    @Test
    public void testFindTableSSTableSupport() {
        // table builder
        Long fileNumber = 2L;
        String fileName = FileName.SSTTableFileName(dbname, fileNumber);
        Pair<Status, WritableFile> filePair = options.getEnv().newWritableFile(fileName);
        assertTrue(filePair.getKey().isOk());
        ITableBuilder builder = ITableBuilder.getDefaultImpl(options, filePair.getValue());

        String userKey = TestUtils.randomKey(5);
        String key = new InternalKey(userKey, 1L, ValueType.kTypeValue).encode();
        String value = TestUtils.randomString(10);
        builder.add(key, value);

        Status status = builder.finish();
        assertTrue(status.isOk());

        TableCache tableCache = new TableCache(dbname, options, 1024);
        GetSaver saver = new GetSaver(userKey, userComparator);
        status = tableCache.get(new ReadOptions(), fileNumber, builder.fileSize(), key, saver);
        assertTrue(status.isOk());
        assertEquals(GetState.kFound, saver.getState());
        assertEquals(value, saver.getValue());
    }

    @Test
    public void testFindTableNewRandomAccessFileError() {
        Env spyEnv = spy(tableCache.options.getEnv());
        doReturn(new Pair<>(Status.Corruption("new random access file force error"), null)).when(spyEnv).newRandomAccessFile(anyString());

        tableCache.options.setEnv(spyEnv);
        Pair<Status, CacheHandle<Pair<RandomAccessFile, ITableReader>>> pair = tableCache.findTable(fileNumber, builder.fileSize());
        assertTrue(pair.getKey().isNotOk());
        assertEquals("new random access file force error", pair.getKey().getMessage());
    }

    @Test
    public void testFindTableTableReaderOpenError() {
        ITableReader mockTableReader = mock(ITableReader.class);
        when(mockTableReader.open(any(Options.class), any(RandomAccessFile.class), anyLong())).thenReturn(Status.Corruption("open force error"));

        TableCache spyTableCache = spy(tableCache);
        doReturn(mockTableReader).when(spyTableCache).newTableReader();

        Pair<Status, CacheHandle<Pair<RandomAccessFile, ITableReader>>> pair = spyTableCache.findTable(fileNumber, builder.fileSize());
        assertTrue(pair.getKey().isNotOk());
        assertEquals("open force error", pair.getKey().getMessage());
    }
}