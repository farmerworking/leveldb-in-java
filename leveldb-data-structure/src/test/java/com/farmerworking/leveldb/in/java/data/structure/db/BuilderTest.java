package com.farmerworking.leveldb.in.java.data.structure.db;

import com.farmerworking.leveldb.in.java.api.Iterator;
import com.farmerworking.leveldb.in.java.api.Options;
import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.data.structure.block.EmptyIterator;
import com.farmerworking.leveldb.in.java.data.structure.cache.TableCache;
import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKey;
import com.farmerworking.leveldb.in.java.data.structure.version.FileMetaData;
import com.farmerworking.leveldb.in.java.file.Env;
import com.farmerworking.leveldb.in.java.file.FileName;
import com.google.common.collect.Lists;
import javafx.util.Pair;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class BuilderTest {
    Builder builder;
    Options options;
    String dbname;
    Iterator<InternalKey, String> validIterator;
    TableCache tableCache;

    @Before
    public void setUp() throws Exception {
        builder = new Builder();
        options = new Options();
        dbname = options.getEnv().getTestDirectory().getValue();
        tableCache = new TableCache(dbname, options, 1024);

        validIterator = new Iterator<InternalKey, String>() {
            List<String> values = Lists.newArrayList("value1", "value2");
            List<InternalKey> keys = Lists.newArrayList(new InternalKey("a", 1L), new InternalKey("b", 2L));

            Integer index = null;

            @Override
            public boolean valid() {
                return index >= 0 && index <= 1;
            }

            @Override
            public void seekToFirst() {
                index = 0;
            }

            @Override
            public void seekToLast() {
                index = 1;
            }

            @Override
            public void seek(String target) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void next() {
                if (index <= 1) {
                    index++;
                }
            }

            @Override
            public void prev() {
                if (index >= 0) {
                    index--;
                }
            }

            @Override
            public InternalKey key() {
                return keys.get(index);
            }

            @Override
            public String value() {
                return values.get(index);
            }

            @Override
            public Status status() {
                return Status.OK();
            }

            @Override
            public void close() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void registerCleanup(Runnable runnable) {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Test
    public void testBuildEmpty() {
        FileMetaData metaData = new FileMetaData();
        metaData.setFileNumber(1L);
        Status status = builder.buildTable(dbname, options.getEnv(), options, null, new EmptyIterator(), metaData);

        assertTrue(status.isOk());
        assertEquals(0, metaData.getFileSize());
    }

    @Test
    public void testBuildTable() {
        FileMetaData metaData = new FileMetaData();
        metaData.setFileNumber(1L);

        String filename = FileName.tableFileName(dbname, metaData.getFileNumber());
        options.getEnv().delete(filename);
        assertFalse(options.getEnv().isFileExists(filename).getValue());

        Status status = builder.buildTable(dbname, options.getEnv(), options, tableCache, validIterator, metaData);

        assertTrue(status.isOk());
        assertTrue(options.getEnv().isFileExists(filename).getValue());
        assertTrue(metaData.getFileSize() > 0);
        assertEquals(new InternalKey("a", 1L), metaData.getSmallest());
        assertEquals(new InternalKey("b", 2L), metaData.getLargest());
    }

    @Test
    public void testExceptionCase() {
        FileMetaData metaData = new FileMetaData();
        metaData.setFileNumber(1L);
        String filename = FileName.tableFileName(dbname, metaData.getFileNumber());

        Status status = builder.buildTable(dbname, options.getEnv(), options, null, new EmptyIterator(Status.IOError("force iterator status error")), metaData);
        assertTrue(status.isNotOk());
        assertEquals("force iterator status error", status.getMessage());
        assertFalse(options.getEnv().isFileExists(filename).getValue());

        Env spyEnv = spy(options.getEnv());
        doReturn(new Pair<>(Status.Corruption("force writable file error"), null)).when(spyEnv).newWritableFile(anyString());
        status = builder.buildTable(dbname, spyEnv, options, null, validIterator, metaData);
        assertTrue(status.isNotOk());
        assertEquals("force writable file error", status.getMessage());
        assertFalse(options.getEnv().isFileExists(filename).getValue());

        Builder spyBuilder = spy(builder);
        doReturn(Status.Corruption("force finish error")).when(spyBuilder).finish(any());
        status = spyBuilder.buildTable(dbname, options.getEnv(), options, null, validIterator, metaData);
        assertTrue(status.isNotOk());
        assertEquals("force finish error", status.getMessage());
        assertFalse(options.getEnv().isFileExists(filename).getValue());
        doCallRealMethod().when(spyBuilder).finish(any());

        doReturn(Status.Corruption("force sync error")).when(spyBuilder).sync(any());
        status = spyBuilder.buildTable(dbname, options.getEnv(), options, null, validIterator, metaData);
        assertTrue(status.isNotOk());
        assertEquals("force sync error", status.getMessage());
        assertFalse(options.getEnv().isFileExists(filename).getValue());
        doCallRealMethod().when(spyBuilder).sync(any());

        doReturn(Status.Corruption("force close error")).when(spyBuilder).close(any());
        status = spyBuilder.buildTable(dbname, options.getEnv(), options, null, validIterator, metaData);
        assertTrue(status.isNotOk());
        assertEquals("force close error", status.getMessage());
        assertFalse(options.getEnv().isFileExists(filename).getValue());
        doCallRealMethod().when(spyBuilder).close(any());

        doReturn(Status.Corruption("force status error")).when(spyBuilder).status(any());
        status = spyBuilder.buildTable(dbname, options.getEnv(), options, tableCache, validIterator, metaData);
        assertTrue(status.isNotOk());
        assertEquals("force status error", status.getMessage());
        assertFalse(options.getEnv().isFileExists(filename).getValue());
        doCallRealMethod().when(spyBuilder).status(any());
    }
}