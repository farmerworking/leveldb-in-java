package com.farmerworking.leveldb.in.java.data.structure.db;

import com.farmerworking.leveldb.in.java.api.Options;
import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.data.structure.log.LogReader;
import com.farmerworking.leveldb.in.java.data.structure.version.VersionEdit;
import com.farmerworking.leveldb.in.java.data.structure.writebatch.WriteBatch;
import com.farmerworking.leveldb.in.java.file.FileName;
import javafx.util.Pair;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class Log2MemtableReaderTest {
    Options options;
    String dbname;
    DBImpl db;
    Log2MemtableReader reader;
    VersionEdit edit;

    public void setUp() {
        setUp(false);
    }

    private void setUp(boolean paranoidCheck) {
        options = new Options();
        options.setParanoidChecks(paranoidCheck);
        dbname = options.getEnv().getTestDirectory().getValue();
        db = new DBImpl(options, dbname);
        edit = new VersionEdit();

        reader = new Log2MemtableReader(db, edit, "", null);
    }

    @Test(expected = AssertionError.class)
    public void testInvokeWithoutLock() {
        setUp();
        reader.invoke();
    }

    @Test
    public void testReadLogRecordError1() {
        setUp(false);
        Log2MemtableReader spyReader = prepareReadLogRecordError();
        spyReader.invoke();

        assertTrue(spyReader.getStatus().isOk());
        verifyReaderDefaultState(spyReader);
    }

    @Test
    public void testReadLogRecordError2() {
        setUp(true);
        Log2MemtableReader spyReader = prepareReadLogRecordError();
        spyReader.invoke();

        assertTrue(spyReader.getStatus().isCorruption());
        assertEquals("force log read error", spyReader.getStatus().getMessage());
        verifyReaderDefaultState(spyReader);
    }

    private Log2MemtableReader prepareReadLogRecordError() {
        options.getEnv().newWritableFile(FileName.logFileName(dbname, 5));
        db.getMutex().lock();

        Log2MemtableReader spyReader = spy(reader);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                spyReader.getLogReporter().corruption(100L, Status.Corruption("force log read error"));
                return new Pair<>(false, null);
            }
        }).when(spyReader).readLogRecord(any(LogReader.class));
        return spyReader;
    }

    @Test
    public void testMalformedLogRecord1() {
        setUp(false);
        Log2MemtableReader spyReader = prepareMalformedLogRecord();
        spyReader.invoke();
        assertTrue(spyReader.getStatus().isOk());
        verifyReaderDefaultState(spyReader);
    }

    @Test
    public void testMalformedLogRecord2() {
        setUp(true);
        Log2MemtableReader spyReader = prepareMalformedLogRecord();
        spyReader.invoke();

        assertTrue(spyReader.getStatus().isCorruption());
        assertEquals("log record too small", spyReader.getStatus().getMessage());
        verifyReaderDefaultState(spyReader);
    }

    private Log2MemtableReader prepareMalformedLogRecord() {
        db.getMutex().lock();
        Log2MemtableReader spyReader = spy(reader);
        doReturn(new Pair<>(true, "")).doReturn(new Pair<>(false, "")).when(spyReader).readLogRecord(any(LogReader.class));
        return spyReader;
    }

    @Test
    public void testBatchIterateError1() {
        setUp(false);
        Log2MemtableReader spyReader = prepareBatchIteratorError();
        spyReader.invoke();

        assertTrue(spyReader.getStatus().isOk());
    }

    @Test
    public void testBatchIterateError2() {
        setUp(true);
        Log2MemtableReader spyReader = prepareBatchIteratorError();
        spyReader.invoke();

        assertTrue(spyReader.getStatus().isCorruption());
        assertEquals("force write batch iterate error", spyReader.getStatus().getMessage());
    }

    private Log2MemtableReader prepareBatchIteratorError() {
        db.getMutex().lock();
        Log2MemtableReader spyReader = spy(reader);

        WriteBatch batch = new WriteBatch();
        batch.put("foo", "bar");
        batch.setSequence(100);

        doReturn(new Pair<>(true, String.valueOf(batch.encode()))).doReturn(new Pair<>(false, "")).when(spyReader).readLogRecord(any());
        doReturn(Status.Corruption("force write batch iterate error")).when(spyReader).iterateBatch(any(), any());
        return spyReader;
    }

    @Test
    public void testWriteLevel0TableError() {
        setUp();
        db.getMutex().lock();
        Log2MemtableReader spyReader = spy(reader);

        WriteBatch batch = new WriteBatch();
        batch.put("foo", "bar");
        batch.setSequence(100);

        doReturn(new Pair<>(true, String.valueOf(batch.encode()))).doReturn(new Pair<>(false, null)).when(spyReader).readLogRecord(any());
        doReturn(0).when(spyReader).getWriteBufferSize();
        doReturn(Status.Corruption("force write level0 table error")).when(spyReader).writeLevel0Table();

        spyReader.invoke();

        assertEquals("force write level0 table error", spyReader.getStatus().getMessage());
    }

    @Test
    public void testInvoke1() {
        setUp();
        db.getMutex().lock();

        WriteBatch batch = new WriteBatch();
        batch.put("foo", "bar");
        batch.setSequence(100);

        Log2MemtableReader spyReader = spy(reader);
        doReturn(new Pair<>(true, String.valueOf(batch.encode()))).doReturn(new Pair<>(false, null)).when(spyReader).readLogRecord(any());

        spyReader.invoke();
        assertTrue(spyReader.getStatus().isOk());
        assertEquals(100, spyReader.getMaxSequence().longValue());
        assertFalse(spyReader.isSaveManifest());
        assertEquals(0, spyReader.getCompactions());
        assertNotNull(spyReader.getMemtable());
    }

    @Test
    public void testInvoke2() {
        setUp();
        db.getMutex().lock();

        WriteBatch batch1 = new WriteBatch();
        batch1.put("foo", "bar");
        batch1.setSequence(100);

        WriteBatch batch2 = new WriteBatch();
        batch2.put("foo1", "bar1");
        batch2.setSequence(200);

        Log2MemtableReader spyReader = spy(reader);
        doReturn(new Pair<>(true, String.valueOf(batch1.encode())))
                .doReturn(new Pair<>(true, String.valueOf(batch2.encode())))
                .doReturn(new Pair<>(false, null))
                .when(spyReader).readLogRecord(any());
        doCallRealMethod()
                .doReturn(0)
                .doCallRealMethod()
                .when(spyReader).getWriteBufferSize();

        spyReader.invoke();
        assertTrue(spyReader.getStatus().isOk());
        assertEquals(200, spyReader.getMaxSequence().longValue());
        assertTrue(spyReader.isSaveManifest());
        assertEquals(1, spyReader.getCompactions());
        assertNull(spyReader.getMemtable());
    }

    void verifyReaderDefaultState(Log2MemtableReader spyReader) {
        assertNull(spyReader.getMaxSequence());
        assertFalse(spyReader.isSaveManifest());
        assertEquals(0, spyReader.getCompactions());
        assertNull(spyReader.getMemtable());
    }
}