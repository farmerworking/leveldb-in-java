package com.farmerworking.leveldb.in.java.data.structure.db;

import com.farmerworking.leveldb.in.java.api.Iterator;
import com.farmerworking.leveldb.in.java.api.Options;
import com.farmerworking.leveldb.in.java.api.ReadOptions;
import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.common.TestUtils;
import com.farmerworking.leveldb.in.java.data.structure.log.ILogReader;
import com.farmerworking.leveldb.in.java.data.structure.log.ILogWriter;
import com.farmerworking.leveldb.in.java.data.structure.log.LogReader;
import com.farmerworking.leveldb.in.java.data.structure.log.LogWriter;
import com.farmerworking.leveldb.in.java.data.structure.memory.*;
import com.farmerworking.leveldb.in.java.data.structure.version.Config;
import com.farmerworking.leveldb.in.java.data.structure.version.FileMetaData;
import com.farmerworking.leveldb.in.java.data.structure.version.Version;
import com.farmerworking.leveldb.in.java.data.structure.version.VersionEdit;
import com.farmerworking.leveldb.in.java.data.structure.writebatch.WriteBatch;
import com.farmerworking.leveldb.in.java.file.Env;
import com.farmerworking.leveldb.in.java.file.FileName;
import com.farmerworking.leveldb.in.java.file.SequentialFile;
import com.farmerworking.leveldb.in.java.file.WritableFile;
import javafx.util.Pair;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;


import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class DBImplTest {
    @Test(expected = AssertionError.class)
    public void testClipToRangeAssertionError() {
        DBImpl.clipToRange(null, "", 1, 0);
    }

    @Test
    public void testClipToRangeNoFieldException() {
        Options options = new Options();
        try {
            DBImpl.clipToRange(options, "whateverFiled", 0, 1);
            assert false;
        } catch (Exception e) {
            assertTrue(e instanceof RuntimeException);
            RuntimeException runtimeException = (RuntimeException) e;
            assertTrue(runtimeException.getCause() instanceof NoSuchFieldException);
            assertEquals("sanitize option field error", e.getMessage());
        }
    }

    @Test
    public void testClipToRange() {
        Options options = new Options();
        DBImpl.clipToRange(options, "maxOpenFiles", 2000, 3000);
        assertEquals(2000, options.getMaxOpenFiles());

        options = new Options();
        DBImpl.clipToRange(options, "maxOpenFiles", 100, 500);
        assertEquals(500, options.getMaxOpenFiles());
    }

    @Test
    public void testSanitizeOptions() {
        Options src = new Options();
        String dbname = src.getEnv().getTestDirectory().getValue();

        // init invalid value
        src.setMaxOpenFiles(0);
        src.setWriteBufferSize(0);
        src.setMaxFileSize(0);
        src.setBlockSize(0);

        Options options = DBImpl.sanitizeOptions(dbname,
                new InternalKeyComparator(src.getComparator()),
                new InternalFilterPolicy(null),
                src);

        assertNotSame(options, src);

        assertNull(src.getFilterPolicy());
        assertNull(options.getFilterPolicy());

        assertTrue(options.getComparator() instanceof InternalKeyComparator);
        assertEquals(((InternalKeyComparator) options.getComparator()).getUserComparator(), src.getComparator());

        assertNotNull(options.getInfoLog());
        assertNull(src.getInfoLog());

        assertNotNull(options.getBlockCache());
        assertNull(src.getBlockCache());

        assertNotEquals(0, options.getMaxOpenFiles());
        assertNotEquals(0, options.getWriteBufferSize());
        assertNotEquals(0, options.getMaxFileSize());
        assertNotEquals(0, options.getBlockSize());
    }

    @Test
    public void testDbConstructor() {
        Options options = new Options();
        String dbname = options.getEnv().getTestDirectory().getValue();

        DBImpl db = new DBImpl(options, dbname);

        assertNotNull(db.getEnv());
        assertNotNull(db.getInternalKeyComparator());
        assertNotNull(db.getInternalFilterPolicy());
        assertNotNull(db.getOptions());
        assertNotSame(db.getOptions(), options);
        assertTrue(StringUtils.isNotEmpty(db.getDbname()));
        assertNull(db.getDbLock());
        assertNotNull(db.getMutex());
        assertNull(db.getShuttingDown());
        assertNotNull(db.getBgCondition());
        assertNull(db.getMemtable());
        assertNull(db.getImmutableMemtable());
        assertNull(db.getLogFile());
        assertEquals(0, db.getLogFileNumber());
        assertNull(db.getLog());
        assertEquals(0, db.getSeed());
        assertFalse(db.isBgCompactionScheduled());
        assertNull(db.getManualCompaction());
        assertNotNull(db.getHasImmutableMemtable());
        assertFalse(db.getHasImmutableMemtable().get());
        assertNotNull(db.getTableCache());
        assertNotNull(db.getVersions());
        assertTrue(db.getPendingOutputs().isEmpty());
        assertNotNull(db.getBuilder());
        assertEquals(Config.kNumLevels, db.getStats().length);
        for (int i = 0; i < Config.kNumLevels; i++) {
            assertNotNull(db.getStats()[i]);
        }
    }

    @Test
    public void testNewDB() {
        Options options = new Options();
        String dbname = options.getEnv().getTestDirectory().getValue();

        DBImpl db = new DBImpl(options, dbname);
        Status status = db.newDB();

        assertTrue(status.isOk());
        String current = FileName.currentFileName(dbname);
        Pair<Status, String> pair = Env.readFileToString(options.getEnv(), current);
        assertTrue(pair.getKey().isOk());
        assertEquals('\n', pair.getValue().charAt(pair.getValue().length() - 1));
        assertEquals(DBImpl.NEW_DB_MANIFEST_NUMBER, Integer.parseInt(String.valueOf(pair.getValue().charAt(pair.getValue().length() - 2)))); // new manifest number

        String manifest = dbname + "/" + pair.getValue().substring(0, pair.getValue().length() - 1);
        Pair<Status, SequentialFile> filePair = options.getEnv().newSequentialFile(manifest);
        assertTrue(filePair.getKey().isOk());
        ILogReader logReader = new LogReader(filePair.getValue(), null, true, 0);
        Pair<Boolean, String> recordPair = logReader.readRecord();
        assertTrue(recordPair.getKey());

        VersionEdit edit = new VersionEdit();
        edit.decodeFrom(recordPair.getValue().toCharArray());

        assertEquals(options.getComparator().name(), edit.getComparatorName());
        assertEquals(DBImpl.NEW_DB_NEXT_FILE_NUMBER, edit.getNextFileNumber());
        assertEquals(DBImpl.NEW_DB_LAST_SEQUENCE, edit.getLastSequence());
        assertEquals(DBImpl.NEW_DB_LOG_NUMBER, edit.getLogNumber());

        assertFalse(edit.isHasPrevLogNumber());
        assertTrue(edit.getCompactPointers().isEmpty());
        assertTrue(edit.getDeletedFiles().isEmpty());
        assertTrue(edit.getNewFiles().isEmpty());
    }

    @Test
    public void testNewDBErrorCase() {
        Options options = new Options();
        String dbname = options.getEnv().getTestDirectory().getValue();
        Env spyEnv = spy(options.getEnv());
        options.setEnv(spyEnv);

        DBImpl db = new DBImpl(options, dbname);

        doReturn(new Pair<>(Status.IOError("force new writable file error"), null)).when(spyEnv).newWritableFile(anyString());
        Status status = db.newDB();
        verifyNewDBError(options, dbname, status, "force new writable file error");

        doCallRealMethod().when(spyEnv).newWritableFile(anyString());
        DBImpl spyDb = spy(db);
        ILogWriter mockLogWriter = mock(LogWriter.class);
        doReturn(Status.IOError("force add record error")).when(mockLogWriter).addRecord(anyString());
        doReturn(mockLogWriter).when(spyDb).newDBGetLogWriter(any());

        status = spyDb.newDB();
        verifyNewDBError(options, dbname, status, "force add record error");

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Pair<Status, WritableFile> result = (Pair<Status, WritableFile>) invocation.callRealMethod();
                WritableFile spyFile = spy(result.getValue());
                doReturn(Status.IOError("force file close error")).when(spyFile).close();

                return new Pair<>(result.getKey(), spyFile);
            }
        }).when(spyEnv).newWritableFile(anyString());
        status = db.newDB();
        verifyNewDBError(options, dbname, status, "force file close error");
    }

    void verifyNewDBError(Options options, String dbname, Status status, String s) {
        assertTrue(status.isNotOk());
        assertEquals(s, status.getMessage());
        assertFalse(options.getEnv().isFileExists(FileName.descriptorFileName(dbname, DBImpl.NEW_DB_MANIFEST_NUMBER)).getValue());
        assertFalse(options.getEnv().isFileExists(FileName.currentFileName(dbname)).getValue());
    }

    @Test
    public void testWriteLevel0Table() {
        innerTestWriteLevel0Table(0, null);


        Version mockVersion = mock(Version.class);
        doReturn(2).when(mockVersion).pickLevelForMemTableOutput(anyString(), anyString());
        innerTestWriteLevel0Table(2, mockVersion);
    }

    private void innerTestWriteLevel0Table(int level, Version version) {
        Options options = new Options();
        String dbname = options.getEnv().getTestDirectory().getValue();

        class DbImplExtend extends DBImpl {
            public DbImplExtend(Options rawOptions, String dbname) {
                super(rawOptions, dbname);
            }

            // not empty before unlock
            @Override
            void unlock() {
                assert !this.pendingOutputs.isEmpty();
                super.unlock();
            }

            // not empty after lock again
            @Override
            void lock() {
                super.lock();
                assert !this.pendingOutputs.isEmpty();
            }
        }

        DBImpl db = new DbImplExtend(options, dbname);

        VersionEdit edit = new VersionEdit();
        IMemtable memtable = new Memtable(db.getInternalKeyComparator());
        long sequence = 1L;
        for (int i = 0; i < 10; i++) {
            memtable.add(sequence ++, ValueType.kTypeValue, TestUtils.randomKey(5), TestUtils.randomString(6));
        }

        // before
        CompactionStats beforeStats = new CompactionStats(db.getStats()[level]);
        assertTrue(db.getPendingOutputs().isEmpty());

        db.getMutex().lock();
        Status status = db.writeLevel0Table(memtable, edit, version);
        assertTrue(status.isOk());

        // pending outputs
        assertTrue(db.getPendingOutputs().isEmpty());

        // lock
        assertTrue(db.getMutex().isHeldByCurrentThread());

        // edit
        assertEquals(1, edit.getNewFiles().size());
        assertEquals(level, edit.getNewFiles().get(0).getKey().intValue());

        FileMetaData metaData = edit.getNewFiles().get(0).getValue();

        // stats
        assertNotEquals(beforeStats, db.getStats()[level]);
        assertEquals(beforeStats.getBytesWritten() + metaData.getFileSize(), db.getStats()[level].getBytesWritten());
        assertTrue(beforeStats.getMicros() < db.getStats()[level].getMicros());

        // FileMetaData
        assertTrue(metaData.getFileNumber() > 0);
        assertTrue(metaData.getFileSize() > 0);
        assertNotNull(metaData.getSmallest());
        assertNotNull(metaData.getLargest());
        assertTrue(db.getInternalKeyComparator().compare(metaData.getSmallest(), metaData.getLargest()) < 0);

        // file
        String filename = FileName.tableFileName(dbname, metaData.getFileNumber());
        assertTrue(options.getEnv().isFileExists(filename).getValue());

        // file content
        Iterator<String, String> iter1 = db.getTableCache().iterator(
                new ReadOptions(),
                metaData.getFileNumber(),
                metaData.getFileSize()).getKey();

        Iterator<InternalKey, String> iter2 = memtable.iterator();
        iter1.seekToFirst();
        iter2.seekToFirst();
        assertTrue(iter1.valid() && iter2.valid());

        while(iter1.valid() && iter2.valid()) {
            assertEquals(iter1.key(), iter2.key().encode());
            assertEquals(iter1.value(), iter2.value());
            iter1.next();
            iter2.next();
        }

        assertTrue(!iter1.valid() && !iter2.valid());
    }

    @Test(expected = AssertionError.class)
    public void testWriteLevel0TableWithoutLock() {
        Options options = new Options();
        String dbname = options.getEnv().getTestDirectory().getValue();

        DBImpl db = new DBImpl(options, dbname);
        Status status = db.writeLevel0Table(null, null, null);
    }

    @Test
    public void testWriteLevel0TableExceptionCase() {
        Options options = new Options();
        String dbname = options.getEnv().getTestDirectory().getValue();

        DBImpl db = new DBImpl(options, dbname);
        db.setBuilder(spy(db.getBuilder()));
        db.getMutex().lock();

        doReturn(Status.Corruption("force build table error")).when(db.getBuilder()).buildTable(anyString(), any(), any(), any(), any(), any());
        VersionEdit edit = new VersionEdit();
        long before = db.getStats()[0].getBytesWritten();
        Status status = db.writeLevel0Table(new Memtable(db.getInternalKeyComparator()), edit, null);

        assertTrue(status.isNotOk());
        assertEquals("force build table error", status.getMessage());
        assertTrue(edit.getNewFiles().isEmpty());
        assertEquals(before, db.getStats()[0].getBytesWritten());

        doReturn(Status.OK()).when(db.getBuilder()).buildTable(anyString(), any(), any(), any(), any(), any());
        status = db.writeLevel0Table(new Memtable(db.getInternalKeyComparator()), edit, null);
        assertTrue(status.isOk());
        assertTrue(edit.getNewFiles().isEmpty());
        assertEquals(before, db.getStats()[0].getBytesWritten());
    }

    @Test
    public void testMaybeIgnoreError() {
        Options options = new Options();
        String dbname = options.getEnv().getTestDirectory().getValue();

        DBImpl db = new DBImpl(options, dbname);
        Status status = db.maybeIgnoreError(Status.OK());
        assertTrue(status.isOk());

        status = db.maybeIgnoreError(Status.Corruption(""));
        assertTrue(status.isOk());

        db.getOptions().setParanoidChecks(true);
        status = db.maybeIgnoreError(Status.Corruption(""));
        assertTrue(status.IsCorruption());
    }

    @Test(expected = AssertionError.class)
    public void testRecoverLogFileWithoutLock() {
        Options options = new Options();
        String dbname = options.getEnv().getTestDirectory().getValue();

        DBImpl db = new DBImpl(options, dbname);
        db.recoverLogFile(0L, true, null);
    }

    @Test
    public void testRecoverLogFileOpenLogFileError() {
        Options options = new Options();
        String dbname = options.getEnv().getTestDirectory().getValue();
        Env spyEnv = spy(options.getEnv());
        options.setEnv(spyEnv);

        DBImpl db = new DBImpl(options, dbname);
        db.getMutex().lock();

        doReturn(new Pair<>(Status.Corruption("force sequence file open error"), null)).when(spyEnv).newSequentialFile(anyString());
        DBImpl.RecoverLogFileResult result = db.recoverLogFile(5L, true, null);
        assertTrue(result.getStatus().isOk());

        db.getOptions().setParanoidChecks(true);
        result = db.recoverLogFile(5L, true, null);
        assertTrue(result.getStatus().IsCorruption());
        assertEquals("force sequence file open error", result.getStatus().getMessage());
    }

    @Test
    public void testRecoverLogFileReadLogRecordError() {
        Options options = new Options();
        String dbname = options.getEnv().getTestDirectory().getValue();

        options.getEnv().newWritableFile(FileName.logFileName(dbname, 5));

        DBImpl db = new DBImpl(options, dbname);
        db.getMutex().lock();

        DBImpl spyDB = spy(db);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                LogReader logReader = invocation.getArgument(0);
                logReader.getReporter().corruption(100L, Status.Corruption("force log read error"));
                return new Pair<>(false, null);
            }
        }).when(spyDB).readLogRecord(any(LogReader.class));
        DBImpl.RecoverLogFileResult result = spyDB.recoverLogFile(5L, true, null);
        assertTrue(result.getStatus().isOk());
        assertNull(result.getMaxSequence());
        assertFalse(result.isSaveManifest());

        db.getOptions().setParanoidChecks(true);
        result = spyDB.recoverLogFile(5L, true, null);
        assertTrue(result.getStatus().IsCorruption());
        assertEquals("force log read error", result.getStatus().getMessage());
        assertNull(result.getMaxSequence());
        assertFalse(result.isSaveManifest());
    }

    @Test
    public void testRecoverLogFileMalformedLogRecord() {
        Options options = new Options();
        String dbname = options.getEnv().getTestDirectory().getValue();

        options.getEnv().newWritableFile(FileName.logFileName(dbname, 5));

        DBImpl db = new DBImpl(options, dbname);
        db.getMutex().lock();

        DBImpl spyDB = spy(db);
        doReturn(new Pair<>(true, "")).doCallRealMethod().when(spyDB).readLogRecord(any(LogReader.class));
        DBImpl.RecoverLogFileResult result = spyDB.recoverLogFile(5L, true, null);
        assertTrue(result.getStatus().isOk());
        assertNull(result.getMaxSequence());
        assertFalse(result.isSaveManifest());

        db.getOptions().setParanoidChecks(true);
        doReturn(new Pair<>(true, "")).doCallRealMethod().when(spyDB).readLogRecord(any(LogReader.class));
        result = spyDB.recoverLogFile(5L, true, null);
        assertTrue(result.getStatus().IsCorruption());
        assertEquals("log record too small", result.getStatus().getMessage());
        assertNull(result.getMaxSequence());
        assertFalse(result.isSaveManifest());
    }

    @Test
    public void testRecoverLogFileWriteBatchIterateError() {
        Options options = new Options();
        String dbname = options.getEnv().getTestDirectory().getValue();

        options.getEnv().newWritableFile(FileName.logFileName(dbname, 5));

        DBImpl db = new DBImpl(options, dbname);
        db.getMutex().lock();

        WriteBatch batch = new WriteBatch();
        batch.put("foo", "bar");
        batch.setSequence(100);

        DBImpl spyDB = spy(db);
        doReturn(new Pair<>(true, String.valueOf(batch.encode()))).doCallRealMethod().when(spyDB).readLogRecord(any());
        doReturn(Status.Corruption("force write batch iterate error")).when(spyDB).iterateBatch(any(), any());
        DBImpl.RecoverLogFileResult result = spyDB.recoverLogFile(5L, true, null);
        assertTrue(result.getStatus().isOk());
        assertEquals(100L, result.getMaxSequence().longValue());
        assertTrue(result.isSaveManifest());

        db.getOptions().setParanoidChecks(true);
        doReturn(new Pair<>(true, String.valueOf(batch.encode()))).doCallRealMethod().when(spyDB).readLogRecord(any());
        result = spyDB.recoverLogFile(5L, true, null);
        assertTrue(result.getStatus().IsCorruption());
        assertEquals("force write batch iterate error", result.getStatus().getMessage());
        assertNull(result.getMaxSequence());
        assertFalse(result.isSaveManifest());
    }

    @Test
    public void testRecoverLogFileWriteLevel0TableError() {
        Options options = new Options();
        String dbname = options.getEnv().getTestDirectory().getValue();

        options.getEnv().newWritableFile(FileName.logFileName(dbname, 5));

        DBImpl db = new DBImpl(options, dbname);
        db.getMutex().lock();

        WriteBatch batch = new WriteBatch();
        batch.put("foo", "bar");
        batch.setSequence(100);

        DBImpl spyDB = spy(db);
        doReturn(Status.Corruption("force write level0 table error")).when(spyDB).writeLevel0Table(any(), any(), any());
        doReturn(new Pair<>(true, String.valueOf(batch.encode()))).doCallRealMethod().when(spyDB).readLogRecord(any());
        doReturn(0).when(spyDB).getWriteBufferSize();

        DBImpl.RecoverLogFileResult result = spyDB.recoverLogFile(5L, true, null);
        assertEquals("force write level0 table error", result.getStatus().getMessage());
        assertEquals(100L, result.getMaxSequence().longValue());
        assertTrue(result.isSaveManifest());

        doCallRealMethod().when(spyDB).getWriteBufferSize();
        doReturn(new Pair<>(true, String.valueOf(batch.encode()))).doCallRealMethod().when(spyDB).readLogRecord(any());
        result = spyDB.recoverLogFile(5L, true, null);
        assertEquals("force write level0 table error", result.getStatus().getMessage());
        assertEquals(100L, result.getMaxSequence().longValue());
        assertTrue(result.isSaveManifest());
    }

    @Test(expected = AssertionError.class)
    public void testRecoverLogFileReuseLogAssertion1() {
        Options options = new Options();
        String dbname = options.getEnv().getTestDirectory().getValue();
        options.setReuseLogs(true);

        options.getEnv().newWritableFile(FileName.logFileName(dbname, 5));

        DBImpl db = new DBImpl(options, dbname);
        db.getMutex().lock();

        db.setLogFile(new WritableFile() {
            @Override
            public Status append(String data) {
                return null;
            }

            @Override
            public Status flush() {
                return null;
            }

            @Override
            public Status close() {
                return null;
            }

            @Override
            public Status sync() {
                return null;
            }
        });
        DBImpl.RecoverLogFileResult result = db.recoverLogFile(5L, true, null);
    }

    @Test(expected = AssertionError.class)
    public void testRecoverLogFileReuseLogAssertion2() {
        Options options = new Options();
        String dbname = options.getEnv().getTestDirectory().getValue();
        options.setReuseLogs(true);

        options.getEnv().newWritableFile(FileName.logFileName(dbname, 5));

        DBImpl db = new DBImpl(options, dbname);
        db.getMutex().lock();

        db.setLog(new ILogWriter() {
            @Override
            public Status addRecord(String data) {
                return null;
            }
        });
        DBImpl.RecoverLogFileResult result = db.recoverLogFile(5L, true, null);
    }

    @Test(expected = AssertionError.class)
    public void testRecoverLogFileReuseLogAssertion3() {
        Options options = new Options();
        String dbname = options.getEnv().getTestDirectory().getValue();
        options.setReuseLogs(true);

        options.getEnv().newWritableFile(FileName.logFileName(dbname, 5));

        DBImpl db = new DBImpl(options, dbname);
        db.getMutex().lock();

        db.setMemtable(new IMemtable() {
            @Override
            public Iterator<InternalKey, String> iterator() {
                return null;
            }

            @Override
            public void add(long sequence, ValueType type, String key, String value) {

            }

            @Override
            public Pair<Boolean, Pair<Status, String>> get(String userKey, long sequence) {
                return null;
            }

            @Override
            public int approximateMemoryUsage() {
                return 0;
            }
        });
        DBImpl.RecoverLogFileResult result = db.recoverLogFile(5L, true, null);
    }

    @Test
    public void testRecoverLogFile() {
        Options options = new Options();
        String dbname = options.getEnv().getTestDirectory().getValue();

        options.getEnv().newWritableFile(FileName.logFileName(dbname, 5));

        WriteBatch batch = new WriteBatch();
        batch.put("foo", "bar");
        batch.setSequence(100);

        DBImpl db = new DBImpl(options, dbname);
        db.getMutex().lock();

        DBImpl spyDB = spy(db);
        doReturn(new Pair<>(true, String.valueOf(batch.encode()))).doCallRealMethod().when(spyDB).readLogRecord(any());

        VersionEdit edit = new VersionEdit();
        DBImpl.RecoverLogFileResult result = spyDB.recoverLogFile(5L, true, edit);
        assertTrue(result.getStatus().isOk());
        assertTrue(result.isSaveManifest());
        assertEquals(100, result.getMaxSequence().longValue());
        assertNull(spyDB.getMemtable());
        assertNull(spyDB.getLog());
        assertNull(spyDB.getLogFile());
        assertEquals(1, edit.getNewFiles().size());

        spyDB.getOptions().setReuseLogs(true);
        // do compactions and reuse log
        doReturn(new Pair<>(true, String.valueOf(batch.encode()))).doCallRealMethod().when(spyDB).readLogRecord(any());
        doReturn(0).when(spyDB).getWriteBufferSize();
        edit = new VersionEdit();
        result = spyDB.recoverLogFile(5L, true, edit);
        assertTrue(result.getStatus().isOk());
        assertTrue(result.isSaveManifest());
        assertEquals(100, result.getMaxSequence().longValue());
        assertNull(spyDB.getMemtable());
        assertNull(spyDB.getLog());
        assertNull(spyDB.getLogFile());
        assertEquals(1, edit.getNewFiles().size());

        // not last log and reuse log
        doReturn(new Pair<>(true, String.valueOf(batch.encode()))).doCallRealMethod().when(spyDB).readLogRecord(any());
        edit = new VersionEdit();
        result = spyDB.recoverLogFile(5L, false, edit);
        assertTrue(result.getStatus().isOk());
        assertTrue(result.isSaveManifest());
        assertEquals(100, result.getMaxSequence().longValue());
        assertNull(spyDB.getMemtable());
        assertNull(spyDB.getLog());
        assertNull(spyDB.getLogFile());
        assertEquals(1, edit.getNewFiles().size());

        // reuse log get file size error
        doReturn(new Pair<>(true, String.valueOf(batch.encode()))).doCallRealMethod().when(spyDB).readLogRecord(any());
        doReturn(new Pair<>(Status.Corruption(""), null)).when(spyDB).getFileSize(anyString());
        edit = new VersionEdit();
        result = spyDB.recoverLogFile(5L, true, edit);
        assertTrue(result.getStatus().isOk());
        assertTrue(result.isSaveManifest());
        assertEquals(100, result.getMaxSequence().longValue());
        assertNull(spyDB.getMemtable());
        assertNull(spyDB.getLog());
        assertNull(spyDB.getLogFile());
        assertEquals(1, edit.getNewFiles().size());
        doCallRealMethod().when(spyDB).getFileSize(anyString());

        // reuse log get appendable file error
        doReturn(new Pair<>(true, String.valueOf(batch.encode()))).doCallRealMethod().when(spyDB).readLogRecord(any());
        doReturn(new Pair<>(Status.Corruption(""), null)).when(spyDB).getAppendableFile(anyString());
        edit = new VersionEdit();
        result = spyDB.recoverLogFile(5L, true, edit);
        assertTrue(result.getStatus().isOk());
        assertTrue(result.isSaveManifest());
        assertEquals(100, result.getMaxSequence().longValue());
        assertNull(spyDB.getMemtable());
        assertNull(spyDB.getLog());
        assertNull(spyDB.getLogFile());
        assertEquals(1, edit.getNewFiles().size());
        doCallRealMethod().when(spyDB).getAppendableFile(anyString());

        // reuse log
        doReturn(new Pair<>(true, String.valueOf(batch.encode()))).doCallRealMethod().when(spyDB).readLogRecord(any());
        doCallRealMethod().when(spyDB).getWriteBufferSize();
        edit = new VersionEdit();
        result = spyDB.recoverLogFile(5L, true, edit);
        assertTrue(result.getStatus().isOk());
        assertFalse(result.isSaveManifest());
        assertEquals(100, result.getMaxSequence().longValue());
        assertNotNull(spyDB.getMemtable());
        assertNotNull(spyDB.getLog());
        assertNotNull(spyDB.getLogFile());
        assertEquals(0, edit.getNewFiles().size());
    }
}