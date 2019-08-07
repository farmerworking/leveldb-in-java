package com.farmerworking.leveldb.in.java.data.structure.db;

import com.farmerworking.leveldb.in.java.api.Iterator;
import com.farmerworking.leveldb.in.java.api.Options;
import com.farmerworking.leveldb.in.java.api.ReadOptions;
import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.common.TestUtils;
import com.farmerworking.leveldb.in.java.data.structure.cache.TableCache;
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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import javafx.util.Pair;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;


import java.io.IOException;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class DBImplTest {
    Options options;
    String dbname;
    DBImpl db;
    DBImpl spyDB;

    @Before
    public void setUp() throws Exception {
        options = new Options();
        dbname = options.getEnv().getTestDirectory().getValue();
        db = new DBImpl(options, dbname);
        spyDB = spy(db);
    }

    @After
    public void tearDown() throws Exception {
        if (db != null && db.getDbLock() != null) {
            options.getEnv().unlockFile(FileName.lockFileName(dbname), db.getDbLock());
        }

        if (spyDB != null && spyDB.getDbLock() != null) {
            options.getEnv().unlockFile(FileName.lockFileName(dbname), spyDB.getDbLock());
        }
    }

    @Test(expected = AssertionError.class)
    public void testClipToRangeAssertionError() {
        DBImpl.clipToRange(null, "", 1, 0);
    }

    @Test
    public void testClipToRangeNoFieldException() {
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
        assertNotNull(db.getEnv());
        assertNotNull(db.getInternalKeyComparator());
        assertNotNull(db.getInternalFilterPolicy());
        assertNotNull(db.getOptions());
        assertNotSame(db.getOptions(), options);
        assertTrue(StringUtils.isNotEmpty(db.getDbname()));
        assertNull(db.getDbLock());
        assertNotNull(db.getMutex());
        assertFalse(db.getShuttingDown().get());
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
        assertTrue(db.getBgError().isOk());
        for (int i = 0; i < Config.kNumLevels; i++) {
            assertNotNull(db.getStats()[i]);
        }
    }

    @Test
    public void testNewDB() {
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
        Env spyEnv = spy(options.getEnv());
        db.setEnv(spyEnv);

        doReturn(new Pair<>(Status.IOError("force new writable file error"), null)).when(spyEnv).newWritableFile(anyString());
        Status status = db.newDB();
        verifyNewDBError(options, dbname, status, "force new writable file error");

        doCallRealMethod().when(spyEnv).newWritableFile(anyString());
        ILogWriter mockLogWriter = mock(LogWriter.class);
        doReturn(Status.IOError("force add record error")).when(mockLogWriter).addRecord(anyString());
        doReturn(mockLogWriter).when(spyDB).newDBGetLogWriter(any());

        status = spyDB.newDB();
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
        assertFalse(options.getEnv().isFileExists(FileName.descriptorFileName(dbname, DBImpl.NEW_DB_MANIFEST_NUMBER)));
        assertFalse(options.getEnv().isFileExists(FileName.currentFileName(dbname)));
    }

    @Test
    public void testWriteLevel0Table() {
        innerTestWriteLevel0Table(0, null);

        Version mockVersion = mock(Version.class);
        doReturn(2).when(mockVersion).pickLevelForMemTableOutput(anyString(), anyString());
        innerTestWriteLevel0Table(2, mockVersion);
    }

    private void innerTestWriteLevel0Table(int level, Version version) {
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
        assertTrue(options.getEnv().isFileExists(filename));

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
        Status status = db.writeLevel0Table(null, null, null);
    }

    @Test
    public void testWriteLevel0TableExceptionCase() {
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
        db.recoverLogFile(0L, true, null);
    }

    @Test
    public void testRecoverLogFileOpenLogFileError() {
        Env spyEnv = spy(options.getEnv());

        db.setEnv(spyEnv);
        db.getMutex().lock();

        doReturn(new Pair<>(Status.Corruption("force sequence file open error"), null)).when(spyEnv).newSequentialFile(anyString());
        RecoverLogFileResult result = db.recoverLogFile(5L, true, null);
        assertTrue(result.getStatus().isOk());

        db.getOptions().setParanoidChecks(true);
        result = db.recoverLogFile(5L, true, null);
        assertTrue(result.getStatus().IsCorruption());
        assertEquals("force sequence file open error", result.getStatus().getMessage());
    }

    @Test
    public void testRecoverLogFileWriteLevel0TableError() {
        options.getEnv().newWritableFile(FileName.logFileName(dbname, 5));
        db.getMutex().lock();

        doReturn(Status.Corruption("force write level0 table error")).when(spyDB).writeLevel0Table(any(), any(), any());

        Log2MemtableReader mockReader = mock(Log2MemtableReader.class);
        doReturn(Status.OK()).when(mockReader).getStatus();
        doReturn(mock(Memtable.class)).when(mockReader).getMemtable();
        doReturn(mockReader).when(mockReader).invoke();
        doReturn(mockReader).when(spyDB).getLog2MemtableReader(any(), anyString(), any());

        RecoverLogFileResult result = spyDB.recoverLogFile(5L, true, null);
        assertEquals("force write level0 table error", result.getStatus().getMessage());
    }

    @Test
    public void testRecoverLogFile() {
        options.getEnv().newWritableFile(FileName.logFileName(dbname, 5));

        WriteBatch batch = new WriteBatch();
        batch.put("foo", "bar");
        batch.setSequence(100);

        db.getMutex().lock();

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Log2MemtableReader reader = (Log2MemtableReader)invocation.callRealMethod();
                Log2MemtableReader spyReader = spy(reader);
                doReturn(new Pair<>(true, String.valueOf(batch.encode()))).doReturn(new Pair<>(false, null)).when(spyReader).readLogRecord(any());
                return spyReader;
            }
        }).when(spyDB).getLog2MemtableReader(any(), anyString(), any());

        VersionEdit edit = new VersionEdit();
        RecoverLogFileResult result = spyDB.recoverLogFile(5L, true, edit);
        assertTrue(result.getStatus().isOk());
        assertTrue(result.isSaveManifest());
        assertEquals(100, result.getMaxSequence());
        assertNull(spyDB.getMemtable());
        assertNull(spyDB.getLog());
        assertNull(spyDB.getLogFile());
        assertEquals(1, edit.getNewFiles().size());
    }

    @Test
    public void testRecoverLogFile2() {
        options.getEnv().newWritableFile(FileName.logFileName(dbname, 5));

        WriteBatch batch = new WriteBatch();
        batch.put("foo", "bar");
        batch.setSequence(100);

        db.getMutex().lock();

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Log2MemtableReader reader = (Log2MemtableReader)invocation.callRealMethod();
                Log2MemtableReader spyReader = spy(reader);
                doReturn(new Pair<>(true, String.valueOf(batch.encode()))).doReturn(new Pair<>(false, null)).when(spyReader).readLogRecord(any());
                return spyReader;
            }
        }).when(spyDB).getLog2MemtableReader(any(), anyString(), any());
        spyDB.getOptions().setReuseLogs(true);

        VersionEdit edit = new VersionEdit();
        RecoverLogFileResult result = spyDB.recoverLogFile(5L, true, edit);
        assertTrue(result.getStatus().isOk());
        assertFalse(result.isSaveManifest());
        assertEquals(100, result.getMaxSequence());
        assertNotNull(spyDB.getMemtable());
        assertNotNull(spyDB.getLog());
        assertNotNull(spyDB.getLogFile());
        assertEquals(0, edit.getNewFiles().size());
    }

    @Test
    public void testShouldReuseLog() {
        db.getMutex().lock();
        assertFalse(db.shouldReuseLog(Status.Corruption(""), true, 10));
        assertFalse(db.shouldReuseLog(Status.OK(), false, 10));
        assertFalse(db.shouldReuseLog(Status.OK(), true, 10));
        assertFalse(db.shouldReuseLog(Status.OK(), true, 0));

        db.getOptions().setReuseLogs(true);
        assertTrue(db.shouldReuseLog(Status.OK(), true, 0));
    }

    @Test(expected = AssertionError.class)
    public void testReuseLogException1() {
        db.setLogFile(mock(WritableFile.class));
        db.reuseLog(0L, "", null);
    }

    @Test(expected = AssertionError.class)
    public void testReuseLogException2() {
        db.setLog(mock(ILogWriter.class));
        db.reuseLog(0L, "", null);
    }

    @Test(expected = AssertionError.class)
    public void testReuseLogException3() {
        db.setMemtable(mock(IMemtable.class));
        db.reuseLog(0L, "", null);
    }

    @Test
    public void testReuseLogGetFileSizeError() {
        db.getMutex().lock();
        doReturn(new Pair<>(Status.Corruption(""), null)).when(spyDB).getFileSize(anyString());

        Memtable memtable = mock(Memtable.class);
        Memtable result = spyDB.reuseLog(0L, "", memtable);

        assertSame(result, memtable);
        assertNull(spyDB.getLogFile());
        assertNull(spyDB.getLog());
        assertNull(spyDB.getMemtable());
    }

    @Test
    public void testReuseLogGetAppendableFileError() {
        db.getMutex().lock();
        doReturn(new Pair<>(Status.Corruption(""), null)).when(spyDB).getAppendableFile(anyString());

        Memtable memtable = mock(Memtable.class);
        Memtable result = spyDB.reuseLog(0L, "", memtable);

        assertSame(result, memtable);
        assertNull(spyDB.getLogFile());
        assertNull(spyDB.getLog());
        assertNull(spyDB.getMemtable());
    }

    @Test
    public void testReuseLog1() {
        db.getMutex().lock();
        long logFileNumber = 5L;
        String filename = FileName.logFileName(dbname, logFileNumber);
        options.getEnv().newWritableFile(filename);

        Memtable memtable = mock(Memtable.class);
        Memtable result = db.reuseLog(logFileNumber, filename, memtable);

        assertNull(result);
        assertNotNull(db.getLog());
        assertNotNull(db.getLogFile());
        assertEquals(logFileNumber, db.getLogFileNumber());
        assertSame(memtable, db.getMemtable());
    }

    @Test
    public void testReuseLog2() {
        db.getMutex().lock();
        long logFileNumber = 5L;
        String filename = FileName.logFileName(dbname, logFileNumber);
        options.getEnv().newWritableFile(filename);

        Memtable result = db.reuseLog(logFileNumber, filename, null);

        assertNull(result);
        assertNotNull(db.getLog());
        assertNotNull(db.getLogFile());
        assertEquals(logFileNumber, db.getLogFileNumber());
        assertNotNull(db.getMemtable());
    }

    @Test(expected = AssertionError.class)
    public void testReuseLogWithoutLock() {
        db.reuseLog(5L, "", null);
    }

    @Test(expected = AssertionError.class)
    public void testShouldReuseLogWithoutLock() {
        db.shouldReuseLog(null, true, 0);
    }

    @Test(expected = AssertionError.class)
    public void testRecoverWithoutLock() {
        db.recover(null);
    }

    @Test
    public void testRecoverCreateDbDirectoryIfNotExist() throws IOException {
        TestUtils.deleteDirectory(dbname);
        assertFalse(options.getEnv().isFileExists(dbname));

        db.getMutex().lock();
        db.recover(null);
        assertTrue(options.getEnv().isFileExists(dbname));
    }

    @Test(expected = AssertionError.class)
    // with dbLock means recover more than once
    public void testRecoverWithDbLock() {
        db.getMutex().lock();
        db.setDbLock(mock(FileLock.class));
        db.recover(null);
    }

    @Test
    public void testRecoverRequireFileLockFail() {
        db.getMutex().lock();

        doReturn(new Pair<>(Status.IOError("force lock file error"), null)).when(spyDB).lockFile();
        Pair<Status, Boolean> pair = spyDB.recover(null);
        assertEquals("force lock file error", pair.getKey().getMessage());
    }

    @Test
    public void testRecoverNotExistError() {
        db.getMutex().lock();
        db.getOptions().setCreateIfMissing(false);
        String current = FileName.currentFileName(dbname);
        options.getEnv().delete(current);
        assertFalse(options.getEnv().isFileExists(current));

        Pair<Status, Boolean> pair = db.recover(null);
        assertEquals(dbname + ": does not exist (createIfMissing is false)", pair.getKey().getMessage());
    }

    @Test
    public void testRecoverExistError() {
        db.getMutex().lock();
        db.getOptions().setErrorIfExists(true);
        String current = FileName.currentFileName(dbname);
        options.getEnv().newWritableFile(current);
        assertTrue(options.getEnv().isFileExists(current));

        Pair<Status, Boolean> pair = db.recover(null);
        assertEquals(dbname + ": exists (errorIfExists is true)", pair.getKey().getMessage());

    }

    @Test
    public void testRecoverNewDBError() {
        db.getMutex().lock();
        db.getOptions().setCreateIfMissing(true);
        String current = FileName.currentFileName(dbname);
        options.getEnv().delete(current);
        assertFalse(options.getEnv().isFileExists(current));

        doReturn(Status.IOError("force new db error") ).when(spyDB).newDB();
        Pair<Status, Boolean> pair = spyDB.recover(null);

        assertEquals("force new db error", pair.getKey().getMessage());
    }

    @Test
    public void testRecoverVersionsRecoverError() {
        db.getMutex().lock();
        db.getOptions().setCreateIfMissing(true);

        doReturn(new Pair<>(Status.Corruption("force versions recovery error"), null)).when(spyDB).versionsRecover();

        Pair<Status, Boolean> pair = spyDB.recover(null);
        assertEquals("force versions recovery error", pair.getKey().getMessage());
    }

    @Test
    public void testRecoverGetChildrenError() {
        db.getMutex().lock();
        db.getOptions().setCreateIfMissing(true);

        doReturn(new Pair<>(Status.IOError("force get children error"), null)).when(spyDB).getChildren(anyString());

        Pair<Status, Boolean> pair = spyDB.recover(null);
        assertEquals("force get children error", pair.getKey().getMessage());
    }

    @Test
    public void testRecoverLiveFileMissing() {
        db.getMutex().lock();
        db.getOptions().setCreateIfMissing(true);

        doReturn(new Pair<>(Status.OK(), new ArrayList<>())).when(spyDB).getChildren(anyString());
        doReturn(Sets.newHashSet(1L)).when(spyDB).getLiveFiles();
        doReturn(new Pair<>(Status.OK(), false)).when(spyDB).versionsRecover();

        Pair<Status, Boolean> pair = spyDB.recover(null);
        assertTrue(pair.getKey().IsCorruption());
        assertEquals("1 missing file; e.g. " + dbname + "/1.ldb", pair.getKey().getMessage());
    }

    @Test
    public void testFilterRecoverLog() {
        List<String> filenames = Lists.newArrayList(
                "whatever", "1.ldb", "2.sst", "7.log", "6.log", "5.log", "4.log", "9.sst", "10.ldb"
        );

        Set<Long> liveTableFileNumbers = Sets.newHashSet(
                1L, 2L, 3L
        );

        db.getMutex().lock();
        List<Long> result = db.filterRecoverLog(6, 4, filenames, liveTableFileNumbers);

        assertEquals(1, liveTableFileNumbers.size());
        assertEquals(3L, liveTableFileNumbers.iterator().next().longValue());
        List<Long> expect = Lists.newArrayList(4L, 6L, 7L);
        assertArrayEquals(expect.toArray(), result.toArray());

        java.util.Iterator<Long> iter = result.iterator();
        Long last = null;
        while(iter.hasNext()) {
            Long item = iter.next();
            if (last != null) {
                assertTrue(item > last);
            }
            last = item;
        }
    }

    @Test(expected = AssertionError.class)
    public void testFilterRecoverLogWithoutLock() {
        List<Long> result = db.filterRecoverLog(1, 1, null, null);
    }

    @Test
    public void testRecoverLogFilesEmpty() {
        db.getMutex().lock();
        Pair<Status, Pair<Long, Boolean>> result = db.recoverLogFiles(Lists.newArrayList(), null);

        assertTrue(result.getKey().isOk());
        assertEquals(0L, result.getValue().getKey().longValue());
        assertEquals(false, result.getValue().getValue());
    }

    @Test
    public void testRecoverLogFilesError() {
        db.getMutex().lock();

        doReturn(new RecoverLogFileResult(Status.IOError("force recover error"))).when(spyDB).recoverLogFile(
                anyLong(), anyBoolean(), any());
        Pair<Status, Pair<Long, Boolean>> result = spyDB.recoverLogFiles(Lists.newArrayList(1L), null);

        assertEquals("force recover error", result.getKey().getMessage());
    }

    @Test(expected = AssertionError.class)
    public void testRecoverLogFilesWithoutLock() {
        db.recoverLogFiles(Lists.newArrayList(), null);
    }

    @Test
    public void testRecoverLogFiles1() {
        db.getMutex().lock();

        final long[] sequence = {db.getVersions().getNextFileNumber()};
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                return new RecoverLogFileResult(Status.OK(), false, sequence[0]++);
            }
        }).when(spyDB).recoverLogFile(anyLong(), anyBoolean(), any());
        Pair<Status, Pair<Long, Boolean>> result = spyDB.recoverLogFiles(Lists.newArrayList(11L, 12L, 13L, 14L), null);

        assertTrue(result.getKey().isOk());
        assertEquals(sequence[0] - 1, result.getValue().getKey().longValue());
        assertFalse(result.getValue().getValue());
        assertEquals(15, spyDB.getVersions().getNextFileNumber());
    }

    @Test
    public void testRecoverLogFiles2() {
        db.getMutex().lock();

        final long[] sequence = {db.getVersions().getNextFileNumber()};
        doReturn(new RecoverLogFileResult(Status.OK(), true, 0L)).doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                return new RecoverLogFileResult(Status.OK(), false, sequence[0]++);
            }
        }).when(spyDB).recoverLogFile(anyLong(), anyBoolean(), any());
        Pair<Status, Pair<Long, Boolean>> result = spyDB.recoverLogFiles(Lists.newArrayList(11L, 12L, 13L, 14L), null);

        assertTrue(result.getKey().isOk());
        assertEquals(sequence[0] - 1, result.getValue().getKey().longValue());
        assertTrue(result.getValue().getValue());
        assertEquals(15, spyDB.getVersions().getNextFileNumber());
    }

    @Test
    public void testRecoverRecoverLogFilesError() {
        db.getMutex().lock();
        db.getOptions().setCreateIfMissing(true);
        doReturn(new Pair<>(Status.Corruption("force recover log files error"), null)).when(spyDB).recoverLogFiles(anyList(), any());

        Pair<Status, Boolean> pair = spyDB.recover(null);
        assertEquals("force recover log files error", pair.getKey().getMessage());
    }

    @Test
    public void testRecoverWhenCreateNewDB() throws IOException {
        db.getMutex().lock();
        db.getOptions().setCreateIfMissing(true);
        TestUtils.deleteDirectory(dbname);

        assertNull(db.getDbLock());
        assertEquals(0, db.getVersions().getManifestFileNumber());
        assertEquals(2, db.getVersions().getNextFileNumber());

        VersionEdit edit = new VersionEdit();
        Pair<Status, Boolean> pair = db.recover(edit);

        assertTrue(pair.getKey().isOk());
        assertTrue(pair.getValue());
        assertNotNull(db.getDbLock());
        assertEquals(2, db.getVersions().getManifestFileNumber());
        assertEquals(3, db.getVersions().getNextFileNumber());
        assertNull(db.getVersions().getDescriptorLog());
        assertNull(db.getVersions().getDescriptorFile());
        assertEquals(new VersionEdit(), edit);
    }

    @Test
    public void testRecoverWhenCreateNewDBReuseManifest() throws IOException {
        db.getMutex().lock();
        db.getOptions().setCreateIfMissing(true);
        db.getOptions().setReuseLogs(true);

        TestUtils.deleteDirectory(dbname);

        assertNull(db.getDbLock());
        assertEquals(0, db.getVersions().getManifestFileNumber());
        assertEquals(2, db.getVersions().getNextFileNumber());

        VersionEdit edit = new VersionEdit();
        Pair<Status, Boolean> pair = db.recover(edit);

        assertTrue(pair.getKey().isOk());
        assertFalse(pair.getValue());
        assertNotNull(db.getDbLock());
        assertEquals(1, db.getVersions().getManifestFileNumber());
        assertEquals(3, db.getVersions().getNextFileNumber());
        assertNotNull(db.getVersions().getDescriptorLog());
        assertNotNull(db.getVersions().getDescriptorFile());
        assertEquals(new VersionEdit(), edit);
    }

    @Test
    public void testRecoverFromExistDb1() throws IOException {
        db.getMutex().lock();
        db.getOptions().setReuseLogs(true);
        TestUtils.deleteDirectory(dbname);

        prepareRecoverFromExistDb();

        assertNull(db.getDbLock());
        assertEquals(0, db.getVersions().getManifestFileNumber());
        assertEquals(2, db.getVersions().getNextFileNumber());
        assertEquals(0, db.getVersions().getLastSequence());
        assertEquals(0, db.getVersions().getLogNumber());
        assertEquals(0, db.getVersions().getPrevLogNumber());
        assertEquals(0, db.getVersions().getLiveFiles().size());
        assertNull(db.getMemtable());

        VersionEdit edit = new VersionEdit();
        Pair<Status, Boolean> pair = db.recover(edit);

        assertTrue(pair.getKey().isOk());
        assertTrue(pair.getValue());
        assertNotNull(db.getDbLock());
        assertEquals(9, db.getVersions().getManifestFileNumber());
        assertEquals(18, db.getVersions().getNextFileNumber()); // recover log file write 2 level0 table
        assertEquals(153, db.getVersions().getLastSequence());
        assertEquals(10, db.getVersions().getLogNumber());
        assertEquals(4, db.getVersions().getPrevLogNumber());
        assertEquals(1, db.getVersions().getLiveFiles().size());
        assertEquals(3L, db.getVersions().getLiveFiles().iterator().next().longValue());
        assertNotNull(db.getVersions().getDescriptorLog());
        assertNotNull(db.getVersions().getDescriptorFile());
        assertNotNull(db.getMemtable());
        assertEquals(2, edit.getNewFiles().size());
        assertEquals(0, edit.getNewFiles().get(0).getKey().intValue());
        assertEquals(0, edit.getNewFiles().get(1).getKey().intValue());
        assertEquals(16, edit.getNewFiles().get(0).getValue().getFileNumber());
        assertEquals(17, edit.getNewFiles().get(1).getValue().getFileNumber());
        assertEquals(edit.getNewFiles().get(0).getValue().getSmallest(), edit.getNewFiles().get(0).getValue().getLargest());
        assertEquals(edit.getNewFiles().get(1).getValue().getSmallest(), edit.getNewFiles().get(1).getValue().getLargest());
        assertTrue(options.getEnv().isFileExists(FileName.tableFileName(dbname, 17)));
        assertTrue(options.getEnv().isFileExists(FileName.tableFileName(dbname, 16)));
    }

    @Test
    public void testRecoverFromExistDb2() throws IOException {
        db.getMutex().lock();
        TestUtils.deleteDirectory(dbname);

        prepareRecoverFromExistDb();

        assertNull(db.getDbLock());
        assertEquals(0, db.getVersions().getManifestFileNumber());
        assertEquals(2, db.getVersions().getNextFileNumber());
        assertEquals(0, db.getVersions().getLastSequence());
        assertEquals(0, db.getVersions().getLogNumber());
        assertEquals(0, db.getVersions().getPrevLogNumber());
        assertEquals(0, db.getVersions().getLiveFiles().size());
        assertNull(db.getMemtable());

        VersionEdit edit = new VersionEdit();
        Pair<Status, Boolean> pair = db.recover(edit);

        assertTrue(pair.getKey().isOk());
        assertTrue(pair.getValue());
        assertNotNull(db.getDbLock());
        assertEquals(15, db.getVersions().getManifestFileNumber());
        assertEquals(19, db.getVersions().getNextFileNumber()); // recover log file write 3 level0 table
        assertEquals(153, db.getVersions().getLastSequence());
        assertEquals(10, db.getVersions().getLogNumber());
        assertEquals(4, db.getVersions().getPrevLogNumber());
        assertEquals(1, db.getVersions().getLiveFiles().size());
        assertEquals(3L, db.getVersions().getLiveFiles().iterator().next().longValue());
        assertNull(db.getVersions().getDescriptorLog());
        assertNull(db.getVersions().getDescriptorFile());
        assertNull(db.getMemtable());
        assertEquals(3, edit.getNewFiles().size());
        assertEquals(0, edit.getNewFiles().get(0).getKey().intValue());
        assertEquals(0, edit.getNewFiles().get(1).getKey().intValue());
        assertEquals(0, edit.getNewFiles().get(2).getKey().intValue());
        assertEquals(16, edit.getNewFiles().get(0).getValue().getFileNumber());
        assertEquals(17, edit.getNewFiles().get(1).getValue().getFileNumber());
        assertEquals(18, edit.getNewFiles().get(2).getValue().getFileNumber());
        assertEquals(edit.getNewFiles().get(0).getValue().getSmallest(), edit.getNewFiles().get(0).getValue().getLargest());
        assertEquals(edit.getNewFiles().get(1).getValue().getSmallest(), edit.getNewFiles().get(1).getValue().getLargest());
        assertEquals(edit.getNewFiles().get(2).getValue().getSmallest(), edit.getNewFiles().get(2).getValue().getLargest());
        assertTrue(options.getEnv().isFileExists(FileName.tableFileName(dbname, 18)));
        assertTrue(options.getEnv().isFileExists(FileName.tableFileName(dbname, 17)));
        assertTrue(options.getEnv().isFileExists(FileName.tableFileName(dbname, 16)));
    }

    private void prepareRecoverFromExistDb() {
        options.getEnv().createDir(dbname);
        String manifest = FileName.descriptorFileName(dbname, 9);

        VersionEdit edit = new VersionEdit();
        edit.setLogNumber(10);
        edit.setPrevLogNumber(4);
        edit.setLastSequence(100);
        edit.setNextFileNumber(15);
        edit.setComparatorName(db.getInternalKeyComparator().getUserComparator().name());
        Vector<Pair<Integer, FileMetaData>> vector = new Vector<>();
        vector.add(new Pair<>(0, new FileMetaData(3, 0L, new InternalKey("a", 10L), new InternalKey("b", 20L))));
        edit.setNewFiles(vector);

        options.getEnv().newWritableFile(FileName.tableFileName(dbname, 3));

        Pair<Status, WritableFile> writable = options.getEnv().newWritableFile(manifest);
        assertTrue(writable.getKey().isOk());
        LogWriter logWriter = new LogWriter(writable.getValue());
        StringBuilder builder = new StringBuilder();
        edit.encodeTo(builder);
        logWriter.addRecord(builder.toString());
        writable.getValue().flush();
        writable.getValue().sync();
        writable.getValue().close();

        FileName.setCurrentFile(options.getEnv(), dbname, 9);

        // log
        List<Long> logs = Lists.newArrayList(4L, 9L, 10L, 11L);
        long sequence = 150L;
        for(Long log : logs) {
            WriteBatch writeBatch = new WriteBatch();
            writeBatch.setSequence(sequence++);
            writeBatch.put(TestUtils.randomKey(5), TestUtils.randomString(6));

            String logFileName = FileName.logFileName(dbname, log);
            writable = options.getEnv().newWritableFile(logFileName);
            assertTrue(writable.getKey().isOk());

            logWriter = new LogWriter(writable.getValue());
            String record = String.valueOf(writeBatch.encode());
            logWriter.addRecord(record);

            writable.getValue().flush();
            writable.getValue().sync();
            writable.getValue().close();
        }
    }

    @Test
    public void testDeleteObsoleteFiles() throws IOException {
        TestUtils.deleteDirectory(dbname);
        options.getEnv().createDir(dbname);
        assertTrue(options.getEnv().getChildren(dbname).getValue().isEmpty());

        String current = FileName.currentFileName(dbname);
        String dbLock = FileName.lockFileName(dbname);
        String infoLog = FileName.infoLogFileName(dbname);

        long number = 100L;
        String tmp1 = FileName.tempFileName(dbname, number ++);
        long tmp2Number = number;
        String tmp2 = FileName.tempFileName(dbname, number ++);

        long table1Number = number;
        String table1 = FileName.tableFileName(dbname, number ++);
        long table2Number = number;
        String table2 = FileName.tableFileName(dbname, number ++);

        String manifest1 = FileName.descriptorFileName(dbname, number ++);
        long manifest2Number = number;
        String manifest2 = FileName.descriptorFileName(dbname, number ++);
        String manifest3 = FileName.descriptorFileName(dbname, number ++);

        long log1Number = number;
        String log1 = FileName.logFileName(dbname, number ++);
        String log2 = FileName.logFileName(dbname, number ++);
        long log3Number = number;
        String log3 = FileName.logFileName(dbname, number ++);
        String log4 = FileName.logFileName(dbname, number ++);

        ArrayList<String> files = Lists.newArrayList(
                dbname + "/whatever", current, dbLock, infoLog, tmp1, tmp2, table1, table2, manifest1, manifest2, manifest3, log1, log2, log3, log4);
        for(String file : files) {
            Pair<Status, WritableFile> writable = options.getEnv().newWritableFile(file);
            assertTrue(writable.getKey().isOk());
        }
        assertEquals(files.size(), options.getEnv().getChildren(dbname).getValue().size());

        // prepare start
        TableCache spyTableCache = spy(spyDB.getTableCache());
        spyDB.setTableCache(spyTableCache);
        spyDB.getVersions().setLogNumber(log3Number);
        spyDB.getVersions().setPrevLogNumber(log1Number);
        spyDB.getVersions().setManifestFileNumber(manifest2Number);
        doReturn(Sets.newHashSet(table1Number)).when(spyDB).getLiveFiles();
        spyDB.getPendingOutputs().add(tmp2Number);
        // prepare end

        // exception case start
        bgError(files);
        getChildrenError(files);
        // exception case end

        spyDB.deleteObsoleteFiles();

        // verify
        ArrayList<String> keep = Lists.newArrayList(
                dbname + "/whatever", current, dbLock, infoLog, tmp2, table1, manifest2, manifest3, log1, log3, log4);
        ArrayList<String> delete = Lists.newArrayList(
                tmp1, table2, manifest1, log2);

        for(String item : keep) {
            assertTrue(item, options.getEnv().isFileExists(item));
        }
        for(String item : delete) {
            assertFalse(options.getEnv().isFileExists(item));
        }
        assertEquals(keep.size(), options.getEnv().getChildren(dbname).getValue().size());
        verify(spyTableCache, times(1)).evict(table2Number);
    }

    private void getChildrenError(ArrayList<String> files) {
        doReturn(new Pair<>(Status.IOError(""), null)).when(spyDB).getChildren(anyString());

        for(String item : files) {
            assertTrue(item, options.getEnv().isFileExists(item));
        }

        doCallRealMethod().when(spyDB).getChildren(anyString());
    }

    private void bgError(ArrayList<String> files) {
        spyDB.setBgError(Status.IOError(""));
        for(String item : files) {
            assertTrue(item, options.getEnv().isFileExists(item));
        }
        spyDB.setBgError(Status.OK());
    }

    @Test(expected = AssertionError.class)
    public void testRecordBackgroundErrorWithoutLock() {
        db.recordBackgroundError(Status.OK());
    }

    @Test
    public void testRecordBackgroundError() throws InterruptedException {
        Status status = Status.Corruption("");
        assertNotEquals(status, db.getBgError());

        AtomicInteger signal = new AtomicInteger(0);

        ReentrantLock lock = new ReentrantLock();
        Condition cond = lock.newCondition();

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while(true) {
                        db.getMutex().lock();
                        db.getBgCondition().await();
                        signal.incrementAndGet();

                        lock.lock();
                        cond.signalAll();
                        lock.unlock();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    db.getMutex().unlock();
                }
            }
        }).start();

        Thread.sleep(100);
        db.getMutex().lock();
        db.recordBackgroundError(status);
        db.getMutex().unlock();

        lock.lock();
        cond.await();
        lock.unlock();

        assertEquals(status, db.getBgError());
        assertEquals(1, signal.get());

        db.getMutex().lock();
        db.recordBackgroundError(Status.IOError(""));
        db.getMutex().unlock();

        assertEquals(status, db.getBgError());
        assertEquals(1, signal.get());
    }

    @Test
    public void testIsManualCompaction() {
        assertFalse(db.isManualCompaction());

        db.setManualCompaction(new ManualCompaction());
        assertTrue(db.isManualCompaction());
    }

    @Test(expected = AssertionError.class)
    public void testMaybeScheduleCompactionWithoutLock() {
        db.maybeScheduleCompaction();
    }

    @Test
    public void testMaybeScheduleCompaction() {
        spyDB.getMutex().lock();

        doNothing().when(spyDB).schedule();

        assertFalse(spyDB.maybeScheduleCompaction());

        spyDB.setImmutableMemtable(new Memtable(spyDB.getInternalKeyComparator()));
        assertTrue(spyDB.maybeScheduleCompaction());
        assertTrue(spyDB.isBgCompactionScheduled());

        spyDB.setBgCompactionScheduled(true);
        assertFalse(spyDB.maybeScheduleCompaction());
        spyDB.setBgCompactionScheduled(false);

        spyDB.getShuttingDown().set(true);
        assertFalse(spyDB.maybeScheduleCompaction());
        spyDB.getShuttingDown().set(false);

        spyDB.setBgError(Status.IOError(""));
        assertFalse(spyDB.maybeScheduleCompaction());
        spyDB.setBgError(Status.OK());

        assertTrue(spyDB.maybeScheduleCompaction());
        assertTrue(spyDB.isBgCompactionScheduled());
    }
}