package com.farmerworking.leveldb.in.java.data.structure.db;

import com.farmerworking.leveldb.in.java.api.Iterator;
import com.farmerworking.leveldb.in.java.api.Options;
import com.farmerworking.leveldb.in.java.api.ReadOptions;
import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.common.TestUtils;
import com.farmerworking.leveldb.in.java.data.structure.block.EmptyIterator;
import com.farmerworking.leveldb.in.java.data.structure.cache.TableCache;
import com.farmerworking.leveldb.in.java.data.structure.log.ILogReader;
import com.farmerworking.leveldb.in.java.data.structure.log.ILogWriter;
import com.farmerworking.leveldb.in.java.data.structure.log.LogReader;
import com.farmerworking.leveldb.in.java.data.structure.log.LogWriter;
import com.farmerworking.leveldb.in.java.data.structure.memory.*;
import com.farmerworking.leveldb.in.java.data.structure.table.TableBuilder;
import com.farmerworking.leveldb.in.java.data.structure.utils.ConsoleLogger;
import com.farmerworking.leveldb.in.java.data.structure.utils.SimpleIterator;
import com.farmerworking.leveldb.in.java.data.structure.version.*;
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
import java.util.concurrent.atomic.AtomicBoolean;
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
        assertNotNull(db.getSnapshots());
        assertNotNull(db.getWriterList());
        assertNotNull(db.getTmpBatch());
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
    public void testNewDBErrorCase() throws IOException {
        TestUtils.deleteDirectory(dbname);
        options.getEnv().createDir(dbname);
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
        assertTrue(beforeStats.getMicros() <= db.getStats()[level].getMicros());

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

        Iterator<String, String> iter2 = memtable.iterator();
        iter1.seekToFirst();
        iter2.seekToFirst();
        assertTrue(iter1.valid() && iter2.valid());

        while(iter1.valid() && iter2.valid()) {
            assertEquals(iter1.key(), iter2.key());
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
        assertTrue(status.isCorruption());
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
        assertTrue(result.getStatus().isCorruption());
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
        assertTrue(pair.getKey().isCorruption());
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

        Thread.sleep(500);
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

    @Test(expected = AssertionError.class)
    public void testCompactMemtableWithoutLock() {
        db.compactMemtable();
    }

    @Test(expected = AssertionError.class)
    public void testCompactMemtableWithoutImmemtable() {
        db.getMutex().lock();
        assertNull(db.getImmutableMemtable());
        db.compactMemtable();
    }

    @Test
    public void testCompactMemtableExceptionCase() {
        spyDB.getMutex().lock();
        spyDB.setImmutableMemtable(new Memtable(spyDB.getInternalKeyComparator()));

        doReturn(Status.IOError("force write level 0 table error")).when(spyDB).writeLevel0Table(any(), any(), any());

        assertTrue(spyDB.getBgError().isOk());
        spyDB.compactMemtable();
        assertEquals("force write level 0 table error", spyDB.getBgError().getMessage());

        doReturn(Status.OK()).when(spyDB).writeLevel0Table(any(), any(), any());
        spyDB.setBgError(Status.OK());
        spyDB.getShuttingDown().set(true);
        spyDB.compactMemtable();
        assertEquals("Deleting DB during memtable compaction", spyDB.getBgError().getMessage());

        spyDB.setBgError(Status.OK());
        spyDB.getShuttingDown().set(false);
        doReturn(Status.IOError("force log and apply error")).when(spyDB).logAndApply(any());
        spyDB.compactMemtable();
        assertEquals("force log and apply error", spyDB.getBgError().getMessage());
    }

    @Test
    public void testCompactMemtable() {
        db.getMutex().lock();
        db.getOptions().setCreateIfMissing(true);
        db.recover(new VersionEdit());

        db.setImmutableMemtable(new Memtable(db.getInternalKeyComparator()));
        db.getImmutableMemtable().add(10L, ValueType.kTypeValue, TestUtils.randomKey(5), TestUtils.randomString(6));
        db.getVersions().setNextFileNumber(12);

        String log8 = FileName.logFileName(dbname, 8);
        String log9 = FileName.logFileName(dbname, 9);
        options.getEnv().newWritableFile(log8);
        options.getEnv().newWritableFile(log9);
        assertTrue(options.getEnv().isFileExists(log8));
        assertTrue(options.getEnv().isFileExists(log9));

        Version before = db.getVersions().getCurrent();
        int beforeTableCount = db.getVersions().getLiveFiles().size();
        assertNotEquals(10, db.getVersions().getLogNumber());
        db.setLogFileNumber(10L);
        db.compactMemtable();
        Version after = db.getVersions().getCurrent();

        assertNotEquals(before, after);
        assertEquals(beforeTableCount + 1, db.getVersions().getLiveFiles().size());
        assertEquals(10, db.getVersions().getLogNumber());
        assertNull(db.getImmutableMemtable());
        assertFalse(db.getHasImmutableMemtable().get());
        assertFalse(options.getEnv().isFileExists(log8));
        assertFalse(options.getEnv().isFileExists(log9));
    }

    @Test(expected = AssertionError.class)
    public void testBackgroundCall1() {
        db.setBgCompactionScheduled(false);
        db.backgroundCall();
    }

    @Test
    public void testBackgroundCall2() {
        db.setBgCompactionScheduled(true);

        db.getShuttingDown().set(true);
        assertFalse(db.backgroundCall());
        db.getShuttingDown().set(false);

        db.setBgCompactionScheduled(true);
        db.setBgError(Status.IOError(""));
        assertFalse(db.backgroundCall());
        db.setBgError(Status.OK());
    }

    @Test
    public void testBackgroundCall3() throws InterruptedException {
        spyDB.setBgCompactionScheduled(true);

        AtomicBoolean signal = new AtomicBoolean(false);
        AtomicBoolean goon = new AtomicBoolean(false);

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    spyDB.getMutex().lock();
                    goon.set(true);
                    spyDB.getBgCondition().await();
                    signal.set(true);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    spyDB.getMutex().unlock();
                }
            }
        }).start();

        while(!goon.get()) {}

        assertTrue(spyDB.backgroundCall());
        Thread.sleep(200);
        assertFalse(spyDB.isBgCompactionScheduled());
        assertTrue(signal.get());
        verify(spyDB, times(1)).maybeScheduleCompaction();
        verify(spyDB, times(1)).backgroundCompaction();
    }

    @Test(expected = AssertionError.class)
    public void testCleanupCompactionWithoutLock() {
        db.cleanupCompaction(null);
    }

    @Test(expected = AssertionError.class)
    public void testCleanupCompactionWithoutBuilderAndWithOutfile() {
        db.getMutex().lock();
        CompactionState compact = new CompactionState(null);
        compact.setOutfile(mock(WritableFile.class));
        db.cleanupCompaction(compact);
    }

    @Test
    public void testCleanupCompaction() {
        db.getMutex().lock();
        CompactionState compact = new CompactionState(null);

        db.pendingOutputs.add(1L);
        db.pendingOutputs.add(2L);
        db.pendingOutputs.add(3L);

        compact.add(new Output(1L));
        compact.add(new Output(3L));

        db.cleanupCompaction(compact);
        assertEquals(1, db.pendingOutputs.size());
        assertEquals(2, db.pendingOutputs.iterator().next().longValue());
    }

    @Test(expected = AssertionError.class)
    public void testOpenCompactionOutputFileAssertion1() {
        db.openCompactionOutputFile(null);
    }

    @Test(expected = AssertionError.class)
    public void testOpenCompactionOutputFileAssertion2() {
        CompactionState compact = new CompactionState(null);
        compact.setBuilder(mock(TableBuilder.class));

        db.openCompactionOutputFile(compact);
    }

    @Test
    public void testOpenCompactionOutputFileNewWritableFileError() {
        doReturn(new Pair<>(Status.IOError("force new writable file error"), null)).when(spyDB).newWritableFile(anyString());

        CompactionState compact = new CompactionState(null);
        Status status = spyDB.openCompactionOutputFile(compact);
        assertEquals("force new writable file error", status.getMessage());
    }

    @Test
    public void testOpenCompactionOutputFile() throws IOException {
        TestUtils.deleteDirectory(dbname);
        options.getEnv().createDir(dbname);

        CompactionState compact = new CompactionState(null);

        long fileNumber = db.getVersions().getNextFileNumber();
        assertFalse(db.getPendingOutputs().contains(fileNumber));
        assertTrue(compact.getOutputs().isEmpty());
        assertFalse(options.getEnv().isFileExists(FileName.tableFileName(dbname, fileNumber)));
        assertNull(compact.getBuilder());
        assertNull(compact.getOutfile());

        Status status = db.openCompactionOutputFile(compact);

        assertTrue(status.isOk());
        assertTrue(db.getPendingOutputs().contains(fileNumber));
        assertEquals(1, compact.getOutputs().size());
        Output output = compact.getOutputs().get(0);
        assertEquals(fileNumber, output.getNumber());
        assertNotNull(output.getSmallest());
        assertNotNull(output.getLargest());
        assertTrue(options.getEnv().isFileExists(FileName.tableFileName(dbname, fileNumber)));
        assertNotNull(compact.getBuilder());
        assertNotNull(compact.getOutfile());
    }

    @Test(expected = AssertionError.class)
    public void testInstallCompactionResultWithoutLock() {
        db.installCompactionResults(null);
    }

    @Test
    public void testInstallCompactionResultLogAndApplyError() {
        doReturn(Status.IOError("force log and apply error")).when(spyDB).logAndApply(any());

        Compaction compaction = new Compaction(options, 1);
        CompactionState compact = new CompactionState(compaction);

        spyDB.getMutex().lock();
        Status status = spyDB.installCompactionResults(compact);
        verify(spyDB, times(1)).finalizeCompactionState(compact);
        assertEquals("force log and apply error", status.getMessage());
    }

    @Test
    public void testFinalizeCompactionState() {
        Compaction compaction = new Compaction(options, 1);
        CompactionState compact = new CompactionState(compaction);

        FileMetaData input1 = mock(FileMetaData.class);
        doReturn(1L).when(input1).getFileNumber();
        FileMetaData input2 = mock(FileMetaData.class);
        doReturn(2L).when(input2).getFileNumber();
        FileMetaData input3 = mock(FileMetaData.class);
        doReturn(3L).when(input3).getFileNumber();

        compaction.getInputs()[0].add(input1);
        compaction.getInputs()[1].add(input2);
        compaction.getInputs()[1].add(input3);

        compact.getOutputs().add(mock(Output.class));
        compact.getOutputs().add(mock(Output.class));

        assertTrue(compaction.getEdit().getDeletedFiles().isEmpty());
        assertTrue(compaction.getEdit().getNewFiles().isEmpty());

        db.finalizeCompactionState(compact);

        assertEquals(3, compaction.getEdit().getDeletedFiles().size());
        assertEquals(2, compaction.getEdit().getNewFiles().size());
    }

    @Test(expected = AssertionError.class)
    public void testFinishCompactionOutoutFileException1() {
        db.finishCompactionOutputFile(null, null);
    }

    @Test(expected = AssertionError.class)
    public void testFinishCompactionOutoutFileException2() {
        db.finishCompactionOutputFile(mock(CompactionState.class), null);
    }

    @Test(expected = AssertionError.class)
    public void testFinishCompactionOutoutFileException3() {
        CompactionState compact = mock(CompactionState.class);
        doReturn(mock(WritableFile.class)).when(compact).getOutfile();
        db.finishCompactionOutputFile(compact, null);
    }

    @Test(expected = AssertionError.class)
    public void testFinishCompactionOutoutFileException4() {
        CompactionState compact = mock(CompactionState.class);
        doReturn(mock(WritableFile.class)).when(compact).getOutfile();
        doReturn(mock(TableBuilder.class)).when(compact).getBuilder();

        Output output = mock(Output.class);
        doReturn(0L).when(output).getNumber();
        doReturn(output).when(compact).currentOutput();

        db.finishCompactionOutputFile(compact, null);
    }

    @Test
    public void testFinishCompactionOutputFileIteratorStatusError() {
        CompactionState compact = spy(new CompactionState(null));
        db.openCompactionOutputFile(compact);

        TableBuilder builder = mock(TableBuilder.class);
        doReturn(builder).when(compact).getBuilder();

        Output output = mock(Output.class);
        doReturn(10L).when(output).getNumber();
        doReturn(output).when(compact).currentOutput();

        Iterator<String, String> iter = mock(Iterator.class);

        doReturn(Status.IOError("force iter status error")).when(iter).status();
        Status status = db.finishCompactionOutputFile(compact, iter);
        verify(builder, times(1)).abandon();
        assertEquals("force iter status error", status.getMessage());
    }

    @Test
    public void testFinishCompactionOutputFileSyncError() {
        CompactionState compact = spy(new CompactionState(null));
        db.openCompactionOutputFile(compact);

        Output output = mock(Output.class);
        doReturn(10L).when(output).getNumber();
        doReturn(output).when(compact).currentOutput();

        Iterator<String, String> iter = mock(Iterator.class);
        doReturn(Status.OK()).when(iter).status();

        WritableFile outfile = spy(compact.getOutfile());
        compact.setOutfile(outfile);
        doReturn(Status.IOError("force sync error")).when(outfile).sync();

        Status status = db.finishCompactionOutputFile(compact, iter);
        assertEquals("force sync error", status.getMessage());
    }

    @Test
    public void testFinishCompactionOutputFileCloseError() {
        CompactionState compact = spy(new CompactionState(null));
        db.openCompactionOutputFile(compact);

        Output output = mock(Output.class);
        doReturn(10L).when(output).getNumber();
        doReturn(output).when(compact).currentOutput();

        Iterator<String, String> iter = mock(Iterator.class);
        doReturn(Status.OK()).when(iter).status();

        WritableFile outfile = spy(compact.getOutfile());
        compact.setOutfile(outfile);
        doReturn(Status.IOError("force close error")).when(outfile).close();

        Status status = db.finishCompactionOutputFile(compact, iter);
        assertEquals("force close error", status.getMessage());
    }

    @Test
    public void testFinishCompactionOutputFileTableCacheVerifyError() {
        CompactionState compact = new CompactionState(null);
        db.openCompactionOutputFile(compact);

        TableBuilder builder = compact.getBuilder();
        builder.add(new InternalKey(TestUtils.randomKey(5), 100L).encode(), TestUtils.randomString(6));

        Iterator<String, String> iter = mock(Iterator.class);
        doReturn(Status.OK()).when(iter).status();

        TableCache tableCache = spy(db.getTableCache());
        db.setTableCache(tableCache);
        Iterator returnIter = mock(Iterator.class);
        doReturn(Status.IOError("force table cache verify error")).when(returnIter).status();
        doReturn(new Pair<>(returnIter, null)).when(tableCache).iterator(any(), anyLong(), anyLong());

        Status status = db.finishCompactionOutputFile(compact, iter);
        assertEquals("force table cache verify error", status.getMessage());
    }

    @Test
    public void testFinishCompactionOutputFile() {
        db.getOptions().setInfoLog(new ConsoleLogger());

        Compaction compaction = mock(Compaction.class);
        doReturn(3).when(compaction).getLevel();

        CompactionState compact = new CompactionState(compaction);
        db.openCompactionOutputFile(compact);

        TableBuilder builder = spy(compact.getBuilder());
        compact.setBuilder(builder);
        builder.add(new InternalKey(TestUtils.randomKey(5), 100L).encode(), TestUtils.randomString(6));

        assertNotNull(compact.getBuilder());
        assertNotNull(compact.getOutfile());
        assertEquals(0, compact.currentOutput().getFileSize());
        long before = compact.getTotalBytes();

        Iterator<String, String> iter = mock(Iterator.class);
        doReturn(Status.OK()).when(iter).status();

        Status status = db.finishCompactionOutputFile(compact, iter);

        assertTrue(status.isOk());
        assertNull(compact.getBuilder());
        assertNull(compact.getOutfile());
        assertEquals(builder.fileSize(), compact.currentOutput().getFileSize());
        assertEquals(before + builder.fileSize(), compact.getTotalBytes());
        verify(builder, times(1)).finish();
    }

    @Test
    public void testIsEntryDroppableInternalKeyParseError() {
        DBImpl.IterateInputState state = new DBImpl.IterateInputState();
        state.currentUserKey = "";
        state.hasCurrentUserKey = true;
        state.lastSequenceForKey = 1L;

        boolean result = db.isEntryDroppable(null, state, new Pair<>(false, null));
        assertFalse(result);
        assertEquals(new DBImpl.IterateInputState(), state);
    }

    @Test
    public void testIsEntryDroppable() {
        DBImpl.IterateInputState state = new DBImpl.IterateInputState();

        CompactionState compact = mock(CompactionState.class);
        doReturn(100L).when(compact).getSmallestSnapshot();

        Compaction compaction = mock(Compaction.class);
        doReturn(compaction).when(compact).getCompaction();
        doReturn(false).when(compaction).isBaseLevelForKey("f");
        doReturn(true).when(compaction).isBaseLevelForKey("g");

        List<Pair<Boolean, ParsedInternalKey>> list = Lists.newArrayList(
                new Pair<>(true, new InternalKey("a", 50L, ValueType.kTypeDeletion).convert()),
                new Pair<>(true, new InternalKey("b", 110L, ValueType.kTypeDeletion).convert()),
                new Pair<>(true, new InternalKey("c", 60L).convert()),
                new Pair<>(true, new InternalKey("d", 120L).convert()),

                new Pair<>(true, new InternalKey("e", 100L).convert()),
                new Pair<>(true, new InternalKey("e", 99L).convert()),
                new Pair<>(true, new InternalKey("e", 98L).convert()),

                new Pair<>(true, new InternalKey("f", 103L).convert()),
                new Pair<>(true, new InternalKey("f", 102L, ValueType.kTypeDeletion).convert()),
                new Pair<>(true, new InternalKey("f", 97L, ValueType.kTypeDeletion).convert()),

                new Pair<>(true, new InternalKey("g", 105L, ValueType.kTypeDeletion).convert()),
                new Pair<>(true, new InternalKey("g", 104L, ValueType.kTypeDeletion).convert()),
                new Pair<>(true, new InternalKey("g", 96L, ValueType.kTypeDeletion).convert()),
                new Pair<>(true, new InternalKey("g", 95L).convert())
        );

        List<Boolean> result = Lists.newArrayList(
                false,
                false,
                false,
                false,

                false,
                true,
                true,

                false,
                false,
                false,

                false,
                false,
                true,
                true
        );

        for (int i = 0; i < list.size(); i++) {
            boolean tmp = db.isEntryDroppable(compact, state, list.get(i));
            assertEquals(result.get(i), tmp);
        }
    }

    @Test
    public void testCompactMemtableFirst() throws InterruptedException {
        assertFalse(db.getHasImmutableMemtable().get());
        long cost = db.compactMemtableFirst();
        assertEquals(0, cost);

        spyDB.getHasImmutableMemtable().set(true);
        spyDB.setImmutableMemtable(mock(IMemtable.class));
        AtomicBoolean signal = new AtomicBoolean(false);
        AtomicBoolean goon = new AtomicBoolean(false);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                assertTrue(spyDB.getMutex().isHeldByCurrentThread());
                Thread.sleep(10);
                return null;
            }
        }).when(spyDB).compactMemtable();

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    spyDB.getMutex().lock();
                    goon.set(true);
                    spyDB.getBgCondition().await();
                    signal.set(true);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    spyDB.getMutex().unlock();
                }
            }
        }).start();

        while(!goon.get()) {}

        cost = spyDB.compactMemtableFirst();

        Thread.sleep(100);
        System.out.println(cost);
        verify(spyDB, times(1)).compactMemtable();
        assertFalse(spyDB.getMutex().isHeldByCurrentThread());
        assertTrue(signal.get());
        assertTrue(cost >= 10);
    }

    @Test
    public void testStopDuringIterateCompactionInput() {
        CompactionState compact = mock(CompactionState.class);
        Compaction compaction = mock(Compaction.class);
        doReturn(true).when(compaction).shouldStopBefore(anyString());
        doReturn(compaction).when(compact).getCompaction();
        doReturn(mock(TableBuilder.class)).when(compact).getBuilder();

        boolean result = spyDB.stopDuringIterateCompactionInput(compact, "");
        assertTrue(result);

        doReturn(null).when(compact).getBuilder();
        result = spyDB.stopDuringIterateCompactionInput(compact, null);
        assertFalse(result);
        doReturn(mock(TableBuilder.class)).when(compact).getBuilder();

        doReturn(false).when(compaction).shouldStopBefore(anyString());
        result = spyDB.stopDuringIterateCompactionInput(compact, null);
        assertFalse(result);
    }

    @Test
    public void testIterateInputEmptyIterator() {
        Iterator<String, String> iter = mock(Iterator.class);
        doReturn(false).when(iter).valid();

        Pair<Status, Long> pair = db.iterateInput(iter, null);
        assertEquals(0L, pair.getValue().longValue());
        assertTrue(pair.getKey().isOk());
    }

    @Test
    public void testIterateInputWhileShutingDown() {
        db.getShuttingDown().set(true);
        Iterator<String, String> iter = mock(Iterator.class);
        doReturn(true).when(iter).valid();

        Pair<Status, Long> pair = db.iterateInput(iter, null);
        assertEquals(0L, pair.getValue().longValue());
        assertTrue(pair.getKey().isOk());
    }

    @Test
    public void testIterateInputFinishCompactionOutputFileError1() {
        doReturn(1L).when(spyDB).compactMemtableFirst();
        doReturn(Status.IOError("force finish compaction output file error")).when(spyDB).
                finishCompactionOutputFile(any(), any());
        doReturn(true).when(spyDB).stopDuringIterateCompactionInput(any(), anyString());

        Iterator<String, String> iter = mock(Iterator.class);
        doReturn(true).when(iter).valid();
        doReturn("").when(iter).key();

        Pair<Status, Long> pair = spyDB.iterateInput(iter, null);
        assertEquals("force finish compaction output file error", pair.getKey().getMessage());
    }

    @Test
    public void testIterateInputFinishCompactionOutputFileError2() {
        doReturn(1L).when(spyDB).compactMemtableFirst();
        doReturn(Status.IOError("force finish compaction output file error")).when(spyDB).
                finishCompactionOutputFile(any(), any());
        doReturn(false).when(spyDB).stopDuringIterateCompactionInput(any(), anyString());
        doReturn(false).when(spyDB).isEntryDroppable(any(), any(), any());

        Iterator<String, String> iter = mock(Iterator.class);
        doReturn(true).when(iter).valid();
        doReturn(new InternalKey("a", 1L).encode()).when(iter).key();
        doReturn("value").when(iter).value();

        Compaction compaction = mock(Compaction.class);
        doReturn(0L).when(compaction).maxOutputFileSize();

        CompactionState compact = new CompactionState(compaction);
        Pair<Status, Long> pair = spyDB.iterateInput(iter, compact);
        assertEquals("force finish compaction output file error", pair.getKey().getMessage());
    }

    @Test
    public void testIterateInputOpenCompactionOutputFileError() {
        doReturn(1L).when(spyDB).compactMemtableFirst();
        doReturn(Status.IOError("force open compaction output file error")).when(spyDB).openCompactionOutputFile(any());
        doReturn(false).when(spyDB).stopDuringIterateCompactionInput(any(), anyString());
        doReturn(false).when(spyDB).isEntryDroppable(any(), any(), any());

        Iterator<String, String> iter = mock(Iterator.class);
        doReturn(true).when(iter).valid();
        doReturn(new InternalKey("a", 1L).encode()).when(iter).key();

        Pair<Status, Long> pair = spyDB.iterateInput(iter, new CompactionState(null));
        assertEquals("force open compaction output file error", pair.getKey().getMessage());
    }

    @Test
    public void testIterateInput() {
        doReturn(10L).when(spyDB).compactMemtableFirst();
        Iterator<String, String> iter = new SimpleIterator(Lists.newArrayList(
                new Pair(new InternalKey("a", 1L).encode(), "value1"),
                new Pair(new InternalKey("b", 100L).encode(), "value2"),
                new Pair(new InternalKey("b", 99L).encode(), "value3"),
                new Pair(new InternalKey("c", 5L).encode(), "value4"),
                new Pair(new InternalKey("d", 6L).encode(), "value5")
        ), spyDB.getInternalKeyComparator());
        doReturn(false, true, false).when(spyDB).stopDuringIterateCompactionInput(any(), anyString());
        doReturn(false, true, false).when(spyDB).isEntryDroppable(any(), any(), any());
        doReturn(false, true, false).when(spyDB).isBigEnough(any());

        Compaction compaction = new Compaction(options, 1);
        CompactionState compact = new CompactionState(compaction);

        assertEquals(0, compact.getOutputs().size());

        Pair<Status, Long> pair = spyDB.iterateInput(iter, compact);

        assertTrue(pair.getKey().isOk());
        assertTrue(pair.getValue() >= 10 * 5);
        assertEquals(3, compact.getOutputs().size());
        assertEquals(new InternalKey("a", 1L), compact.getOutputs().get(0).getSmallest());
        assertEquals(new InternalKey("a", 1L), compact.getOutputs().get(0).getLargest());

        assertEquals(new InternalKey("b", 99L), compact.getOutputs().get(1).getSmallest());
        assertEquals(new InternalKey("b", 99L), compact.getOutputs().get(1).getLargest());

        assertEquals(new InternalKey("c", 5L), compact.getOutputs().get(2).getSmallest());
        assertEquals(new InternalKey("d", 6L), compact.getOutputs().get(2).getLargest());
    }

    @Test(expected = AssertionError.class)
    public void testActualCompactShouldDoInLockReleaseStateForPerformance() {
        db.getMutex().lock();
        db.actualCompact(null, 0L, null);
    }

    @Test
    public void testActualCompactShuttingDown() {
        doReturn(new Pair<>(Status.OK(), 0L)).when(spyDB).iterateInput(any(), any());
        Compaction compaction = new Compaction(options, 1);
        CompactionState compact = new CompactionState(compaction);

        spyDB.getShuttingDown().set(true);
        Status status = spyDB.actualCompact(compact, 0L, new CompactionStats());

        assertEquals("Deleting DB during compaction", status.getMessage());
    }

    @Test
    public void testActualCompactInputError() {
        doReturn(new Pair<>(Status.OK(), 0L)).when(spyDB).iterateInput(any(), any());
        doReturn(new EmptyIterator(Status.Corruption("force iterator error"))).when(spyDB).makeInputIterator(any());

        Compaction compaction = new Compaction(options, 1);
        CompactionState compact = new CompactionState(compaction);

        Status status = spyDB.actualCompact(compact, 0L, new CompactionStats());

        assertEquals("force iterator error", status.getMessage());
    }

    @Test
    public void testActualCompactFinishCompactionOutputError() {
        doReturn(new Pair<>(Status.OK(), 0L)).when(spyDB).iterateInput(any(), any());
        doReturn(Status.Corruption("force finish compaction output error")).when(spyDB).finishCompactionOutputFile(any(), any());

        Compaction compaction = new Compaction(options, 1);
        CompactionState compact = new CompactionState(compaction);
        compact.setBuilder(mock(TableBuilder.class));

        Status status = spyDB.actualCompact(compact, 0L, new CompactionStats());

        assertEquals("force finish compaction output error", status.getMessage());
    }

    @Test
    public void testActualCompaction() {
        Compaction compaction = new Compaction(options, 1);

        CompactionState compact = new CompactionState(compaction);
        compact.setSmallestSnapshot(spyDB.getVersions().getLastSequence());

        compaction.getInputs()[0].add(new FileMetaData(1L, 100L, null, null));
        compaction.getInputs()[1].add(new FileMetaData(2L, 200L, null, null));
        compaction.setInputVersion(spyDB.getVersions().getCurrent());

        doReturn(new SimpleIterator(Lists.newArrayList(
                new Pair<>(new InternalKey("a", 1L, ValueType.kTypeValue).encode(), "value1"),
                new Pair<>(new InternalKey("b", 2L, ValueType.kTypeValue).encode(), "value2"),
                new Pair<>(new InternalKey("c", 3L, ValueType.kTypeValue).encode(), "value3")
        ), spyDB.getInternalKeyComparator())).when(spyDB).makeInputIterator(compact);

        CompactionStats stats = new CompactionStats();
        Status status = spyDB.actualCompact(compact, System.currentTimeMillis(), stats);
        assertTrue(status.isOk());
        assertEquals(300L, stats.getBytesRead());
        assertEquals(141L, stats.getBytesWritten());
        assertTrue(stats.getMicros() > 0);
    }

    @Test(expected = AssertionError.class)
    public void testDoCompactionWorkAssertion1() {
        CompactionState compactionState = mock(CompactionState.class);
        Compaction compaction = new Compaction(options, 1);
        doReturn(compaction).when(compactionState).getCompaction();

        doReturn(0).when(spyDB).numLevelFiles(anyInt());
        spyDB.doCompactionWork(compactionState);
    }

    @Test(expected = AssertionError.class)
    public void testDoCompactionWorkAssertion2() {
        doReturn(1).when(spyDB).numLevelFiles(anyInt());

        CompactionState compactionState = mock(CompactionState.class);
        Compaction compaction = new Compaction(options, 1);
        doReturn(compaction).when(compactionState).getCompaction();

        doReturn(mock(TableBuilder.class)).when(compactionState).getBuilder();
        spyDB.doCompactionWork(compactionState);
    }

    @Test(expected = AssertionError.class)
    public void testDoCompactionWorkAssertion3() {
        doReturn(1).when(spyDB).numLevelFiles(anyInt());

        CompactionState compactionState = mock(CompactionState.class);
        Compaction compaction = new Compaction(options, 1);
        doReturn(compaction).when(compactionState).getCompaction();
        doReturn(mock(WritableFile.class)).when(compactionState).getOutfile();

        spyDB.doCompactionWork(compactionState);
    }

    @Test
    public void testDoCompactionWorkActualCompactError() {
        Compaction compaction = new Compaction(options, 1);
        CompactionState compact = new CompactionState(compaction);

        doReturn(1).when(spyDB).numLevelFiles(anyInt());
        doReturn(Status.Corruption("force actual compact error")).when(spyDB).actualCompact(any(), anyLong(), any());

        spyDB.getMutex().lock();
        Status status = spyDB.doCompactionWork(compact);
        verify(spyDB, times(1)).recordBackgroundError(any());
        assertEquals("force actual compact error", status.getMessage());
    }

    @Test
    public void testDoCompactionWorkInstallCompactionResultError() {
        Compaction compaction = new Compaction(options, 1);
        CompactionState compact = new CompactionState(compaction);

        doReturn(1).when(spyDB).numLevelFiles(anyInt());
        doReturn(Status.OK()).when(spyDB).actualCompact(any(), anyLong(), any());
        doReturn(Status.Corruption("force install compaction result error")).when(spyDB).actualCompact(any(), anyLong(), any());

        spyDB.getMutex().lock();
        Status status = spyDB.doCompactionWork(compact);
        verify(spyDB, times(1)).recordBackgroundError(any());
        assertEquals("force install compaction result error", status.getMessage());
    }

    @Test
    public void testDoCompactionWork1() {
        spyDB.getMutex().lock();
        Compaction compaction = new Compaction(options, 1);
        CompactionState compact = new CompactionState(compaction);
        long snapshot = spyDB.getVersions().getLastSequence();
        doReturn(1).when(spyDB).numLevelFiles(anyInt());

        spyDB.getOptions().setCreateIfMissing(true);
        Pair<Status, Boolean> pair = spyDB.recover(new VersionEdit());
        assertTrue(pair.getKey().isOk());
        
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Thread.sleep(10);
                return invocation.callRealMethod();
            }
        }).when(spyDB).actualCompact(any(), anyLong(), any());

        CompactionStats before = new CompactionStats(spyDB.getStats()[compaction.getLevel() + 1]);
        Status status = spyDB.doCompactionWork(compact);

        assertTrue(status.isOk());
        assertEquals(snapshot, compact.getSmallestSnapshot().longValue());
        verify(spyDB, times(1)).installCompactionResults(compact);
        assertTrue(spyDB.getMutex().isHeldByCurrentThread());
        assertNotEquals(before, spyDB.getStats()[compaction.getLevel() + 1]);
    }

    @Test
    public void testDoCompactionWork2() {
        spyDB.getMutex().lock();
        Compaction compaction = new Compaction(options, 1);
        CompactionState compact = new CompactionState(compaction);
        spyDB.getSnapshots().add(9999L);
        long snapshot = spyDB.getSnapshots().getFirst();
        doReturn(1).when(spyDB).numLevelFiles(anyInt());

        spyDB.getOptions().setCreateIfMissing(true);
        Pair<Status, Boolean> pair = spyDB.recover(new VersionEdit());
        assertTrue(pair.getKey().isOk());

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Thread.sleep(10);
                return invocation.callRealMethod();
            }
        }).when(spyDB).actualCompact(any(), anyLong(), any());

        CompactionStats before = new CompactionStats(spyDB.getStats()[compaction.getLevel() + 1]);
        Status status = spyDB.doCompactionWork(compact);

        assertTrue(status.isOk());
        assertEquals(snapshot, compact.getSmallestSnapshot().longValue());
        verify(spyDB, times(1)).installCompactionResults(compact);
        assertTrue(spyDB.getMutex().isHeldByCurrentThread());
        assertNotEquals(before, spyDB.getStats()[compaction.getLevel() + 1]);
    }

    @Test
    public void testPickCompaction() {
        VersionSet versionSet = mock(VersionSet.class);

        db.setVersions(versionSet);
        Pair<Compaction, InternalKey> pair = db.pickCompaction(false);

        verify(versionSet, times(1)).pickCompaction();
        assertNull(pair.getValue());
    }

    @Test
    public void testPickCompactionManual1() {
        VersionSet versionSet = mock(VersionSet.class);
        doReturn(null).when(versionSet).compactRange(anyInt(), any(), any());
        db.setVersions(versionSet);
        db.setManualCompaction(new ManualCompaction());

        Pair<Compaction, InternalKey> pair = db.pickCompaction(true);

        verify(versionSet, never()).pickCompaction();
        assertTrue(db.getManualCompaction().isDone());
        assertNull(pair.getValue());
    }

    @Test
    public void testPickCompactionManual2() {
        VersionSet versionSet = mock(VersionSet.class);
        Compaction compaction = new Compaction(options, 1);
        InternalKey smallest = new InternalKey("a", 100L, ValueType.kTypeValue);
        InternalKey biggest = new InternalKey("g", 200L, ValueType.kTypeValue);
        compaction.getInputs()[0].add(new FileMetaData(1L, 1L, smallest, biggest));

        doReturn(compaction).when(versionSet).compactRange(anyInt(), any(), any());
        db.setVersions(versionSet);
        db.setManualCompaction(new ManualCompaction());

        Pair<Compaction, InternalKey> pair = db.pickCompaction(true);

        verify(versionSet, never()).pickCompaction();
        assertFalse(db.getManualCompaction().isDone());
        assertEquals(biggest, pair.getValue());
    }

    @Test
    public void testDoBackgroundCompactionNothingTodo() {
        Status status = db.doBackgroundCompaction(true, null);
        assertTrue(status.isOk());
    }

    @Test
    public void testDoBackgroundCompactionDoCompactionWorkError() {
        doReturn(Status.Corruption("force do compaction work error")).when(spyDB).doCompactionWork(any());
        spyDB.getMutex().lock();
        Status status = spyDB.doBackgroundCompaction(true, mock(Compaction.class));
        assertEquals("force do compaction work error", status.getMessage());
    }

    @Test
    public void testDoBackgroundCompaction() {
        Compaction compaction = mock(Compaction.class);
        doReturn(Status.OK()).when(spyDB).doCompactionWork(any());

        spyDB.getMutex().lock();
        Status status = spyDB.doBackgroundCompaction(true, compaction);

        assertTrue(status.isOk());
        verify(spyDB, times(1)).doCompactionWork(any());
        verify(spyDB, times(1)).cleanupCompaction(any());
        verify(spyDB, times(1)).deleteObsoleteFiles();
        verify(compaction, times(1)).releaseInputs();
    }

    @Test
    public void testDoBackgroundCompactionTrivialMoveLogAndApplyError() {
        doReturn(Status.Corruption("force log and apply error")).when(spyDB).logAndApply(any());

        Compaction compaction = new Compaction(options, 1);
        compaction.setInputVersion(spyDB.getVersions().getCurrent());
        compaction.getInputs()[0].add(mock(FileMetaData.class));
        assertTrue(compaction.isTrivialMove());

        spyDB.getMutex().lock();
        Status status = spyDB.doBackgroundCompaction(false, compaction);

        assertEquals("force log and apply error", status.getMessage());
    }

    @Test
    public void testDoBackgroundCompactionTrivialMove() {
        Compaction compaction = new Compaction(options, 1);
        compaction.setInputVersion(spyDB.getVersions().getCurrent());
        compaction.getInputs()[0].add(new FileMetaData(100L, 100L, null, null));

        assertTrue(compaction.isTrivialMove());
        assertTrue(compaction.getEdit().getNewFiles().isEmpty());
        assertTrue(compaction.getEdit().getDeletedFiles().isEmpty());

        doReturn(Status.OK()).when(spyDB).logAndApply(any());

        Status status = spyDB.doBackgroundCompaction(false, compaction);
        verify(spyDB, times(1)).logAndApply(compaction.getEdit());

        assertTrue(status.isOk());
        assertEquals(1, compaction.getEdit().getNewFiles().size());
        assertEquals(1, compaction.getEdit().getDeletedFiles().size());

        assertEquals(2, compaction.getEdit().getNewFiles().get(0).getKey().intValue());
        assertEquals(100L, compaction.getEdit().getNewFiles().get(0).getValue().getFileNumber());
        Pair<Integer, Long> tmp = compaction.getEdit().getDeletedFiles().iterator().next();
        assertEquals(1, tmp.getKey().intValue());
        assertEquals(100L, tmp.getValue().longValue());
    }

    @Test(expected =  AssertionError.class)
    public void testBackgroundCompactionHoldLockException() {
        db.backgroundCompaction();
    }

    @Test
    public void testBackgroundCompactionMemtableCompact() {
        spyDB.getMutex().lock();
        spyDB.setImmutableMemtable(mock(Memtable.class));
        doNothing().when(spyDB).compactMemtable();

        spyDB.backgroundCompaction();
        verify(spyDB, times(1)).compactMemtable();
    }

    @Test
    public void testBackgroundCompactionManualError() {
        ManualCompaction compaction = new ManualCompaction();
        spyDB.getMutex().lock();
        spyDB.setManualCompaction(compaction);

        doReturn(Status.Corruption("")).when(spyDB).doBackgroundCompaction(anyBoolean(), any());
        spyDB.backgroundCompaction();
        assertTrue(compaction.isDone());
        assertNull(spyDB.getManualCompaction());
    }

    @Test
    public void testBackgroundCompactionManualCompactionPart() {
        ManualCompaction compaction = new ManualCompaction();
        spyDB.getMutex().lock();
        spyDB.setManualCompaction(compaction);

        doReturn(Status.OK()).when(spyDB).doBackgroundCompaction(anyBoolean(), any());
        InternalKey start = new InternalKey();
        doReturn(new Pair<>(null, start)).when(spyDB).pickCompaction(anyBoolean());

        spyDB.backgroundCompaction();
        assertNull(spyDB.getManualCompaction());
        assertEquals(compaction.getBegin(), start);
        assertFalse(compaction.isDone());
    }
}