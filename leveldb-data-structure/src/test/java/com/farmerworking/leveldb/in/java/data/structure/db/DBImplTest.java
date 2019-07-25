package com.farmerworking.leveldb.in.java.data.structure.db;

import com.farmerworking.leveldb.in.java.api.Options;
import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.data.structure.log.ILogReader;
import com.farmerworking.leveldb.in.java.data.structure.log.ILogWriter;
import com.farmerworking.leveldb.in.java.data.structure.log.LogReader;
import com.farmerworking.leveldb.in.java.data.structure.log.LogWriter;
import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKeyComparator;
import com.farmerworking.leveldb.in.java.data.structure.version.VersionEdit;
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
        assertNotNull(db.getTmpBatch());
        assertFalse(db.isBgCompactionScheduled());
        assertNull(db.getManualCompaction());
        assertNotNull(db.getHasImmutableMemtable());
        assertFalse(db.getHasImmutableMemtable().get());
        assertNotNull(db.getTableCache());
        assertNotNull(db.getVersions());
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
}