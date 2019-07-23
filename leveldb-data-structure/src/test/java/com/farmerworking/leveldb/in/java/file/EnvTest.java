package com.farmerworking.leveldb.in.java.file;

import com.farmerworking.leveldb.in.java.api.Options;
import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.common.TestUtils;
import javafx.util.Pair;
import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Random;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public abstract class EnvTest {
    Env env;

    protected abstract Env getImpl();
    
    @Before
    public void setUp() throws Exception {
        env = getImpl();
    }

    @Test
    public void testReadWrite() {
        Pair<Status, String> pair = env.getTestDirectory();
        assertTrue(pair.getKey().isOk());
        String testDir = pair.getValue();

        // Get file to use for testing.
        String testFileName = testDir + "/open_on_read.txt";
        env.delete(testFileName);

        Pair<Status, WritableFile> filePair = env.newWritableFile(testFileName);
        assertTrue(filePair.getKey().isOk());
        WritableFile writableFile = filePair.getValue();

        // Fill a file with data generated via a sequence of randomly sized writes.
        int kDataSize = 10 * 1048576;
        StringBuilder builder = new StringBuilder();
        Random random = new Random();
        while (builder.length() < kDataSize) {
            int len = random.nextInt(1024 * 100);  // Up to 2^18 - 1, but typically much smaller
            String s = TestUtils.randomString(len);
            assertTrue(writableFile.append(s).isOk());
            builder.append(s);
            if (random.nextInt(10) == 0) {
                assertTrue(writableFile.flush().isOk());
            }
        }
        assertTrue(writableFile.sync().isOk());
        assertTrue(writableFile.close().isOk());

        // Read all data using a sequence of randomly sized reads.
        Pair<Status, SequentialFile> filePair2 = env.newSequentialFile(testFileName);
        assertTrue(filePair2.getKey().isOk());
        SequentialFile sequentialFile = filePair2.getValue();

        StringBuilder readResult = new StringBuilder();
        while (readResult.length() < builder.length()) {
            int len = Math.min(random.nextInt(1024 * 100), builder.length() - readResult.length());
            Pair<Status, String> readPair = sequentialFile.read(len);
            assertTrue(readPair.getKey().isOk());
            if (len > 0) {
                assertTrue(readPair.getValue().length() > 0);
            }
            assertTrue(readPair.getValue().length() <= len);
            readResult.append(readPair.getValue());
        }
        assertEquals(readResult.toString(), builder.toString());
    }

    @Test
    public void testOpenNonExistentFile() {
        // Write some test data to a single file that will be opened |n| times.
        Pair<Status, String> pair = env.getTestDirectory();
        assertTrue(pair.getKey().isOk());
        String testDir = pair.getValue();

        String nonExistentFile = testDir + "/non_existent_file";
        Pair<Status, Boolean> existPair = env.isFileExists(nonExistentFile);
        assertTrue(existPair.getKey().isOk());
        assertFalse(existPair.getValue());

        Pair<Status, RandomAccessFile> filePair = env.newRandomAccessFile(nonExistentFile);
        assertTrue(filePair.getKey().IsNotFound());

        Pair<Status, SequentialFile> filePair2 = env.newSequentialFile(nonExistentFile);
        assertTrue(filePair2.getKey().IsNotFound());
    }

    @Test
    public void testReopenWritableFile() {
        Pair<Status, String> pair = env.getTestDirectory();
        assertTrue(pair.getKey().isOk());
        String testDir = pair.getValue();

        String testFileName = testDir + "/reopen_writable_file.txt";
        env.delete(testFileName);

        Pair<Status, WritableFile> filePair = env.newWritableFile(testFileName);
        assertTrue(filePair.getKey().isOk());
        WritableFile writableFile = filePair.getValue();

        String data = "hello world!";
        assertTrue(writableFile.append(data).isOk());
        assertTrue(writableFile.close().isOk());

        filePair = env.newWritableFile(testFileName);
        assertTrue(filePair.getKey().isOk());
        writableFile = filePair.getValue();

        data = "42";
        assertTrue(writableFile.append(data).isOk());
        assertTrue(writableFile.close().isOk());

        Pair<Status, String> contentPair = Env.readFileToString(env, testFileName);
        assertTrue(contentPair.getKey().isOk());
        assertEquals("42", contentPair.getValue());
        env.delete(testFileName);
    }

    @Test
    public void testReopenAppendableFile() {
        Pair<Status, String> pair = env.getTestDirectory();
        assertTrue(pair.getKey().isOk());
        String testDir = pair.getValue();

        String testFileName = testDir + "/reopen_appendable_file.txt";
        env.delete(testFileName);

        Pair<Status, WritableFile> filePair = env.newAppendableFile(testFileName);
        assertTrue(filePair.getKey().isOk());
        WritableFile appendableFile = filePair.getValue();

        String data =  "hello world!";
        assertTrue(appendableFile.append(data).isOk());
        assertTrue(appendableFile.close().isOk());

        filePair = env.newAppendableFile(testFileName);
        assertTrue(filePair.getKey().isOk());
        appendableFile = filePair.getValue();

        data = "42";
        assertTrue(appendableFile.append(data).isOk());
        assertTrue(appendableFile.close().isOk());

        Pair<Status, String> contentPair = Env.readFileToString(env, testFileName);
        assertTrue(contentPair.getKey().isOk());
        assertEquals("hello world!42", contentPair.getValue());
        env.delete(testFileName);
    }

    @Test
    public void testGetFileSize() {
        Options options = new Options();
        String dbname = options.getEnv().getTestDirectory().getValue();
        String filename = dbname + "/" + TestUtils.randomString(5);

        Pair<Status, Boolean> tmp = getImpl().delete(filename);
        assertTrue(tmp.getKey().isOk());

        // not exist file
        Pair<Status, Long> tmp2 = getImpl().getFileSize(filename);
        assertTrue(tmp2.getKey().IsIOError());

        tmp = getImpl().isFileExists(filename);
        assertTrue(tmp.getKey().isOk());
        assertFalse(tmp.getValue());

        Pair<Status, WritableFile> pair = getImpl().newWritableFile(filename);
        Pair<Status, Long> tmp1 = getImpl().getFileSize(filename);
        assertTrue(tmp1.getKey().isOk());
        assertEquals(0L, tmp1.getValue().longValue());

        pair.getValue().append("aaa");
        pair.getValue().flush();
        pair.getValue().sync();
        pair.getValue().close();

        tmp1 = getImpl().getFileSize(filename);
        assertTrue(tmp1.getKey().isOk());
        assertEquals(3L, tmp1.getValue().longValue());
    }

    @Test
    public void testRenameFile() {
        Options options = new Options();
        String dbname = options.getEnv().getTestDirectory().getValue();
        String filename1 = dbname + "/" + TestUtils.randomString(5);
        String filename2 = filename1 + "1";

        assertTrue(getImpl().delete(filename1).getKey().isOk());
        assertTrue(getImpl().delete(filename2).getKey().isOk());

        assertTrue(getImpl().newWritableFile(filename1).getKey().isOk());

        Pair<Status, Boolean> pair = getImpl().isFileExists(filename1);
        assertTrue(pair.getKey().isOk());
        assertTrue(pair.getValue());

        pair = getImpl().isFileExists(filename2);
        assertTrue(pair.getKey().isOk());
        assertFalse(pair.getValue());

        assertTrue(getImpl().renameFile(filename1, filename2).isOk());

        pair = getImpl().isFileExists(filename1);
        assertTrue(pair.getKey().isOk());
        assertFalse(pair.getValue());

        pair = getImpl().isFileExists(filename2);
        assertTrue(pair.getKey().isOk());
        assertTrue(pair.getValue());
    }

    @Test
    public void testWriteStringToFileAndReadFileToString() {
        Options options = new Options();
        String dbname = options.getEnv().getTestDirectory().getValue();
        String filename = dbname + "/" + TestUtils.randomString(5);

        assertTrue(getImpl().delete(filename).getKey().isOk());

        String s = "abcdefg";
        Env.writeStringToFileSync(getImpl(), s, filename);

        Pair<Status, String> pair2 = Env.readFileToString(options.getEnv(), filename);
        assertTrue(pair2.getKey().isOk());
        assertEquals(s, pair2.getValue());

        String s2 = StringUtils.repeat(s, 10000);
        Env.writeStringToFileSync(getImpl(), s2, filename);

        pair2 = Env.readFileToString(options.getEnv(), filename);
        assertTrue(pair2.getKey().isOk());
        assertEquals(s2, pair2.getValue());
    }

    @Test
    public void testWriteStringToFileErrorCase() {
        Options options = new Options();
        String dbname = options.getEnv().getTestDirectory().getValue();
        String filename = dbname + "/" + TestUtils.randomString(5);

        String s = "abcdefg";
        Env env = getImpl();
        Env spyEnv = spy(env);
        doReturn(new Pair<>(Status.Corruption("force new writable file error"), null)).
                when(spyEnv).newWritableFile(anyString());
        Status status = Env.writeStringToFileSync(spyEnv, s, filename);
        assertTrue(status.IsCorruption());
        assertEquals("force new writable file error", status.getMessage());

        WritableFile writableFile = mock(WritableFile.class);
        doReturn(new Pair<>(Status.OK(), writableFile)).when(spyEnv).newWritableFile(anyString());

        // append
        when(writableFile.append(anyString())).thenReturn(Status.Corruption("force append error"));
        status = Env.writeStringToFileSync(spyEnv, s, filename);
        assertTrue(status.IsCorruption());
        assertEquals("force append error", status.getMessage());
        assertFalse(spyEnv.isFileExists(filename).getValue());

        // sync
        when(writableFile.append(anyString())).thenReturn(Status.OK());
        when(writableFile.sync()).thenReturn(Status.Corruption("force sync error"));
        status = Env.writeStringToFileSync(spyEnv, s, filename);
        assertTrue(status.IsCorruption());
        assertEquals("force sync error", status.getMessage());
        assertFalse(spyEnv.isFileExists(filename).getValue());

        // close
        when(writableFile.sync()).thenReturn(Status.OK());
        when(writableFile.close()).thenReturn(Status.Corruption("force close error"));
        status = Env.writeStringToFileSync(spyEnv, s, filename);
        assertTrue(status.IsCorruption());
        assertEquals("force close error", status.getMessage());
        assertFalse(spyEnv.isFileExists(filename).getValue());

        // delete
        doReturn(new Pair<>(Status.Corruption("force delete error"), null)).when(spyEnv).delete(anyString());
        status = Env.writeStringToFileSync(spyEnv, s, filename);
        assertTrue(status.IsCorruption());
        assertEquals("force close error", status.getMessage());
    }
}