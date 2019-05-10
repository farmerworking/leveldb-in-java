package com.farmerworking.leveldb.in.java.file;

import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.common.TestUtils;
import javafx.util.Pair;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.*;

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

        Pair<Status, String> contentPair = readFileToString(env, testFileName);
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

        Pair<Status, String> contentPair = readFileToString(env, testFileName);
        assertTrue(contentPair.getKey().isOk());
        assertEquals("hello world!42", contentPair.getValue());
        env.delete(testFileName);
    }

    private Pair<Status, String> readFileToString(Env env, String fname) {
        StringBuilder builder = new StringBuilder();
        Pair<Status, SequentialFile> pair = env.newSequentialFile(fname);
        if (pair.getKey().isNotOk()) {
            return new Pair<>(pair.getKey(), null);
        }

        SequentialFile file = pair.getValue();
        int kBufferSize = 8192;

        Status status;
        while (true) {
            Pair<Status, String> readPair = file.read(kBufferSize);
            status = readPair.getKey();

            if (status.isNotOk()) {
                break;
            }
            builder.append(readPair.getValue());
            if (readPair.getValue().isEmpty()) {
                break;
            }
        }
        return new Pair<>(status, builder.toString());
    }
}