package com.farmerworking.leveldb.in.java.api;

import com.farmerworking.leveldb.in.java.data.structure.cache.ShardedLRUCache;
import com.farmerworking.leveldb.in.java.data.structure.filter.BloomFilterPolicy;
import com.farmerworking.leveldb.in.java.data.structure.harness.ReverseKeyComparator;
import com.farmerworking.leveldb.in.java.file.Env;
import com.farmerworking.leveldb.in.java.file.RandomAccessFile;
import com.farmerworking.leveldb.in.java.file.SequentialFile;
import com.farmerworking.leveldb.in.java.file.WritableFile;
import com.farmerworking.leveldb.in.java.file.impl.LogImpl;
import javafx.util.Pair;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.channels.FileLock;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.*;

public class OptionsTest {
    @Test
    public void testCopy() throws IllegalAccessException {
        Options src = new Options();

        // set
        src.setBlockRestartInterval(100);
        src.setComparator(new ReverseKeyComparator());
        src.setFilterPolicy(new BloomFilterPolicy(1024));
        src.setBlockSize(1024);
        src.setCompression(CompressionType.kNoCompression);
        src.setParanoidChecks(true);
        src.setBlockCache(new ShardedLRUCache(1024));
        src.setEnv(new Env() {
            @Override
            public Pair<Status, WritableFile> newWritableFile(String filename) {
                return null;
            }

            @Override
            public Pair<Status, WritableFile> newAppendableFile(String filename) {
                return null;
            }

            @Override
            public Pair<Status, RandomAccessFile> newRandomAccessFile(String filename) {
                return null;
            }

            @Override
            public Pair<Status, SequentialFile> newSequentialFile(String filename) {
                return null;
            }

            @Override
            public Pair<Status, String> getTestDirectory() {
                return null;
            }

            @Override
            public Pair<Status, Boolean> delete(String filename) {
                return null;
            }

            @Override
            public boolean isFileExists(String filename){
                return false;
            }

            @Override
            public Pair<Status, Long> getFileSize(String filename) {
                return null;
            }

            @Override
            public Status renameFile(String from, String to) {
                return null;
            }

            @Override
            public Status createDir(String name) {
                return null;
            }

            @Override
            public Pair<Status, Options.Logger> newLogger(String logFileName) {
                return null;
            }

            @Override
            public Pair<Status, List<String>> getChildren(String dbname) {
                return null;
            }

            @Override
            public Pair<Status, FileLock> lockFile(String lockFileName) {
                return null;
            }
        });
        src.setMaxFileSize(10);
        src.setInfoLog(new LogImpl("/tmp/abc"));
        src.setReuseLogs(true);
        src.setWriteBufferSize(888);
        src.setMaxOpenFiles(999);


        Options dst = new Options(src);
        for(Field field : Options.class.getDeclaredFields()) {
            field.setAccessible(true);
            assertEquals(field.getName(), field.get(src), field.get(dst));
        }
    }
}