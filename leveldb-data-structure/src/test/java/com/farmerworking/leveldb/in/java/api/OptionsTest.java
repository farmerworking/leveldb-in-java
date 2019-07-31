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
import static org.mockito.Mockito.mock;

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
        src.setEnv(mock(Env.class));
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