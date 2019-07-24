package com.farmerworking.leveldb.in.java.data.structure.db;

import com.farmerworking.leveldb.in.java.api.BytewiseComparator;
import com.farmerworking.leveldb.in.java.api.Options;
import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKeyComparator;
import org.junit.Test;

import static org.junit.Assert.*;

public class DbImplTest {
    @Test(expected = AssertionError.class)
    public void testClipToRangeAssertionError() {
        DbImpl.clipToRange(null, "", 1, 0);
    }

    @Test
    public void testClipToRangeNoFieldException() {
        Options options = new Options();
        try {
            DbImpl.clipToRange(options, "whateverFiled", 0, 1);
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
        DbImpl.clipToRange(options, "maxOpenFiles", 2000, 3000);
        assertEquals(2000, options.getMaxOpenFiles());

        options = new Options();
        DbImpl.clipToRange(options, "maxOpenFiles", 100, 500);
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

        Options options = DbImpl.sanitizeOptions(dbname,
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
}