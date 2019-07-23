package com.farmerworking.leveldb.in.java.data.structure.db;

import com.farmerworking.leveldb.in.java.api.Options;
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
}