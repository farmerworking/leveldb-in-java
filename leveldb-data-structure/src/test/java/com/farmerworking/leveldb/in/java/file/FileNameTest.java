package com.farmerworking.leveldb.in.java.file;

import com.farmerworking.leveldb.in.java.api.Options;
import com.farmerworking.leveldb.in.java.api.Status;
import javafx.util.Pair;
import org.junit.Test;

import static com.farmerworking.leveldb.in.java.file.FileType.*;
import static org.junit.Assert.*;

public class FileNameTest {
    @Test
    public void testFileNameTestConstruction() {
        String fname;

        fname = FileName.currentFileName("foo");
        assertEquals("foo/", fname.substring(0, 4));
        Pair<Long, FileType> pair = FileName.parseFileName(fname.substring(4));
        assertNotNull(pair);
        assertEquals(0, pair.getKey().longValue());
        assertEquals(kCurrentFile, pair.getValue());

        fname = FileName.lockFileName("foo");
        assertEquals("foo/", fname.substring(0, 4));
        pair = FileName.parseFileName(fname.substring(4));
        assertNotNull(pair);
        assertEquals(0, pair.getKey().longValue());
        assertEquals(kDBLockFile, pair.getValue());

        fname = FileName.logFileName("foo", 192);
        assertEquals("foo/", fname.substring(0, 4));
        pair = FileName.parseFileName(fname.substring(4));
        assertNotNull(pair);
        assertEquals(192, pair.getKey().longValue());
        assertEquals(kLogFile, pair.getValue());

        fname = FileName.tableFileName("bar", 200);
        assertEquals("bar/", fname.substring(0, 4));
        pair = FileName.parseFileName(fname.substring(4));
        assertNotNull(pair);
        assertEquals(200, pair.getKey().longValue());
        assertEquals(kTableFile, pair.getValue());

        fname = FileName.descriptorFileName("bar", 100);
        assertEquals("bar/", fname.substring(0, 4));
        pair = FileName.parseFileName(fname.substring(4));
        assertNotNull(pair);
        assertEquals(100, pair.getKey().longValue());
        assertEquals(kDescriptorFile, pair.getValue());

        fname = FileName.tempFileName("tmp", 999);
        assertEquals("tmp/", fname.substring(0, 4));
        pair = FileName.parseFileName(fname.substring(4));
        assertNotNull(pair);
        assertEquals(999, pair.getKey().longValue());
        assertEquals(kTempFile, pair.getValue());

        fname = FileName.infoLogFileName("foo");
        assertEquals("foo/", fname.substring(0, 4));
        pair = FileName.parseFileName(fname.substring(4));
        assertNotNull(pair);
        assertEquals(0, pair.getKey().longValue());
        assertEquals(kInfoLogFile, pair.getValue());

        fname = FileName.oldInfoLogFileName("foo");
        assertEquals("foo/", fname.substring(0, 4));
        pair = FileName.parseFileName(fname.substring(4));
        assertNotNull(pair);
        assertEquals(0, pair.getKey().longValue());
        assertEquals(kInfoLogFile, pair.getValue());
    }

    class TestObject {
        String fname;
        long number;
        FileType type;

        public TestObject(String fname, long number, FileType type) {
            this.fname = fname;
            this.number = number;
            this.type = type;
        }
    }

    @Test
    public void testFileNameParse() {
        // Successful parses
        TestObject[] cases = new TestObject[]{
                new TestObject( "100.log",            100,   kLogFile ),
                new TestObject( "0.log",              0,     kLogFile ),
                new TestObject( "0.sst",              0,     kTableFile ),
                new TestObject( "0.ldb",              0,     kTableFile ),
                new TestObject( "CURRENT",            0,     kCurrentFile ),
                new TestObject( "LOCK",               0,     kDBLockFile ),
                new TestObject( "MANIFEST-2",         2,     kDescriptorFile ),
                new TestObject( "MANIFEST-7",         7,     kDescriptorFile ),
                new TestObject( "LOG",                0,     kInfoLogFile ),
                new TestObject( "LOG.old",            0,     kInfoLogFile ),
                new TestObject( "18446744073709551615.log", Long.parseUnsignedLong("18446744073709551615"), kLogFile ),
        };
        for (int i = 0; i < cases.length; i++) {
            String f = cases[i].fname;
            Pair<Long, FileType> pair = FileName.parseFileName(f);
            assertNotNull(pair);
            assertEquals(cases[i].type, pair.getValue());
            assertEquals(cases[i].number, pair.getKey().longValue());
        }

        // Errors
        String[] errors = new String[]{
                "",
                "foo",
                "foo-dx-100.log",
                ".log",
                "",
                "manifest",
                "CURREN",
                "CURRENTX",
                "MANIFES",
                "MANIFEST",
                "MANIFEST-",
                "XMANIFEST-3",
                "MANIFEST-3x",
                "LOC",
                "LOCKx",
                "LO",
                "LOGx",
                "18446744073709551616.log",
                "184467440737095516150.log",
                "100",
                "100.",
                "100.lop"
        };

        for (int i = 0; i < errors.length; i++) {
            String f = errors[i];
            assertNull(FileName.parseFileName(f));
        }
    }

    @Test
    public void testSetCurrentFile() {
        Options options = new Options();
        String dbname = options.getEnv().getTestDirectory().getValue();

        String filename = FileName.currentFileName(dbname);

        Status status = FileName.setCurrentFile(options.getEnv(), dbname, 10L);
        assertTrue(status.isOk());

        assertEquals(String.format("MANIFEST-%d\n", 10L), Env.readFileToString(options.getEnv(), filename).getValue());

        status = FileName.setCurrentFile(options.getEnv(), dbname, 11L);
        assertTrue(status.isOk());
        assertEquals(String.format("MANIFEST-%d\n", 11L), Env.readFileToString(options.getEnv(), filename).getValue());
    }
}