package com.farmerworking.leveldb.in.java.data.structure.db;

import com.farmerworking.leveldb.in.java.api.Options;
import com.farmerworking.leveldb.in.java.api.WriteOptions;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class DBTestRunner {
    private DBTest dbTest;

    @Before
    public void setUp() throws Exception {
        dbTest = new DBTest();
    }

    @After
    public void tearDown() throws Exception {
        dbTest.destroy();
    }

    @Test
    public void testEmpty() {
        do {
            assertNotNull(dbTest.db);
            assertEquals("NOT_FOUND", dbTest.get("foo"));
        } while (dbTest.changeOptions());
    }

    @Test
    public void testReadWrite() {
        do {
            assertTrue(dbTest.put("foo", "v1").isOk());
            assertEquals("v1", dbTest.get("foo"));
            assertTrue(dbTest.put("bar", "v2").isOk());
            assertTrue(dbTest.put("foo", "v3").isOk());
            assertEquals("v3", dbTest.get("foo"));
            assertEquals("v2", dbTest.get("bar"));
        } while(dbTest.changeOptions());
    }

    @Test
    public void testPutDeleteGet() {
        do {
            assertTrue(dbTest.put("foo", "v1").isOk());
            assertEquals("v1", dbTest.get("foo"));
            assertTrue(dbTest.put("foo", "v2").isOk());
            assertEquals("v2", dbTest.get("foo"));
            assertTrue(dbTest.db.delete(new WriteOptions(), "foo").isOk());
            assertEquals("NOT_FOUND", dbTest.get("bar"));
        } while(dbTest.changeOptions());
    }

    @Test
    public void testGetFromImmutableLayer() {
        do {
            Options options = dbTest.currentOptions();
            options.setEnv(dbTest.env);
            options.setWriteBufferSize(100000);
            dbTest.reopen(options);

            assertTrue(dbTest.put("foo", "v1").isOk());
            assertEquals("v1", dbTest.get("foo"));

            dbTest.env.delayDataSync.set(true);
            dbTest.put("k1", StringUtils.repeat('x', 100000)); // fill memtable
            dbTest.put("k2", StringUtils.repeat('y', 100000)); // trigger compaction
            assertEquals("v1", dbTest.get("foo"));
            dbTest.env.delayDataSync.set(false);
        } while(dbTest.changeOptions());
    }

    @Test
    public void testGetFromVersions() {
        do {
            assertTrue(dbTest.put("foo", "v1").isOk());
            ((DBImpl)dbTest.db).TEST_compactMemtable();
            assertEquals("v1", dbTest.get("foo"));
        } while (dbTest.changeOptions());
    }

    @Test
    public void testGetSnapshot() {
        do {
            // Try with both a short key and a long key
            for (int i = 0; i < 2; i++) {
                String key = i == 0 ? "foo" : StringUtils.repeat('x', 200);
                assertTrue(dbTest.put(key, "v1").isOk());
                long snapshot = dbTest.db.getSnapshot();
                assertTrue(dbTest.put(key, "v2").isOk());
                assertEquals("v2", dbTest.get(key));
                assertEquals("v1", dbTest.get(key, snapshot));
                ((DBImpl) dbTest.db).TEST_compactMemtable();
                assertEquals("v2", dbTest.get(key));
                assertEquals("v1", dbTest.get(key, snapshot));
                ((DBImpl)dbTest.db).releaseSnapshot(snapshot);
            }
        } while (dbTest.changeOptions());
    }

    @Test
    public void testGetLevel0Ordering() {
        do {
            // Check that we process level-0 files in correct order.  The code
            // below generates two level-0 files where the earlier one comes
            // before the later one in the level-0 file list since the earlier
            // one has a smaller "smallest" key.
            assertTrue(dbTest.put("bar", "b").isOk());
            assertTrue(dbTest.put("foo", "v1").isOk());
            ((DBImpl) dbTest.db).TEST_compactMemtable();
            assertTrue(dbTest.put("foo", "v2").isOk());
            ((DBImpl) dbTest.db).TEST_compactMemtable();
            assertEquals("v2", dbTest.get("foo"));
        } while (dbTest.changeOptions());
    }

    @Test
    public void testGetOrderedByLevels() {
        do {
            assertTrue(dbTest.put("foo", "v1").isOk());
            dbTest.db.compactRange("a", "z");
            assertEquals("v1", dbTest.get("foo"));
            assertTrue(dbTest.put("foo", "v2").isOk());
            assertEquals("v2", dbTest.get("foo"));
            ((DBImpl) dbTest.db).TEST_compactMemtable();
            assertEquals("v2", dbTest.get("foo"));
        } while (dbTest.changeOptions());
    }

    @Test
    public void testPicksCorrectFile() {
        do {
            assertTrue(dbTest.put("a", "va").isOk());
            dbTest.db.compactRange("a", "b");
            assertTrue(dbTest.put("x", "vx").isOk());
            dbTest.db.compactRange("x", "y");
            assertTrue(dbTest.put("f", "vf").isOk());
            dbTest.db.compactRange("f", "g");
            assertEquals("va", dbTest.get("a"));
            assertEquals("vf", dbTest.get("f"));
            assertEquals("vx", dbTest.get("x"));
        } while (dbTest.changeOptions());
    }
}
