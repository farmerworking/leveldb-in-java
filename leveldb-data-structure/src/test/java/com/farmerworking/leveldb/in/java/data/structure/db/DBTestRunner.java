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
}
