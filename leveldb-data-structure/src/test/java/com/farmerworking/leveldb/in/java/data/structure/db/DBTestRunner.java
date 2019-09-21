package com.farmerworking.leveldb.in.java.data.structure.db;

import com.farmerworking.leveldb.in.java.api.Iterator;
import com.farmerworking.leveldb.in.java.api.Options;
import com.farmerworking.leveldb.in.java.api.ReadOptions;
import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.api.WriteOptions;
import javafx.util.Pair;
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
    public void testGetMemUsage() {
        do {
            assertTrue(dbTest.put("foo", "v1").isOk());
            Pair<Boolean, String> pair = dbTest.db.getProperty("leveldb.approximate-memory-usage");
            assertTrue(pair.getKey());
            Integer usage = Integer.valueOf(pair.getValue());
            assertTrue(usage > 0);
            assertTrue(usage < 5 * 1024 * 1024);
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

    @Test
    public void testGetEncountersEmptyLevel() {
        do {
            // Arrange for the following to happen:
            //   * sstable A in level 0
            //   * nothing in level 1
            //   * sstable B in level 2
            // Then do enough Get() calls to arrange for an automatic compaction
            // of sstable A.  A bug would cause the compaction to be marked as
            // occurring at level 1 (instead of the correct level 0).

            // Step 1: First place sstables in levels 0 and 2
            int compactionCount = 0;
            while(dbTest.numTableFilesAtLevel(0) == 0 || dbTest.numTableFilesAtLevel(2) == 0) {
                assertTrue("could not fill levels 0 and 2", compactionCount < 100);
                compactionCount ++;
                dbTest.put("a", "begin");
                dbTest.put("z", "end");
                ((DBImpl) dbTest.db).TEST_compactMemtable();
            }

            // Step 2: clear level 1 if necessary.
            dbTest.db.TEST_compactRange(1, null, null);
            assertEquals(dbTest.numTableFilesAtLevel(0), 1);
            assertEquals(dbTest.numTableFilesAtLevel(1), 0);
            assertEquals(dbTest.numTableFilesAtLevel(2), 1);

            // Step 3: read a bunch of times
            for (int i = 0; i < 1000; i++) {
                assertEquals("NOT_FOUND", dbTest.get("missing"));
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }

            assertEquals(dbTest.numTableFilesAtLevel(0), 0);
        } while (dbTest.changeOptions());
    }

    @Test
    public void testIterEmpty() {
        Iterator<String, String> iter = dbTest.db.iterator(new ReadOptions());

        iter.seekToFirst();
        assertEquals(dbTest.iterStatus(iter), "(invalid)");

        iter.seekToLast();
        assertEquals(dbTest.iterStatus(iter), "(invalid)");

        iter.seek("foo");
        assertEquals(dbTest.iterStatus(iter), "(invalid)");
    }

    @Test
    public void testIterSingle() {
        assertTrue(dbTest.put("a", "va").isOk());
        Iterator<String, String> iter = dbTest.db.iterator(new ReadOptions());

        iter.seekToFirst();
        assertEquals(dbTest.iterStatus(iter), "a->va");
        iter.next();
        assertEquals(dbTest.iterStatus(iter), "(invalid)");
        iter.seekToFirst();
        assertEquals(dbTest.iterStatus(iter), "a->va");
        iter.prev();
        assertEquals(dbTest.iterStatus(iter), "(invalid)");

        iter.seekToLast();
        assertEquals(dbTest.iterStatus(iter), "a->va");
        iter.next();
        assertEquals(dbTest.iterStatus(iter), "(invalid)");
        iter.seekToLast();
        assertEquals(dbTest.iterStatus(iter), "a->va");
        iter.prev();
        assertEquals(dbTest.iterStatus(iter), "(invalid)");

        iter.seek("");
        assertEquals(dbTest.iterStatus(iter), "a->va");
        iter.next();
        assertEquals(dbTest.iterStatus(iter), "(invalid)");

        iter.seek("a");
        assertEquals(dbTest.iterStatus(iter), "a->va");
        iter.next();
        assertEquals(dbTest.iterStatus(iter), "(invalid)");

        iter.seek("b");
        assertEquals(dbTest.iterStatus(iter), "(invalid)");
    }

    @Test
    public void testIterMulti() {
        assertTrue(dbTest.put("a", "va").isOk());
        assertTrue(dbTest.put("b", "vb").isOk());
        assertTrue(dbTest.put("c", "vc").isOk());
        Iterator<String, String> iter = dbTest.db.iterator(new ReadOptions());

        iter.seekToFirst();
        assertEquals(dbTest.iterStatus(iter), "a->va");
        iter.next();
        assertEquals(dbTest.iterStatus(iter), "b->vb");
        iter.next();
        assertEquals(dbTest.iterStatus(iter), "c->vc");
        iter.next();
        assertEquals(dbTest.iterStatus(iter), "(invalid)");
        iter.seekToFirst();
        assertEquals(dbTest.iterStatus(iter), "a->va");
        iter.prev();
        assertEquals(dbTest.iterStatus(iter), "(invalid)");

        iter.seekToLast();
        assertEquals(dbTest.iterStatus(iter), "c->vc");
        iter.prev();
        assertEquals(dbTest.iterStatus(iter), "b->vb");
        iter.prev();
        assertEquals(dbTest.iterStatus(iter), "a->va");
        iter.prev();
        assertEquals(dbTest.iterStatus(iter), "(invalid)");
        iter.seekToLast();
        assertEquals(dbTest.iterStatus(iter), "c->vc");
        iter.next();
        assertEquals(dbTest.iterStatus(iter), "(invalid)");

        iter.seek("");
        assertEquals(dbTest.iterStatus(iter), "a->va");
        iter.seek("a");
        assertEquals(dbTest.iterStatus(iter), "a->va");
        iter.seek("ax");
        assertEquals(dbTest.iterStatus(iter), "b->vb");
        iter.seek("b");
        assertEquals(dbTest.iterStatus(iter), "b->vb");
        iter.seek("z");
        assertEquals(dbTest.iterStatus(iter), "(invalid)");

        // Switch from reverse to forward
        iter.seekToLast();
        iter.prev();
        iter.prev();
        iter.next();
        assertEquals(dbTest.iterStatus(iter), "b->vb");

        // Switch from forward to reverse
        iter.seekToFirst();
        iter.next();
        iter.next();
        iter.prev();
        assertEquals(dbTest.iterStatus(iter), "b->vb");

        // Make sure iter stays at snapshot
        assertTrue(dbTest.put("a", "va2").isOk());
        assertTrue(dbTest.put("a2", "va3").isOk());
        assertTrue(dbTest.put("b", "vb2").isOk());
        assertTrue(dbTest.put("c", "vc2").isOk());
        assertTrue(dbTest.delete("b").isOk());
        iter.seekToFirst();
        assertEquals(dbTest.iterStatus(iter), "a->va");
        iter.next();
        assertEquals(dbTest.iterStatus(iter), "b->vb");
        iter.next();
        assertEquals(dbTest.iterStatus(iter), "c->vc");
        iter.next();
        assertEquals(dbTest.iterStatus(iter), "(invalid)");
        iter.seekToLast();
        assertEquals(dbTest.iterStatus(iter), "c->vc");
        iter.prev();
        assertEquals(dbTest.iterStatus(iter), "b->vb");
        iter.prev();
        assertEquals(dbTest.iterStatus(iter), "a->va");
        iter.prev();
        assertEquals(dbTest.iterStatus(iter), "(invalid)");
    }

    @Test
    public void iterSmallAndLargeMix() {
        assertTrue(dbTest.put("a", "va").isOk());
        assertTrue(dbTest.put("b", StringUtils.repeat('b', 100000)).isOk());
        assertTrue(dbTest.put("c", "vc").isOk());
        assertTrue(dbTest.put("d", StringUtils.repeat('d', 100000)).isOk());
        assertTrue(dbTest.put("e", StringUtils.repeat('e', 100000)).isOk());

        Iterator<String, String> iter = dbTest.db.iterator(new ReadOptions());

        iter.seekToFirst();
        assertEquals(dbTest.iterStatus(iter), "a->va");
        iter.next();
        assertEquals(dbTest.iterStatus(iter), "b->" + StringUtils.repeat('b', 100000));
        iter.next();
        assertEquals(dbTest.iterStatus(iter), "c->vc");
        iter.next();
        assertEquals(dbTest.iterStatus(iter), "d->" + StringUtils.repeat('d', 100000));
        iter.next();
        assertEquals(dbTest.iterStatus(iter), "e->" + StringUtils.repeat('e', 100000));
        iter.next();
        assertEquals(dbTest.iterStatus(iter), "(invalid)");

        iter.seekToLast();
        assertEquals(dbTest.iterStatus(iter), "e->" + StringUtils.repeat('e', 100000));
        iter.prev();
        assertEquals(dbTest.iterStatus(iter), "d->" + StringUtils.repeat('d', 100000));
        iter.prev();
        assertEquals(dbTest.iterStatus(iter), "c->vc");
        iter.prev();
        assertEquals(dbTest.iterStatus(iter), "b->" + StringUtils.repeat('b', 100000));
        iter.prev();
        assertEquals(dbTest.iterStatus(iter), "a->va");
        iter.prev();
        assertEquals(dbTest.iterStatus(iter), "(invalid)");
    }

    @Test
    public void testIterMultiWithDelete() {
        do {
            assertTrue(dbTest.put("a", "va").isOk());
            assertTrue(dbTest.put("b", "vb").isOk());
            assertTrue(dbTest.put("c", "vc").isOk());
            assertTrue(dbTest.delete("b").isOk());
            assertEquals("NOT_FOUND", dbTest.get("b"));

            Iterator<String, String> iter = dbTest.db.iterator(new ReadOptions());
            iter.seek("c");
            assertEquals(dbTest.iterStatus(iter), "c->vc");
            iter.prev();
            assertEquals(dbTest.iterStatus(iter), "a->va");
        } while (dbTest.changeOptions());
    }

    @Test
    public void testRecover() {
        do {
            assertTrue(dbTest.put("foo", "v1").isOk());
            assertTrue(dbTest.put("baz", "v5").isOk());

            dbTest.reopen();
            assertEquals("v1", dbTest.get("foo"));
            assertEquals("v5", dbTest.get("baz"));

            assertTrue(dbTest.put("bar", "v2").isOk());
            assertTrue(dbTest.put("foo", "v3").isOk());

            dbTest.reopen();
            assertEquals("v3", dbTest.get("foo"));
            assertTrue(dbTest.put("foo", "v4").isOk());
            assertEquals("v4", dbTest.get("foo"));
            assertEquals("v2", dbTest.get("bar"));
            assertEquals("v5", dbTest.get("baz"));
        } while (dbTest.changeOptions());
    }

    @Test
    public void testRecoveryWithEmptyLog() {
        do {
            assertTrue(dbTest.put("foo", "v1").isOk());
            assertTrue(dbTest.put("foo", "v2").isOk());
            dbTest.reopen();
            dbTest.reopen(); // no log
            assertTrue(dbTest.put("foo", "v3").isOk());
            dbTest.reopen();
            assertEquals("v3", dbTest.get("foo"));
        } while (dbTest.changeOptions());
    }

    // Check that writes done during a memtable compaction are recovered
    // if the database is shutdown during the memtable compaction.
    @Test
    public void testRecoverDuringMemtableCompaction() {
        do {
            Options options = dbTest.currentOptions();
            options.setEnv(dbTest.env);
            options.setWriteBufferSize(1000000);
            dbTest.reopen(options);

            // Trigger a long memtable compaction and reopen the database during it
            assertTrue(dbTest.put("foo", "v1").isOk()); // Goes to 1st log file
            assertTrue(dbTest.put("big1", StringUtils.repeat('x', 10000000)).isOk()); // Fills memtable
            assertTrue(dbTest.put("big2", StringUtils.repeat('y', 1000)).isOk()); // Triggers compaction
            assertTrue(dbTest.put("bar", "v2").isOk()); // Goes to new log file

            dbTest.reopen(options);
            assertEquals("v1", dbTest.get("foo"));
            assertEquals("v2", dbTest.get("bar"));
            assertEquals(StringUtils.repeat('x', 10000000), dbTest.get("big1"));
            assertEquals(StringUtils.repeat('y', 1000), dbTest.get("big2"));
        } while (dbTest.changeOptions());

    }
}
