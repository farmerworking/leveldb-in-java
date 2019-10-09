package com.farmerworking.leveldb.in.java.data.structure.db;

import java.util.List;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;

import com.farmerworking.leveldb.in.java.api.BytewiseComparator;
import com.farmerworking.leveldb.in.java.api.Comparator;
import com.farmerworking.leveldb.in.java.api.CompressionType;
import com.farmerworking.leveldb.in.java.api.Iterator;
import com.farmerworking.leveldb.in.java.api.Options;
import com.farmerworking.leveldb.in.java.api.ReadOptions;
import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.api.WriteOptions;
import com.farmerworking.leveldb.in.java.common.TestUtils;
import com.farmerworking.leveldb.in.java.data.structure.cache.ShardedLRUCache;
import com.farmerworking.leveldb.in.java.data.structure.filter.BloomFilterPolicy;
import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKey;
import com.farmerworking.leveldb.in.java.data.structure.memory.ParsedInternalKey;
import com.farmerworking.leveldb.in.java.data.structure.memory.ValueType;
import com.farmerworking.leveldb.in.java.data.structure.version.Config;
import com.google.common.collect.Lists;
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

    private String key(int i) {
        String s = String.valueOf(i);
        if (s.length() < 6) {
            s = StringUtils.repeat('0', 6 - s.length()) + s;
        } else if (s.length() > 6) {
            s.substring(s.length() - 6, s.length());
        }

        return "key" + s;
    }

    private int totalTableFiles() {
        int result = 0;
        for (int i = 0; i < Config.kNumLevels; i++) {
            result += dbTest.db.numLevelFiles(i);
        }
        return result;
    }

    @Test
    public void testMinorCompactionsHappen() {
        Options options = dbTest.currentOptions();
        options.setWriteBufferSize(10000);
        dbTest.reopen(options);

        int N = 500;
        int startTablesNum = totalTableFiles();
        for (int i = 0; i < N; i++) {
            assertTrue(dbTest.put(key(i), key(i) + StringUtils.repeat('v', 1000)).isOk());
        }
        int endTablesNum = totalTableFiles();
        assertTrue(endTablesNum > startTablesNum);

        for (int i = 0; i < N; i++) {
            assertEquals(key(i) + StringUtils.repeat('v', 1000), dbTest.get(key(i)));
        }

        dbTest.reopen();

        for (int i = 0; i < N; i++) {
            assertEquals(key(i) + StringUtils.repeat('v', 1000), dbTest.get(key(i)));
        }
    }

    @Test
    public void testRecoverWithLargeLog() {
        Options options = dbTest.currentOptions();
        dbTest.reopen(options);
        assertTrue(dbTest.put("big1", StringUtils.repeat('1', 200000)).isOk());
        assertTrue(dbTest.put("big2", StringUtils.repeat('2', 200000)).isOk());
        assertTrue(dbTest.put("small3", StringUtils.repeat('3', 10)).isOk());
        assertTrue(dbTest.put("small4", StringUtils.repeat('4', 10)).isOk());
        assertEquals(dbTest.db.numLevelFiles(0), 0);

        // Make sure that if we re-open with a small write buffer size that
        // we flush table files in the middle of a large log file.
        Options options1 = dbTest.currentOptions();
        options1.setWriteBufferSize(100000);
        dbTest.reopen(options1);

        assertEquals(dbTest.get("big1"), StringUtils.repeat('1', 200000));
        assertEquals(dbTest.get("big2"), StringUtils.repeat('2', 200000));
        assertEquals(dbTest.get("small3"), StringUtils.repeat('3', 10));
        assertEquals(dbTest.get("small4"), StringUtils.repeat('4', 10));
        assertEquals(dbTest.db.numLevelFiles(0), 3);
    }

    @Test
    public void testCompactionGenerateMultipleFiles() {
        Options options = dbTest.currentOptions();
        options.setWriteBufferSize(100000000);
        dbTest.reopen(options);

        Random random = new Random();

        // Write 8MB (80 values, each 100K)
        assertEquals(dbTest.db.numLevelFiles(0), 0);
        Vector<String> values = new Vector<>();
        for (int i = 0; i < 80; i++) {
            values.add(TestUtils.randomString(100000));
            assertTrue(dbTest.put(key(i), values.get(i)).isOk());
        }

        // Reopening moves updates to level-0
        dbTest.reopen(options);
        dbTest.db.TEST_compactRange(0, null, null);

        assertEquals(dbTest.db.numLevelFiles(0), 0);
        assertTrue(dbTest.db.numLevelFiles(1) > 1);

        for (int i = 0; i < 80; i++) {
            assertEquals(dbTest.get(key(i)), values.get(i));
        }
    }

    @Test
    public void testRepeatedWritesToSameKey() {
        Options options = dbTest.currentOptions();
        options.setEnv(dbTest.env);
        options.setWriteBufferSize(100000); // Small write buffer
        dbTest.reopen(options);

        // We must have at most one file per level except for level-0,
        // which may have up to kL0_StopWritesTrigger files.
        int maxFiles = Config.kNumLevels + Config.kL0_StopWritesTrigger;
        String value = TestUtils.randomString(2 * options.getWriteBufferSize());
        for (int i = 0; i < 5 * maxFiles; i++) {
            assertTrue(dbTest.put("key", value).isOk());
            assertTrue(totalTableFiles() < maxFiles);
            System.out.println(String.format("after %d: %d files", i + 1, totalTableFiles()));
        }
    }

    private void makeTables(int n, String smallest, String largest) {
        for (int i = 0; i < n; i++) {
            dbTest.put(smallest, "begin");
            dbTest.put(largest, "end");
            dbTest.db.TEST_compactMemtable();
        }
    }

    // Prevent pushing of new sstables into deeper levels by addin
    // tables that cover a specified range to all levels
    private void fillLevels(String smallest, String largest) {
        makeTables(Config.kNumLevels, smallest, largest);
    }

    @Test
    public void testSparseMerge() {
        Options options = dbTest.currentOptions();
        options.setCompression(CompressionType.kNoCompression);
        dbTest.reopen(options);

        fillLevels("A", "Z");
        
        // Suppose there is:
        //    small amount of data with prefix A
        //    large amount of data with prefix B
        //    small amount of data with prefix C
        // and that recent updates have made small changes to all three prefixes.
        // Check that we do not do a compaction that merges all of B in one shot.
        String value = StringUtils.repeat('x', 1000);
        dbTest.put("A", "va");
        for (int i = 0; i < 100000; i++) {
            dbTest.put("B" + i, value);
        }
        dbTest.put("C", "vc");
        dbTest.db.TEST_compactMemtable();
        dbTest.db.TEST_compactRange(0, null, null);

        // Make sparse update
        dbTest.put("A",    "va2");
        dbTest.put("B100", "bvalue2");
        dbTest.put("C",    "vc2");
        dbTest.db.TEST_compactMemtable();

        // Compactions should not cause us to create a situation where
        // a file overlaps too much data at the next level.
        assertTrue(dbTest.db.TEST_maxNextLevelOverlappingBytes() <= 20*1048576);
        dbTest.db.TEST_compactRange(0, null, null);
        assertTrue(dbTest.db.TEST_maxNextLevelOverlappingBytes() <= 20*1048576);
        dbTest.db.TEST_compactRange(1, null, null);
        assertTrue(dbTest.db.TEST_maxNextLevelOverlappingBytes() <= 20*1048576);
    }

    private boolean between(long value, long low, long high) {
        boolean result = (value >= low) && (value <= high);
        if (!result) {
            System.out.println(String.format("value %d is not in range (%d, %d)", value, low, high));
        }
        return result;
    }

    private long size(String start, String limit) {
        List<Pair<String, String>> range = Lists.newArrayList(new Pair<>(start, limit));
        return dbTest.db.getApproximateSizes(range, 1).get(0);
    }

    @Test
    public void testApproximateSizes() {
        do {
            Options options = dbTest.currentOptions();
            options.setWriteBufferSize(100000000); // large write buffer
            options.setCompression(CompressionType.kNoCompression);
            dbTest.destroyAndReopon();

            assertTrue(between(size("", "xyz"), 0, 0));
            dbTest.reopen(options);
            assertTrue(between(size("", "xyz"), 0, 0));

            // Write 8MB (80 values, each 100K)
            assertEquals(dbTest.numTableFilesAtLevel(0), 0);
            int N = 80;
            int S1 = 100000;
            int S2 = 105000;
            for (int i = 0; i < N; i++) {
                assertTrue(dbTest.put(key(i), TestUtils.randomString(S1)).isOk());
            }

            // 0 because GetApproximateSizes() does not account for memtable space
            assertTrue(between(size("", key(50)), 0, 0));

            if (options.isReuseLogs()) {
                // Recovery will reuse memtable, and GetApproximateSizes() does not
                // account for memtable usage;
                dbTest.reopen(options);
                assertTrue(between(size("", key(50)), 0, 0));
                continue;
            }

            // Check sizes across recovery by reopening a few times
            for (int run = 0; run < 3; run++) {
                dbTest.reopen(options);

                for (int compactStart = 0; compactStart < N; compactStart += 10){
                    for (int i = 0; i < N; i += 10) {
                        assertTrue(between(size("", key(i)), S1*i, S2*i));
                        assertTrue(between(size("", key(i) + ".suffix"), S1*(i+1), S2*(i+1)));
                        assertTrue(between(size(key(i), key(i+10)), S1*10, S2*10));
                    }
                    assertTrue(between(size("", key(50)), S1*50, S2*50));
                    assertTrue(between(size("", key(50) + ".suffix"), S1*50, S2*50));

                    String start = key(compactStart);
                    String end = key(compactStart + 9);
                    dbTest.db.TEST_compactRange(0, start, end);
                }

                assertEquals(dbTest.numTableFilesAtLevel(0), 0);
                assertTrue(dbTest.numTableFilesAtLevel(1) > 0);
            }
        } while (dbTest.changeOptions());
    }

    @Test
    public void testApproximateSizes_MixOfSmallAndLarge() {
        do {
            Options options = dbTest.currentOptions();
            options.setCompression(CompressionType.kNoCompression);
            dbTest.reopen();

            String big1 = TestUtils.randomString(100000);
            assertTrue(dbTest.put(key(0), TestUtils.randomString(10000)).isOk());
            assertTrue(dbTest.put(key(1), TestUtils.randomString(10000)).isOk());
            assertTrue(dbTest.put(key(2), big1).isOk());
            assertTrue(dbTest.put(key(3), TestUtils.randomString(10000)).isOk());
            assertTrue(dbTest.put(key(4), big1).isOk());
            assertTrue(dbTest.put(key(5), TestUtils.randomString(10000)).isOk());
            assertTrue(dbTest.put(key(6), TestUtils.randomString(300000)).isOk());
            assertTrue(dbTest.put(key(7), TestUtils.randomString(10000)).isOk());

            if (options.isReuseLogs()) {
                // Need to force a memtable compaction since recovery does not do so.
                assertTrue(dbTest.db.TEST_compactMemtable().isOk());
            }

            // Check sizes across recovery by reopening a few times
            for (int run = 0; run < 3; run++) {
                dbTest.reopen(options);
                assertTrue(between(size("", key(0)),0, 0));
                assertTrue(between(size("", key(1)),10000, 11000));
                assertTrue(between(size("", key(2)),20000, 21000));
                assertTrue(between(size("", key(3)),120000, 121000));
                assertTrue(between(size("", key(4)),130000, 131000));
                assertTrue(between(size("", key(5)),230000, 231000));
                assertTrue(between(size("", key(6)),240000, 241000));
                assertTrue(between(size("", key(7)),540000, 541000));
                assertTrue(between(size("", key(8)),550000, 560000));

                assertTrue(between(size(key(3), key(5)),110000, 111000));
                dbTest.db.TEST_compactRange(0, null, null);
            }
        } while (dbTest.changeOptions());
    }

    @Test
    public void testIteratorPinsRef() {
        dbTest.put("foo", "hello");

        // Get iterator that will yield the current contents of the DB.
        Iterator<String, String> iter = dbTest.db.iterator(new ReadOptions());

        // Write to force compactions
        dbTest.put("foo", "newvalue1");
        for (int i = 0; i < 100; i++) {
            assertTrue(dbTest.put(key(i), key(i) + StringUtils.repeat('v', 100000)).isOk()); // 100K values
        }
        dbTest.put("foo", "newvalue2");

        iter.seekToFirst();
        assertTrue(iter.valid());
        assertEquals("foo", iter.key());
        assertEquals("hello", iter.value());
        iter.next();
        assertFalse(iter.valid());
    }

    @Test
    public void testSnapshot() {
        do {
            dbTest.put("foo", "v1");
            long s1 = dbTest.db.getSnapshot();
            dbTest.put("foo", "v2");
            long s2 = dbTest.db.getSnapshot();
            dbTest.put("foo", "v3");
            long s3 = dbTest.db.getSnapshot();

            dbTest.put("foo", "v4");
            assertEquals("v1", dbTest.get("foo", s1));
            assertEquals("v2", dbTest.get("foo", s2));
            assertEquals("v3", dbTest.get("foo", s3));
            assertEquals("v4", dbTest.get("foo"));

            dbTest.db.releaseSnapshot(s3);
            assertEquals("v1", dbTest.get("foo", s1));
            assertEquals("v2", dbTest.get("foo", s2));
            assertEquals("v4", dbTest.get("foo"));

            dbTest.db.releaseSnapshot(s1);
            assertEquals("v2", dbTest.get("foo", s2));
            assertEquals("v4", dbTest.get("foo"));

            dbTest.db.releaseSnapshot(s2);
            assertEquals("v4", dbTest.get("foo"));
        } while (dbTest.changeOptions());
    }

    private String allEntriesFor(String userKey) {
        Iterator<String, String> iter = dbTest.db.TEST_newInternalIterator();
        InternalKey target = new InternalKey(userKey, InternalKey.kMaxSequenceNumber, ValueType.kTypeValue);
        iter.seek(target.encode());
        String result;

        if (iter.status().isNotOk()) {
            result = iter.status().toString();
        } else {
            result = "[ ";
            boolean first = true;
            while (iter.valid()) {
                Pair<Boolean, ParsedInternalKey> parse = InternalKey.parseInternalKey(iter.key());
                if (!parse.getKey()) {
                    result += "CORRUPTED";
                } else {
                    if (dbTest.lastOptions.getComparator().compare(parse.getValue().getUserKeyChar(), userKey.toCharArray()) != 0) {
                        break;
                    }

                    if (!first) {
                        result += ", ";
                    }

                    first = false;
                    switch (parse.getValue().getValueType()) {
                        case kTypeValue:
                            result += iter.value();
                            break;
                        case kTypeDeletion:
                            result += "DEL";
                            break;
                    }
                }
                iter.next();
            }

            if (!first) {
                result += " ";
            }

            result += "]";
        }

        return result;
    }

    @Test
    public void testHiddenValuesAreRemoved() {
        do {
            fillLevels("a", "z");
            String big = TestUtils.randomString(50000);
            dbTest.put("foo", big);
            dbTest.put("pastfoo", "v");
            long s = dbTest.db.getSnapshot();
            dbTest.put("foo", "tiny");
            dbTest.put("pastfoo2", "v2"); // Advance sequence number one more

            assertTrue(dbTest.db.TEST_compactMemtable().isOk());
            assertTrue(dbTest.numTableFilesAtLevel(0) > 0);

            assertEquals(big, dbTest.get("foo", s));
            assertTrue(between(size("", "pastfoo"), 50000, 60000));
            dbTest.db.releaseSnapshot(s);
            assertEquals(allEntriesFor("foo"), "[ tiny, " + big + " ]");
            dbTest.db.TEST_compactRange(0, null, "x");
            assertEquals(allEntriesFor("foo"), "[ tiny ]");
            assertEquals(dbTest.numTableFilesAtLevel(0), 0);
            assertEquals(dbTest.numTableFilesAtLevel(1), 1);
            dbTest.db.TEST_compactRange(1, null, "x");
            assertEquals(allEntriesFor("foo"), "[ tiny ]");
            assertTrue(between(size("", "pastfoo"), 0, 1000));
        } while (dbTest.changeOptions());
    }

    @Test
    public void testDeletionMarkers1() {
        dbTest.put("foo", "v1");
        assertTrue(dbTest.db.TEST_compactMemtable().isOk());
        int last = Config.kMaxMemCompactLevel;
        assertEquals(dbTest.numTableFilesAtLevel(last), 1); // foo => v1 is now in last level

        // Place a table at level last-1 to prevent merging with preceding mutation
        dbTest.put("a", "begin");
        dbTest.put("z", "end");
        dbTest.db.TEST_compactMemtable();
        assertEquals(dbTest.numTableFilesAtLevel(last), 1);
        assertEquals(dbTest.numTableFilesAtLevel(last - 1), 1);

        dbTest.delete("foo");
        dbTest.put("foo", "v2");
        assertEquals(allEntriesFor("foo"), "[ v2, DEL, v1 ]");
        dbTest.db.TEST_compactMemtable(); // Moves to level last-2
        assertEquals(allEntriesFor("foo"), "[ v2, DEL, v1 ]");
        dbTest.db.TEST_compactRange(last - 2, null, "z");
        // DEL eliminated, but v1 remains because we aren't compacting that level
        // (DEL can be eliminated because v2 hides v1).
        assertEquals(allEntriesFor("foo"), "[ v2, v1 ]");
        dbTest.db.TEST_compactRange(last - 1, null, null);
        // Merging last-1 w/ last, so we are the base level for "foo", so
        // DEL is removed.  (as is v1).
        assertEquals(allEntriesFor("foo"), "[ v2 ]");
    }

    @Test
    public void testDeletionMarkers2() {
        dbTest.put("foo", "v1");
        assertTrue(dbTest.db.TEST_compactMemtable().isOk());
        int last = Config.kMaxMemCompactLevel;
        assertEquals(dbTest.numTableFilesAtLevel(last), 1); // foo => v1 is now in last level

        // Place a table at level last-1 to prevent merging with preceding mutation
        dbTest.put("a", "begin");
        dbTest.put("z", "end");
        dbTest.db.TEST_compactMemtable();
        assertEquals(dbTest.numTableFilesAtLevel(last), 1);
        assertEquals(dbTest.numTableFilesAtLevel(last - 1), 1);

        dbTest.delete("foo");
        assertEquals(allEntriesFor("foo"), "[ DEL, v1 ]");
        dbTest.db.TEST_compactMemtable(); // Moves to level last-2
        assertEquals(allEntriesFor("foo"), "[ DEL, v1 ]");
        dbTest.db.TEST_compactRange(last - 2, null, "z");
        // DEL kept: "last" file overlaps
        assertEquals(allEntriesFor("foo"), "[ DEL, v1 ]");
        dbTest.db.TEST_compactRange(last - 1, null, null);
        // Merging last-1 w/ last, so we are the base level for "foo", so
        // DEL is removed.  (as is v1).
        assertEquals(allEntriesFor("foo"), "[ ]");
    }

    private String filesPerLevel() {
        String result = "";
        int lastNonZeroOffset = 0;
        for (int i = 0; i < Config.kNumLevels; i++) {
            int f = dbTest.numTableFilesAtLevel(i);
            result += String.format("%s%d", i > 0 ? "," : "", f);
            if (f > 0) {
                lastNonZeroOffset = result.length();
            }
        }
        return result.substring(0, lastNonZeroOffset);
    }

    @Test
    public void testOverlapInLevel0() {
        do {
            assertEquals("Fix test to match config", Config.kMaxMemCompactLevel, 2);

            assertTrue(dbTest.put("100", "v100").isOk());
            assertTrue(dbTest.put("999", "v999").isOk());
            dbTest.db.TEST_compactMemtable();
            assertTrue(dbTest.delete("100").isOk());
            assertTrue(dbTest.delete("999").isOk());
            dbTest.db.TEST_compactMemtable();
            assertEquals("0,1,1", filesPerLevel());

            // Make files spanning the following ranges in level-0:
            //  files[0]  200 .. 900
            //  files[1]  300 .. 500
            // Note that files are sorted by smallest key.
            assertTrue(dbTest.put("300", "v300").isOk());
            assertTrue(dbTest.put("500", "v500").isOk());
            dbTest.db.TEST_compactMemtable();
            assertTrue(dbTest.put("200", "v200").isOk());
            assertTrue(dbTest.put("600", "v600").isOk());
            assertTrue(dbTest.put("900", "v900").isOk());
            dbTest.db.TEST_compactMemtable();
            assertEquals("2,1,1", filesPerLevel());

            dbTest.db.TEST_compactRange(1, null, null);
            dbTest.db.TEST_compactRange(2, null, null);
            assertEquals("2", filesPerLevel());

            // Do a memtable compaction.  Before bug-fix, the compaction would
            // not detect the overlap with level-0 files and would incorrectly place
            // the deletion in a deeper level.
            assertTrue(dbTest.delete("600").isOk());
            dbTest.db.TEST_compactMemtable();
            assertEquals("3", filesPerLevel());
            assertEquals("NOT_FOUND", dbTest.get("600"));
        } while (dbTest.changeOptions());
    }

    // Return a string that contains all key,value pairs in order,
    // formatted like "(k1->v1)(k2->v2)".
    private String contents() {
        Vector<String> forward = new Vector<>();
        String result = "";
        Iterator<String, String> iter = dbTest.db.iterator(new ReadOptions());
        for(iter.seekToFirst(); iter.valid(); iter.next()) {
            String s = dbTest.iterStatus(iter);
            result += "(";
            result += s;
            result += ")";
            forward.add(s);
        }

        // Check reverse iteration results are the reverse of forward results
        int match = 0;
        for (iter.seekToLast(); iter.valid(); iter.prev()) {
            assertTrue(match < forward.size());
            assertEquals(dbTest.iterStatus(iter), forward.get(forward.size() - match - 1));
            match ++;
        }
        assertEquals(match, forward.size());
        return result;
    }

    @Test
    public void testL0_CompactionBug_Issue44_a() throws InterruptedException {
        dbTest.reopen();
        assertTrue(dbTest.put("b", "v").isOk());
        dbTest.reopen();
        assertTrue(dbTest.delete("b").isOk());
        assertTrue(dbTest.delete("a").isOk());
        dbTest.reopen();
        assertTrue(dbTest.delete("a").isOk());
        dbTest.reopen();
        assertTrue(dbTest.put("a", "v").isOk());
        dbTest.reopen();
        dbTest.reopen();
        assertEquals("(a->v)", contents());
        Thread.sleep(1000); // wait for compaction finish
        assertEquals("(a->v)", contents());

    }

    @Test
    public void testL0_CompactionBug_Issue44_b() throws InterruptedException {
        dbTest.reopen();
        assertTrue(dbTest.put("", "").isOk());
        dbTest.reopen();
        assertTrue(dbTest.delete("e").isOk());
        assertTrue(dbTest.put("", "").isOk());
        dbTest.reopen();
        assertTrue(dbTest.put("c", "cv").isOk());
        dbTest.reopen();
        assertTrue(dbTest.put("", "").isOk());
        dbTest.reopen();
        assertTrue(dbTest.put("", "").isOk());
        Thread.sleep(1000); // wait for compaction finish
        dbTest.reopen();
        assertTrue(dbTest.put("d", "dv").isOk());
        dbTest.reopen();
        assertTrue(dbTest.put("", "").isOk());
        dbTest.reopen();
        assertTrue(dbTest.delete("d").isOk());
        assertTrue(dbTest.delete("b").isOk());
        dbTest.reopen();
        assertEquals("(->)(c->cv)", contents());
        Thread.sleep(1000); // wait for compaction finish
        assertEquals("(->)(c->cv)", contents());
    }

    class NewComparator implements Comparator {
        private BytewiseComparator bytewiseComparator = new BytewiseComparator();

        @Override
        public String name() {
            return "leveldb.NewComparator";
        }

        @Override
        public int compare(char[] a, char[] b) {
            return bytewiseComparator.compare(a, b);
        }

        @Override
        public char[] findShortestSeparator(char[] a, char[] b) {
            return bytewiseComparator.findShortestSeparator(a, b);
        }

        @Override
        public char[] findShortSuccessor(char[] a) {
            return bytewiseComparator.findShortSuccessor(a);
        }
    }

    @Test
    public void testComparatorCheck() {
        Options options = dbTest.currentOptions();
        options.setComparator(new NewComparator());
        Status status = dbTest.tryReopen(options);
        assertTrue(status.isNotOk());
        assertEquals("Invalid argument: leveldb.BytewiseComparator does not match existing comparator: leveldb.NewComparator", status.toString());
    }

    class NumberComparator implements Comparator {
        @Override
        public String name() {
            return "test.NumberComparator";
        }

        @Override
        public int compare(char[] a, char[] b) {
            return toNumber(a) - toNumber(b);
        }

        @Override
        public char[] findShortestSeparator(char[] a, char[] b) {
            toNumber(a);
            toNumber(b);
            return a;
        }

        @Override
        public char[] findShortSuccessor(char[] a) {
            toNumber(a);
            return a;
        }

        private int toNumber(char[] chars) {
            assertTrue(chars.length >= 2);
            assertEquals('[', chars[0]);
            assertEquals(']', chars[chars.length - 1]);
            return Integer.decode(new String(chars, 1, chars.length - 2));
        }
    }

    @Test
    public void testCustomComparator() {
        NumberComparator numberComparator = new NumberComparator();
        Options options = dbTest.currentOptions();
        options.setCreateIfMissing(true);
        options.setComparator(numberComparator);
        options.setFilterPolicy(null); // Cannot use bloom filters
        options.setWriteBufferSize(1000); // Compact more often
        dbTest.destroyAndReopon(options);
        assertTrue(dbTest.put("[10]", "ten").isOk());
        assertTrue(dbTest.put("[0x14]", "twenty").isOk());
        for (int i = 0; i < 2; i++) {
            assertEquals("ten", dbTest.get("[10]"));
            assertEquals("ten", dbTest.get("[0xa]"));
            assertEquals("twenty", dbTest.get("[20]"));
            assertEquals("twenty", dbTest.get("[0x14]"));
            assertEquals("NOT_FOUND", dbTest.get("[15]"));
            assertEquals("NOT_FOUND", dbTest.get("[0xf]"));
            dbTest.db.compactRange("[0]", "[9999]");
        }

        for (int run = 0; run < 2; run++) {
            for (int i = 0; i < 1000; i++) {
                String value = String.format("[%d]", i * 10);
                assertTrue(dbTest.put(value, value).isOk());
            }

            dbTest.db.compactRange("[0]", "[1000000]");
        }
    }

    @Test
    public void testManualCompaction() {
        assertEquals("Need to update this test to match kMaxMemCompactLevel", 2, Config.kMaxMemCompactLevel);

        makeTables(3, "p", "q");
        assertEquals("1,1,1", filesPerLevel());

        // Compaction range falls before files
        dbTest.db.compactRange("", "c");
        assertEquals("1,1,1", filesPerLevel());

        // Compaction range falls after files
        dbTest.db.compactRange("r", "z");
        assertEquals("1,1,1", filesPerLevel());

        // Compaction range overlaps files
        dbTest.db.compactRange("p1", "p9");
        assertEquals("0,0,1", filesPerLevel());

        // Populate a different range
        makeTables(3, "c", "e");
        assertEquals("1,1,2", filesPerLevel());

        // Compact just the new range
        dbTest.db.compactRange("b", "f");
        assertEquals("0,0,2", filesPerLevel());

        // compact all
        makeTables(1, "a", "z");
        assertEquals("0,1,2", filesPerLevel());
        dbTest.db.compactRange(null, null);
        assertEquals("0,0,1", filesPerLevel());
    }

    @Test
    public void testDBOpen_Options() {
        String dbname = dbTest.env.getTestDirectory().getValue() + "/db_options_test";
        DB.destroyDB(dbname, new Options());

        // Does not exist, and create_if_missing == false: error
        Options options = new Options();
        options.setCreateIfMissing(false);
        Pair<Status, DB> pair = DB.open(options, dbname);
        assertTrue(pair.getKey().toString().contains("does not exist (createIfMissing is false)"));
        assertNull(pair.getValue());

        // Does not exist, and create_if_missing == true: OK
        options.setCreateIfMissing(true);
        pair = DB.open(options, dbname);
        assertTrue(pair.getKey().isOk());
        assertNotNull(pair.getValue());

        pair.getValue().close();

        // Does exist, and error_if_exists == true: error
        options.setCreateIfMissing(false);
        options.setErrorIfExists(true);
        pair = DB.open(options, dbname);
        assertTrue(pair.getKey().toString().contains("exists (errorIfExists is true)"));
        assertNull(pair.getValue());

        // Does exist, and error_if_exists == false: OK
        options.setCreateIfMissing(true);
        options.setErrorIfExists(false);
        pair = DB.open(options, dbname);
        assertTrue(pair.getKey().isOk());
        assertNotNull(pair.getValue());

        pair.getValue().close();
    }

    @Test
    public void testLocking() {
        Pair<Status, DB> pair = DB.open(dbTest.currentOptions(), dbTest.dbname);
        assertTrue(pair.getKey().isNotOk());
    }

    private int countFiles() {
        Pair<Status, List<String>> tmp = dbTest.env.getChildren(dbTest.dbname);
        assertTrue(tmp.getKey().isOk());
        return tmp.getValue().size();
    }

    // Check that number of files does not grow when we are out of space
    @Test
    public void testNoSpace() {
        Options options = dbTest.currentOptions();
        options.setEnv(dbTest.env);
        dbTest.reopen(options);

        assertTrue(dbTest.put("foo", "v1").isOk());
        assertEquals("v1", dbTest.get("foo"));
        dbTest.db.compactRange("a", "z");
        int numFiles = countFiles();
        dbTest.env.noSpace.set(true); // Force out-of-space errors
        for (int i = 0; i < 10; i++) {
            for (int level = 0; level < Config.kNumLevels - 1; level++) {
                dbTest.db.TEST_compactRange(level, null, null);
            }
        }
        dbTest.env.noSpace.set(false);
        assertTrue(countFiles() < numFiles + 3);
    }

    @Test
    public void testNonWritableFileSystem() throws InterruptedException {
        Options options = dbTest.currentOptions();
        options.setWriteBufferSize(1000);
        options.setEnv(dbTest.env);
        dbTest.reopen(options);

        assertTrue(dbTest.put("foo", "v1").isOk());
        dbTest.env.nonWritable.set(true); // Force errors for new files
        String big = StringUtils.repeat('x', 100000);
        int errors = 0;
        for (int i = 0; i < 20; i++) {
            System.out.println(String.format("iter %d; errors %d", i, errors));
            if (dbTest.put("foo", big).isNotOk()) {
                errors ++;
                Thread.sleep(100);
            }
        }
        assertTrue(errors > 0);
        dbTest.env.nonWritable.set(false);
    }

    @Test
    public void testWriteSyncError() {
        // Check that log sync errors cause the DB to disallow future writes.

        // (a) Cause log sync calls to fail
        Options options = dbTest.currentOptions();
        options.setEnv(dbTest.env);
        dbTest.reopen(options);
        dbTest.env.dataSyncError.set(true);

        // (b) Normal write should succeed
        WriteOptions writeOptions = new WriteOptions();
        assertTrue(dbTest.put(writeOptions, "k1", "v1").isOk());
        assertEquals("v1", dbTest.get("k1"));

        // (c) Do a sync write; should fail
        writeOptions.setSync(true);
        assertTrue(dbTest.put(writeOptions, "k2", "v2").isNotOk());
        assertEquals("v1", dbTest.get("k1"));
        assertEquals("NOT_FOUND", dbTest.get("k2"));

        // (d) make sync behave normally
        dbTest.env.dataSyncError.set(false);

        // (e) Do a non-sync write; should fail
        writeOptions.setSync(false);
        assertTrue(dbTest.put(writeOptions, "k3", "v3").isNotOk());
        assertEquals("v1", dbTest.get("k1"));
        assertEquals("NOT_FOUND", dbTest.get("k2"));
        assertEquals("NOT_FOUND", dbTest.get("k3"));
    }

    @Test
    public void testManifestWriteError() {
        // Test for the following problem:
        // (a) Compaction produces file F
        // (b) Log record containing F is written to MANIFEST file, but Sync() fails
        // (c) GC deletes F
        // (d) After reopening DB, reads fail since deleted F is named in log record

        // We iterate twice.  In the second iteration, everything is the
        // same except the log record never makes it to the MANIFEST file.
        for (int iter = 0; iter < 2; iter++) {
            AtomicBoolean errorType = (iter == 0) ? dbTest.env.manifestSyncError
                : dbTest.env.manifestWriteError;

            // Insert foo=>bar mapping
            Options options = dbTest.currentOptions();
            options.setEnv(dbTest.env);
            options.setCreateIfMissing(true);
            options.setErrorIfExists(false);
            dbTest.destroyAndReopon(options);
            assertTrue(dbTest.put("foo", "bar").isOk());
            assertEquals("bar", dbTest.get("foo"));

            // Memtable compaction (will succeed)
            dbTest.db.TEST_compactMemtable();
            assertEquals("bar", dbTest.get("foo"));
            int last = Config.kMaxMemCompactLevel;
            assertEquals(dbTest.numTableFilesAtLevel(last), 1);   // foo=>bar is now in last level

            // Merging compaction (will fail)
            errorType.set(true);
            dbTest.db.TEST_compactRange(last, null, null); // Should fail
            assertEquals("bar", dbTest.get("foo"));

            // Recovery: should not lose data
            errorType.set(false);
            dbTest.reopen(options);
            assertEquals("bar", dbTest.get("foo"));
        }
    }

    @Test
    public void testMissingSSTFile() {
        assertTrue(dbTest.put("foo", "bar").isOk());
        assertEquals("bar", dbTest.get("foo"));

        // Dump the memtable to disk.
        dbTest.db.TEST_compactMemtable();
        assertEquals("bar", dbTest.get("foo"));

        dbTest.close();
        assertTrue(dbTest.deleteAnSSTFile());
        Options options = dbTest.currentOptions();
        options.setParanoidChecks(true);
        Status status = dbTest.tryReopen(options);
        assertTrue(status.isNotOk());
        assertTrue(status.getMessage().contains("1 missing file"));
    }

    @Test
    public void testStillReadSST() {
        assertTrue(dbTest.put("foo", "bar").isOk());
        assertEquals("bar", dbTest.get("foo"));

        // Dump the memtable to disk.
        dbTest.db.TEST_compactMemtable();
        assertEquals("bar", dbTest.get("foo"));
        dbTest.close();

        assertTrue(dbTest.renameLDBToSST() > 0);
        Options options = dbTest.currentOptions();
        options.setParanoidChecks(true);
        Status status = dbTest.tryReopen(options);
        assertTrue(status.isOk());
        assertEquals("bar", dbTest.get("foo"));
    }

    @Test
    public void testFilesDeletedAfterCompaction() {
        assertTrue(dbTest.put("foo", "v2").isOk());
        dbTest.db.compactRange("a", "z");
        int count = countFiles();
        for (int i = 0; i < 10; i++) {
            assertTrue(dbTest.put("foo", "v2").isOk());
            dbTest.db.compactRange("a", "z");
        }
        assertEquals(count, countFiles());
    }

    @Test
    public void testBloomFilter() {
        dbTest.env.countRandomReads = true;
        Options options = dbTest.currentOptions();
        options.setEnv(dbTest.env);
        options.setBlockCache(new ShardedLRUCache(0)); // Prevent cache hits
        options.setFilterPolicy(new BloomFilterPolicy(10));
        dbTest.reopen(options);

        // Populate multiple layers
        int N = 10000;
        for (int i = 0; i < N; i++) {
            assertTrue(dbTest.put(key(i), key(i)).isOk());
        }
        dbTest.db.compactRange("a", "z");
        for (int i = 0; i < N; i+=100) {
            assertTrue(dbTest.put(key(i), key(i)).isOk());
        }
        dbTest.db.TEST_compactMemtable();

        // Prevent auto compactions triggered by seeks
        dbTest.env.delayDataSync.set(true);

        // Lookup present keys.  Should rarely read from small sstable.
        dbTest.env.counter.set(0);
        for (int i = 0; i < N; i++) {
            assertEquals(key(i), dbTest.get(key(i)));
        }
        int reads = dbTest.env.counter.get();
        System.out.println(String.format("%d present => %d reads", N, reads));
        assertTrue(reads >= N);
        assertTrue(reads <= N + 2*N/100);

        // Lookup unpresent keys.  Should rarely read from either sstable.
        dbTest.env.counter.set(0);
        for (int i = 0; i < N; i++) {
            assertEquals("NOT_FOUND", dbTest.get(key(i) + ".missing"));
        }
        reads = dbTest.env.counter.get();
        System.out.println(String.format("%d missing => %d reads", N, reads));
        assertTrue(reads <= 3*N/100);

        dbTest.env.delayDataSync.set(false);
        dbTest.close();
    }
}
