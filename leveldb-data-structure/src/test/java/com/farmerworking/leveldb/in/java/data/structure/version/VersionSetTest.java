package com.farmerworking.leveldb.in.java.data.structure.version;

import com.farmerworking.leveldb.in.java.api.BytewiseComparator;
import com.farmerworking.leveldb.in.java.api.Iterator;
import com.farmerworking.leveldb.in.java.api.Options;
import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.common.TestUtils;
import com.farmerworking.leveldb.in.java.data.structure.cache.TableCache;
import com.farmerworking.leveldb.in.java.data.structure.log.ILogReader;
import com.farmerworking.leveldb.in.java.data.structure.log.ILogWriter;
import com.farmerworking.leveldb.in.java.data.structure.log.LogReader;
import com.farmerworking.leveldb.in.java.data.structure.log.LogWriter;
import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKey;
import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKeyComparator;
import com.farmerworking.leveldb.in.java.data.structure.table.ITableBuilder;
import com.farmerworking.leveldb.in.java.data.structure.table.ITableReader;
import com.farmerworking.leveldb.in.java.file.Env;
import com.farmerworking.leveldb.in.java.file.FileName;
import com.farmerworking.leveldb.in.java.file.WritableFile;
import com.google.common.collect.Lists;
import javafx.util.Pair;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class VersionSetTest {


    @Test(expected = AssertionError.class)
    public void testGetRangeNull() {
        VersionSet versionSet = new VersionSet("test", new Options(), null, null);
        versionSet.getRange(null);
    }

    @Test(expected = AssertionError.class)
    public void testGetRangeEmpty() {
        VersionSet versionSet = new VersionSet("test", new Options(), null, null);
        versionSet.getRange(new Vector<>());
    }

    @Test
    public void testGetRange() {
        VersionSet versionSet = new VersionSet("test", new Options(), null,
                new InternalKeyComparator(new BytewiseComparator()));
        Vector<FileMetaData> vector = new Vector<>();

        InternalKey small1 = new InternalKey("h", 100L);
        InternalKey large1 = new InternalKey("h", 50L);
        vector.add(new FileMetaData(1L, 0L, small1, large1));

        Pair<InternalKey, InternalKey> pair = versionSet.getRange(vector);
        assertEquals(small1, pair.getKey());
        assertEquals(large1, pair.getValue());

        InternalKey small2 = new InternalKey("c", 100L);
        InternalKey large2 = new InternalKey("e", 50L);
        vector.add(new FileMetaData(1L, 0L, small2, large2));

        pair = versionSet.getRange(vector);
        assertEquals(small2, pair.getKey());
        assertEquals(large1, pair.getValue());
    }

    @Test
    public void testGetRange2() {
        VersionSet versionSet = new VersionSet("test", new Options(), null,
                new InternalKeyComparator(new BytewiseComparator()));
        Vector<FileMetaData> vector = new Vector<>();

        InternalKey small1 = new InternalKey("h", 100L);
        InternalKey large1 = new InternalKey("h", 50L);
        vector.add(new FileMetaData(1L, 0L, small1, large1));

        InternalKey small2 = new InternalKey("c", 100L);
        InternalKey large2 = new InternalKey("e", 50L);
        vector.add(new FileMetaData(1L, 0L, small2, large2));

        Vector<FileMetaData> vector2 = new Vector<>();

        InternalKey small3 = new InternalKey("x", 100L);
        InternalKey large3 = new InternalKey("y", 50L);
        vector2.add(new FileMetaData(1L, 0L, small3, large3));

        assertEquals(2, vector.size());
        assertEquals(1, vector2.size());

        Pair<InternalKey, InternalKey> pair = versionSet.getRange2(vector, vector2);
        assertEquals(small2, pair.getKey());
        assertEquals(large3, pair.getValue());

        // do not change argument passed in
        assertEquals(2, vector.size());
        assertEquals(1, vector2.size());
    }

    @Test
    public void testSetupOtherInputs() {
        int level = 0;
        Options options = new Options();
        options.setInfoLog(new Options.Logger() {
            @Override
            public void log(String msg, String... args) {
                System.out.println(msg);
                for(String arg : args) {
                    System.out.println(arg);
                }
            }
        });

        VersionSet versionSet = new VersionSet("test", options, null, new InternalKeyComparator(new BytewiseComparator()));

        InternalKey small1 = new InternalKey("d", 1L);
        InternalKey large1 = new InternalKey("k", 1L);
        FileMetaData metaData = new FileMetaData(1L, 1L, small1, large1);

        versionSet.getCurrent().files.get(level).add(metaData);
        versionSet.getCurrent().files.get(level).add(new FileMetaData(0L, 1L,
                new InternalKey("a", 1L),
                new InternalKey("c", 1L)
        ));

        versionSet.getCurrent().files.get(level + 1).add(new FileMetaData(1L, 1L,
                new InternalKey("a", 1L),
                new InternalKey("g", 1L)));
        versionSet.getCurrent().files.get(level + 1).add(new FileMetaData(2L, 1L,
                new InternalKey("h", 1L),
                new InternalKey("n", 1L)));
        versionSet.getCurrent().files.get(level + 1).add(new FileMetaData(3L, 1L,
                new InternalKey("o", 1L),
                new InternalKey("t", 1L)));

        // grand parent level
        versionSet.getCurrent().files.get(level + 2).add(new FileMetaData(3L, 1L,
                new InternalKey("x", 1L),
                new InternalKey("y", 1L)));
        versionSet.getCurrent().files.get(level + 2).add(new FileMetaData(3L, 1L,
                new InternalKey("b", 1L),
                new InternalKey("c", 1L)));

        Compaction compaction = new Compaction(versionSet.getOptions(), level);
        compaction.inputs[0].add(metaData);

        assertEquals(1, compaction.inputs[0].size());
        assertTrue(compaction.inputs[1].isEmpty());
        assertTrue(compaction.getEdit().getCompactPointers().isEmpty());
        assertNull(versionSet.compactPointer[level]);
        assertTrue(compaction.grandparents.isEmpty());

        versionSet.setupOtherInputs(compaction);

        assertEquals(2, compaction.inputs[0].size());
        assertEquals(2, compaction.inputs[1].size());
        assertEquals(large1, compaction.getEdit().getCompactPointers().get(0).getValue());
        assertEquals(large1.encode(), versionSet.compactPointer[level]);
        assertEquals(1, compaction.grandparents.size());

        // expand over large case
        VersionSet spyVersionSet = spy(versionSet);
        doReturn(0L).when(spyVersionSet).expandedCompactionByteSizeLimit(any());

        compaction = new Compaction(versionSet.getOptions(), level);
        compaction.inputs[0].add(metaData);

        assertEquals(1, compaction.inputs[0].size());

        spyVersionSet.setupOtherInputs(compaction);

        assertEquals(1, compaction.inputs[0].size());

        // expand change parent overlap file size
        versionSet.setCurrent(spy(versionSet.getCurrent()));
        Vector<FileMetaData> vector = new Vector<>();
        vector.add(null);
        vector.add(null);
        vector.add(null);
        doCallRealMethod().
                doReturn(vector).when(versionSet.getCurrent()).getOverlappingInputs(eq(level + 1), any(), any());

        compaction = new Compaction(versionSet.getOptions(), level);
        compaction.inputs[0].add(metaData);

        assertEquals(1, compaction.inputs[0].size());

        versionSet.setupOtherInputs(compaction);

        assertEquals(1, compaction.inputs[0].size());
    }

    @Test
    public void testMaxBytesForLevel() {
        VersionSet versionSet = new VersionSet("test", new Options(), null, null);
        long level1Size = versionSet.maxBytesForLevel(new Options(), 1);
        assertEquals(10 * level1Size, versionSet.maxBytesForLevel(new Options(), 2));
        assertEquals(10 * 10 * level1Size, versionSet.maxBytesForLevel(new Options(), 3));
    }

    @Test(expected = AssertionError.class)
    public void testAppendVersionException() {
        VersionSet versionSet = new VersionSet("test", new Options(), null, null);
        versionSet.appendVersion(versionSet.getCurrent());
    }

    @Test
    public void testAppendVersion() {
        VersionSet versionSet = new VersionSet("test", new Options(), null, null);
        Version version = new Version(versionSet);

        assertFalse(versionSet.getDummyVersions().contains(version));
        assertNotEquals(version, versionSet.getCurrent());

        versionSet.appendVersion(version);

        assertTrue(versionSet.getDummyVersions().contains(version));
        assertEquals(version, versionSet.getCurrent());
    }

    @Test
    public void testFinalizeVersion() {
        VersionSet versionSet = new VersionSet("test", new Options(), null, null);
        Version version = new Version(versionSet);

        assertEquals(-1, version.compactionLevel);
        assertEquals(-1, version.compactionScore, 0);

        versionSet.finalizeVersion(version);

        assertEquals(0, version.compactionLevel);
        assertEquals(0, version.compactionScore, 0);

        version.files.get(0).add(new FileMetaData(1L, 0L, null, null));
        version.files.get(0).add(new FileMetaData(1L, 0L, null, null));

        versionSet.finalizeVersion(version);

        assertEquals(0, version.compactionLevel);
        assertEquals(0.5, version.compactionScore, 0);

        version.files.get(1).add(new FileMetaData(1L, 10 * 1024 * 1024L, null, null));

        versionSet.finalizeVersion(version);

        assertEquals(1, version.compactionLevel);
        assertEquals(1, version.compactionScore, 0);
    }

    @Test
    public void testWriteSnapshot() {
        VersionSet versionSet = new VersionSet("test", new Options(), null, new InternalKeyComparator(new BytewiseComparator()));

        InternalKey internalKey = new InternalKey("a", 1L);
        versionSet.compactPointer[0] = internalKey.encode();
        FileMetaData metaData = new FileMetaData(1L, 0L,
                new InternalKey("a", 1L),
                new InternalKey("b", 2L));
        versionSet.getCurrent().files.get(0).add(metaData);

        final String[] logContent = new String[1];
        versionSet.writeSnapshot(new ILogWriter() {
            @Override
            public Status addRecord(String data) {
                logContent[0] = data;
                return Status.OK();
            }
        });

        VersionEdit edit = new VersionEdit();

        assertTrue(StringUtils.isEmpty(edit.getComparatorName()));
        assertTrue(edit.getCompactPointers().isEmpty());
        assertTrue(edit.getNewFiles().isEmpty());

        edit.decodeFrom(logContent[0].toCharArray());

        assertEquals(versionSet.getInternalKeyComparator().getUserComparator().name(), edit.getComparatorName());
        assertEquals(1, edit.getCompactPointers().size());
        assertEquals(0, edit.getCompactPointers().get(0).getKey().intValue());
        assertEquals(internalKey, edit.getCompactPointers().get(0).getValue());

        assertEquals(1, edit.getNewFiles().size());
        assertEquals(0, edit.getNewFiles().get(0).getKey().intValue());
        assertEquals(metaData, edit.getNewFiles().get(0).getValue());
    }

    @Test
    public void testAddLiveFiles() {
        VersionSet versionSet = new VersionSet("test", new Options(), null, null);

        Set<Long> liveFiles = versionSet.getLiveFiles();
        assertTrue(liveFiles.isEmpty());

        Version version = new Version(versionSet);
        version.files.get(0).add(new FileMetaData(1, 0L, null, null));
        versionSet.appendVersion(version);
        liveFiles = versionSet.getLiveFiles();
        assertEquals(1, liveFiles.size());

        version = new Version(versionSet);
        version.files.get(1).add(new FileMetaData(2, 0L, null, null));
        versionSet.appendVersion(version);
        liveFiles = versionSet.getLiveFiles();
        assertEquals(2, liveFiles.size());

        version = new Version(versionSet);
        version.files.get(2).add(new FileMetaData(2, 0L, null, null));
        versionSet.appendVersion(version);
        liveFiles = versionSet.getLiveFiles();
        assertEquals(2, liveFiles.size());
    }

    @Test
    public void testLevelSummary() {
        VersionSet versionSet = new VersionSet("test", new Options(), null, null);
        System.out.println(versionSet.levelSummary());
    }

    @Test
    public void testNeedCompaction() {
        VersionSet versionSet = new VersionSet("test", new Options(), null, null);

        assertFalse(versionSet.needCompaction());

        versionSet.getCurrent().compactionScore = 10;
        assertTrue(versionSet.needCompaction());

        versionSet.getCurrent().compactionScore = 0;
        assertFalse(versionSet.needCompaction());

        versionSet.getCurrent().fileToCompact = new FileMetaData(1L, 1L, null, null);
        assertTrue(versionSet.needCompaction());
    }

    @Test
    public void testApproximateOffset() {
        ITableReader mockTableReader = mock(ITableReader.class);
        when(mockTableReader.approximateOffsetOf(anyString())).thenReturn(300L);


        TableCache mockTableCache = mock(TableCache.class);
        when(mockTableCache.iterator(any(), anyLong(), anyLong())).thenReturn(new Pair<>(null, mockTableReader));

        VersionSet versionSet = new VersionSet("test",
                new Options(),
                mockTableCache,
                new InternalKeyComparator(new BytewiseComparator()));

        Version version = new Version(versionSet);

        version.files.get(0).add(new FileMetaData(1L, 1000L, new InternalKey("a", 1L), new InternalKey("b", 1L)));
        version.files.get(0).add(new FileMetaData(1L, 1000L, new InternalKey("m", 1L), new InternalKey("n", 1L)));
        version.files.get(0).add(new FileMetaData(1L, 1000L, new InternalKey("a", 1L), new InternalKey("b", 1L)));
        version.files.get(1).add(new FileMetaData(1L, 1000L, new InternalKey("i", 1L), new InternalKey("j", 1L)));
        version.files.get(1).add(new FileMetaData(1L, 1000L, new InternalKey("k", 1L), new InternalKey("l", 1L)));
        version.files.get(2).add(new FileMetaData(1L, 1000L, new InternalKey("g", 1L), new InternalKey("i", 1L)));
        version.files.get(3).add(new FileMetaData(1L, 1000L, new InternalKey("g", 1L), new InternalKey("i", 1L)));
        assertEquals(2600, versionSet.approximateOffsetOf(version, new InternalKey("h", 1L)));
    }

    @Test
    public void testMakeInputIterator() {
        Options options = new Options();
        InternalKeyComparator internalKeyComparator = new InternalKeyComparator(new BytewiseComparator());

        Pair<Status, String> pair = options.getEnv().getTestDirectory();
        String dbname = pair.getValue();

        Compaction compaction = new Compaction(options, 0);

        Long sequence = 1L;
        List<ITableBuilder> builderList = new ArrayList<>();
        List<InternalKey> internalKeys = new ArrayList<>();
        Map<String, String> data = new HashMap<>();
        for (int i = 1; i < 5; i++) {
            ITableBuilder tableBuilder = ITableBuilder.getDefaultImpl(
                    options,
                    options.getEnv().newWritableFile(FileName.tableFileName(dbname, i)).getValue());
            InternalKey internalKey = new InternalKey(TestUtils.randomKey(5), sequence++);
            String value = TestUtils.randomString(5);
            tableBuilder.add(internalKey.encode(), value);
            assert tableBuilder.finish().isOk();

            builderList.add(tableBuilder);
            internalKeys.add(internalKey);
            data.put(internalKey.encode(), value);
        }

        for (int i = 0; i < 4; i++) {
            if (i < 3) {
                compaction.inputs[0].add(new FileMetaData(i + 1, builderList.get(i).fileSize(), internalKeys.get(i), internalKeys.get(i)));
            } else {
                compaction.inputs[1].add(new FileMetaData(i + 1, builderList.get(i).fileSize(), internalKeys.get(i), internalKeys.get(i)));
            }
        }

        VersionSet versionSet = new VersionSet(dbname, options,
                new TableCache(dbname, options, 1024),
                internalKeyComparator);

        Iterator<String, String> iter = versionSet.makeInputIterator(compaction);
        iter.seekToFirst();
        assertTrue(iter.valid());

        Collections.sort(internalKeys, new Comparator<InternalKey>() {
            @Override
            public int compare(InternalKey o1, InternalKey o2) {
                return internalKeyComparator.compare(o1, o2);
            }
        });

        for (int i = 0; i < 4; i++) {
            assertEquals(iter.key(), internalKeys.get(i).encode());
            assertEquals(iter.value(), data.get(iter.key()));
            iter.next();
        }

        assertFalse(iter.valid());
    }

    @Test
    public void testMaxNextLevelOverlappingBytes() {
        VersionSet versionSet = new VersionSet("", new Options(),
                null, new InternalKeyComparator(new BytewiseComparator()));

        assertEquals(0, versionSet.maxNextLevelOverlappingBytes());

        versionSet.getCurrent().files.get(0).add(new FileMetaData(1L, 1000L,
                new InternalKey("a", 1L),
                new InternalKey("g", 1L)));
        assertEquals(0, versionSet.maxNextLevelOverlappingBytes());

        versionSet.getCurrent().files.get(1).add(new FileMetaData(1L, 1000L,
                new InternalKey("a", 1L),
                new InternalKey("g", 1L)));
        assertEquals(0, versionSet.maxNextLevelOverlappingBytes());

        versionSet.getCurrent().files.get(2).add(new FileMetaData(1L, 1000L,
                new InternalKey("a", 1L),
                new InternalKey("g", 1L)));
        assertEquals(1000L, versionSet.maxNextLevelOverlappingBytes());
    }

    @Test
    public void testCompactRange() {
        VersionSet versionSet = new VersionSet("", new Options(),
                null, new InternalKeyComparator(new BytewiseComparator()));

        assertNull(
                versionSet.compactRange(0,
                        new InternalKey("a", 1L),
                        new InternalKey("g", 1L))
        );

        versionSet.getCurrent().files.get(0).add(new FileMetaData(1L, 1000L,
                new InternalKey("a", 1L),
                new InternalKey("b", 1L)));
        versionSet.getCurrent().files.get(0).add(new FileMetaData(1L, 1000L,
                new InternalKey("c", 1L),
                new InternalKey("d", 1L)));

        Compaction compaction = versionSet.compactRange(0,
                new InternalKey("a", 1L),
                new InternalKey("g", 1L));
        assertEquals(2, compaction.inputs[0].size());

        versionSet.getCurrent().files.get(1).add(new FileMetaData(1L, 1000L,
                new InternalKey("a", 1L),
                new InternalKey("b", 1L)));
        versionSet.getCurrent().files.get(1).add(new FileMetaData(1L, 1000L,
                new InternalKey("c", 1L),
                new InternalKey("d", 1L)));

        compaction = versionSet.compactRange(1,
                new InternalKey("a", 1L),
                new InternalKey("g", 1L));
        assertEquals(2, compaction.inputs[0].size());

        VersionSet spyVersionSet = spy(versionSet);
        doReturn(0L).when(spyVersionSet).maxFileSizeForLevel(anyInt());

        compaction = spyVersionSet.compactRange(0,
                new InternalKey("a", 1L),
                new InternalKey("g", 1L));
        assertEquals(2, compaction.inputs[0].size());

        compaction = spyVersionSet.compactRange(1,
                new InternalKey("a", 1L),
                new InternalKey("g", 1L));
        assertEquals(1, compaction.inputs[0].size());
    }

    @Test
    public void testPickCompaction() {
        VersionSet versionSet = new VersionSet("", new Options(),
                null, new InternalKeyComparator(new BytewiseComparator()));

        Compaction compaction = versionSet.pickCompaction();
        assertNull(compaction);

        FileMetaData a = new FileMetaData(1L, 1000L,
                new InternalKey("a", 1L),
                new InternalKey("c", 1L));
        FileMetaData b = new FileMetaData(1L, 1000L,
                new InternalKey("f", 1L),
                new InternalKey("g", 1L));

        FileMetaData c = new FileMetaData(1L, 1000L,
                new InternalKey("a", 1L),
                new InternalKey("c", 1L));
        FileMetaData d = new FileMetaData(1L, 1000L,
                new InternalKey("f", 1L),
                new InternalKey("g", 1L));

        versionSet.getCurrent().files.get(0).add(a);
        versionSet.getCurrent().files.get(0).add(b);

        versionSet.getCurrent().files.get(2).add(c);
        versionSet.getCurrent().files.get(2).add(d);

        versionSet.getCurrent().fileToCompact = new FileMetaData(1L, 1000L,
                new InternalKey("a", 1L),
                new InternalKey("g", 1L));
        versionSet.getCurrent().fileToCompactLevel = 1;
        compaction = versionSet.pickCompaction();
        assertNotNull(compaction);
        assertEquals(1, compaction.getLevel());
        assertEquals(1, compaction.inputs[0].size());
        assertEquals(versionSet.getCurrent().fileToCompact, compaction.inputs[0].get(0));

        versionSet.getCurrent().fileToCompactLevel = 0;
        compaction = versionSet.pickCompaction();
        assertNotNull(compaction);
        assertEquals(0, compaction.getLevel());
        assertEquals(2, compaction.inputs[0].size());
        assertEquals(a, compaction.inputs[0].get(0));
        assertEquals(b, compaction.inputs[0].get(1));

        versionSet.getCurrent().compactionScore = 2;
        versionSet.getCurrent().compactionLevel = 2;
        assertTrue(StringUtils.isEmpty(versionSet.compactPointer[2]));
        compaction = versionSet.pickCompaction();
        assertNotNull(compaction);
        assertEquals(2, compaction.getLevel());
        assertEquals(1, compaction.inputs[0].size());
        assertEquals(c, compaction.inputs[0].get(0));

        assertFalse(StringUtils.isEmpty(versionSet.compactPointer[2]));
        assertEquals(c.getLargest().encode(), versionSet.compactPointer[2]);
        compaction = versionSet.pickCompaction();
        assertNotNull(compaction);
        assertEquals(2, compaction.getLevel());
        assertEquals(1, compaction.inputs[0].size());
        assertEquals(d, compaction.inputs[0].get(0));
    }

    @Test(expected = AssertionError.class)
    public void testSetSequence() {
        VersionSet versionSet = new VersionSet("", new Options(),
                null, new InternalKeyComparator(new BytewiseComparator()));
        versionSet.setLastSequence(versionSet.getLastSequence() - 1);
    }

    @Test(expected = AssertionError.class)
    public void testNumLevelBytes() {
        VersionSet versionSet = new VersionSet("", new Options(),
                null, new InternalKeyComparator(new BytewiseComparator()));
        versionSet.numLevelBytes(-1);
    }

    @Test(expected = AssertionError.class)
    public void testNumLevelBytes1() {
        VersionSet versionSet = new VersionSet("", new Options(),
                null, new InternalKeyComparator(new BytewiseComparator()));
        versionSet.numLevelBytes(Config.kNumLevels);
    }

    @Test(expected = AssertionError.class)
    public void testNumLevelFiles() {
        VersionSet versionSet = new VersionSet("", new Options(),
                null, new InternalKeyComparator(new BytewiseComparator()));
        versionSet.numLevelFiles(-1);
    }

    @Test(expected = AssertionError.class)
    public void testNumLevelFiles1() {
        VersionSet versionSet = new VersionSet("", new Options(),
                null, new InternalKeyComparator(new BytewiseComparator()));
        versionSet.numLevelFiles(Config.kNumLevels);
    }

    @Test
    public void testNewFileNumber() {
        VersionSet versionSet = new VersionSet("", new Options(),
                null, new InternalKeyComparator(new BytewiseComparator()));
        assertEquals(versionSet.newFileNumber() + 1, versionSet.newFileNumber());
    }

    @Test
    public void testReuseFileNumber() {
        VersionSet versionSet = new VersionSet("", new Options(),
                null, new InternalKeyComparator(new BytewiseComparator()));
        long fileNumber = versionSet.newFileNumber();
        assertEquals(fileNumber + 1, versionSet.getNextFileNumber());

        versionSet.reuseFileNumber(fileNumber - 1);
        assertEquals(fileNumber + 1, versionSet.getNextFileNumber());

        versionSet.reuseFileNumber(fileNumber + 1);
        assertEquals(fileNumber + 1, versionSet.getNextFileNumber());

        versionSet.reuseFileNumber(fileNumber);
        assertEquals(fileNumber, versionSet.getNextFileNumber());
    }

    @Test
    public void testMarkFileNumberUsed() {
        VersionSet versionSet = new VersionSet("", new Options(),
                null, new InternalKeyComparator(new BytewiseComparator()));
        long fileNumber = versionSet.getNextFileNumber();

        versionSet.markFileNumberUsed(fileNumber - 1);
        assertEquals(fileNumber, versionSet.getNextFileNumber());

        versionSet.markFileNumberUsed(fileNumber + 100);
        assertEquals(fileNumber + 101, versionSet.getNextFileNumber());
    }

    @Test
    public void testReuseManifest() {
        Options options = new Options();
        String dbname = options.getEnv().getTestDirectory().getValue();
        VersionSet versionSet = new VersionSet(dbname, options,
                null, new InternalKeyComparator(new BytewiseComparator()));

        assertFalse(versionSet.reuseManifest("", ""));
        options.setReuseLogs(true);
        assertFalse(versionSet.reuseManifest("", ""));

        Pair<Status, WritableFile> pair = options.getEnv().newWritableFile(dbname + "/MANIFEST-9");
        assertTrue(pair.getKey().isOk());
        assertNull(versionSet.getDescriptorFile());
        assertNull(versionSet.getDescriptorLog());
        assertNotEquals(9, versionSet.getManifestFileNumber());

        assertTrue(versionSet.reuseManifest(dbname + "/MANIFEST-9", "MANIFEST-9"));
        assertNotNull(versionSet.getDescriptorFile());
        assertNotNull(versionSet.getDescriptorLog());
        assertEquals(9, versionSet.getManifestFileNumber());

        // getFileSize error
        versionSet = new VersionSet(dbname, options,
                null, new InternalKeyComparator(new BytewiseComparator()));
        Env env = options.getEnv();
        versionSet.setEnv(spy(env));
        doReturn(new Pair<>(Status.Corruption("force error"), null)).when(versionSet.getEnv()).getFileSize(anyString());
        assertFalse(versionSet.reuseManifest(dbname + "/MANIFEST-9", "MANIFEST-9"));

        // too large
        versionSet = new VersionSet(dbname, options,
                null, new InternalKeyComparator(new BytewiseComparator()));
        versionSet.setEnv(spy(env));
        doReturn(new Pair<>(Status.OK(), VersionUtils.targetFileSize(options) - 1)).when(versionSet.getEnv()).getFileSize(anyString());
        assertTrue(versionSet.reuseManifest(dbname + "/MANIFEST-9", "MANIFEST-9"));
        doReturn(new Pair<>(Status.OK(), VersionUtils.targetFileSize(options) + 1)).when(versionSet.getEnv()).getFileSize(anyString());
        assertFalse(versionSet.reuseManifest(dbname + "/MANIFEST-9", "MANIFEST-9"));

        // appendable file error
        versionSet = new VersionSet(dbname, options,
                null, new InternalKeyComparator(new BytewiseComparator()));
        versionSet.setEnv(spy(env));
        doReturn(new Pair<>(Status.Corruption(""), null)).when(versionSet.getEnv()).newAppendableFile(anyString());
        assertFalse(versionSet.reuseManifest(dbname + "/MANIFEST-9", "MANIFEST-9"));
        versionSet.setEnv(env);
        assertTrue(versionSet.reuseManifest(dbname + "/MANIFEST-9", "MANIFEST-9"));
    }

    @Test
    public void testRecover() {
        Options options = new Options();
        String dbname = options.getEnv().getTestDirectory().getValue();
        newDb(options, dbname);

        VersionSet versionSet = new VersionSet(dbname, options, null, new InternalKeyComparator(new BytewiseComparator()));
        Pair<Status, Boolean> pair = versionSet.recover();
        assertTrue(pair.getKey().isOk());
        assertTrue(pair.getValue());

        // reuse manifest
        VersionSet spyVersionSet = spy(versionSet);
        doReturn(true).when(spyVersionSet).reuseManifest(anyString(), anyString());
        pair = spyVersionSet.recover();
        assertTrue(pair.getKey().isOk());
        assertFalse(pair.getValue());

        // read current file error
        doReturn(new Pair<>(Status.Corruption("force error"), null)).when(spyVersionSet).readFileToString();
        pair = spyVersionSet.recover();
        assertTrue(pair.getKey().isNotOk());
        assertEquals("force error", pair.getKey().getMessage());

        // current file content error
        doReturn(new Pair<>(Status.OK(), "")).when(spyVersionSet).readFileToString();
        pair = spyVersionSet.recover();
        assertTrue(pair.getKey().isNotOk());
        assertEquals("CURRENT file does not end with newline", pair.getKey().getMessage());

        // current file content error
        doReturn(new Pair<>(Status.OK(), "abc")).when(spyVersionSet).readFileToString();
        pair = spyVersionSet.recover();
        assertTrue(pair.getKey().isNotOk());
        assertEquals("CURRENT file does not end with newline", pair.getKey().getMessage());

        // manifest file not exist
        doReturn(new Pair<>(Status.OK(), "notexist\n")).when(spyVersionSet).readFileToString();
        pair = spyVersionSet.recover();
        assertTrue(pair.getKey().isNotOk());
        assertTrue(pair.getKey().getMessage().contains("No such file or directory"));
        doCallRealMethod().when(spyVersionSet).readFileToString();

        // log read error
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                VersionSet.LogReporter reporter = invocation.getArgument(1);
                reporter.corruption(1000L, Status.Corruption("force log read error"));
                return new LogReader(invocation.getArgument(0), reporter, true, 0);
            }
        }).when(spyVersionSet).getLogReader(any(), any());
        pair = spyVersionSet.recover();
        assertTrue(pair.getKey().isNotOk());
        assertEquals("force log read error", pair.getKey().getMessage());
        doCallRealMethod().when(spyVersionSet).getLogReader(any(), any());

        // edit decode error
        VersionEdit mockEdit = mock(VersionEdit.class);
        when(mockEdit.decodeFrom(any())).thenReturn(Status.Corruption("decode error"));
        doReturn(mockEdit).when(spyVersionSet).getVersionEdit();
        pair = spyVersionSet.recover();
        assertTrue(pair.getKey().isNotOk());
        assertEquals("decode error", pair.getKey().getMessage());
        doCallRealMethod().when(spyVersionSet).getVersionEdit();

        // comparator not match
        VersionEdit edit = new VersionEdit();
        edit.setComparatorName("lalalal");
        StringBuilder builder = new StringBuilder();
        edit.encodeTo(builder);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                LogReader logReader = (LogReader) invocation.callRealMethod();
                LogReader spyLogReader = spy(logReader);
                doReturn(new Pair<>(true, builder.toString())).when(spyLogReader).readRecord();
                return spyLogReader;
            }
        }).when(spyVersionSet).getLogReader(any(), any());
        pair = spyVersionSet.recover();
        assertTrue(pair.getKey().isNotOk());
        assertEquals("lalalal does not match existing comparator: leveldb.BytewiseComparator", pair.getKey().getMessage());
        doCallRealMethod().when(spyVersionSet).getLogReader(any(), any());

        // no next file
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                VersionEdit edit = (VersionEdit) invocation.callRealMethod();
                VersionEdit spyEdit = spy(edit);
                doReturn(false).when(spyEdit).isHasNextFileNumber();
                return spyEdit;
            }
        }).when(spyVersionSet).getVersionEdit();
        pair = spyVersionSet.recover();
        assertTrue(pair.getKey().isNotOk());
        assertEquals("no meta-nextfile entry in descriptor", pair.getKey().getMessage());

        // no log number
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                VersionEdit edit = (VersionEdit) invocation.callRealMethod();
                VersionEdit spyEdit = spy(edit);
                doReturn(false).when(spyEdit).isHasLogNumber();
                return spyEdit;
            }
        }).when(spyVersionSet).getVersionEdit();
        pair = spyVersionSet.recover();
        assertTrue(pair.getKey().isNotOk());
        assertEquals("no meta-lognumber entry in descriptor", pair.getKey().getMessage());

        // no last sequence
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                VersionEdit edit = (VersionEdit) invocation.callRealMethod();
                VersionEdit spyEdit = spy(edit);
                doReturn(false).when(spyEdit).isHasLastSequence();
                return spyEdit;
            }
        }).when(spyVersionSet).getVersionEdit();
        pair = spyVersionSet.recover();
        assertTrue(pair.getKey().isNotOk());
        assertEquals("no last-sequence-number entry in descriptor", pair.getKey().getMessage());

        // final redo recover
        pair = versionSet.recover();
        assertTrue(pair.getKey().isOk());
        assertTrue(pair.getValue());
    }

    @Test(expected = AssertionError.class)
    public void testLogAndApplyNeedLockHold() {
        VersionSet versionSet = new VersionSet("", new Options(), null, null);
        versionSet.logAndApply(new VersionEdit(), new ReentrantLock());
    }

    @Test(expected = AssertionError.class)
    public void testLogAndApplyMalformedLogNumber() {
        VersionSet versionSet = new VersionSet("", new Options(), null, null);
        ReentrantLock lock = new ReentrantLock();
        lock.lock();

        VersionEdit edit = new VersionEdit();
        edit.setLogNumber(versionSet.getLogNumber() - 1);
        versionSet.logAndApply(edit, lock);
    }

    @Test(expected = AssertionError.class)
    public void testLogAndApplyMalformedLogNumber1() {
        VersionSet versionSet = new VersionSet("", new Options(), null, null);
        ReentrantLock lock = new ReentrantLock();
        lock.lock();

        VersionEdit edit = new VersionEdit();
        edit.setLogNumber(versionSet.getNextFileNumber());
        versionSet.logAndApply(edit, lock);
    }

    @Test(expected = AssertionError.class)
    public void testLogAndApplyDescriptorLogNullAndFileNotNull() {
        VersionSet versionSet = new VersionSet("", new Options(), null, null);
        ReentrantLock lock = new ReentrantLock();
        lock.lock();

        VersionEdit edit = new VersionEdit();
        versionSet.setDescriptorFile(new WritableFile() {
            @Override
            public Status append(String data) {
                return null;
            }

            @Override
            public Status flush() {
                return null;
            }

            @Override
            public Status close() {
                return null;
            }

            @Override
            public Status sync() {
                return null;
            }
        });
        versionSet.logAndApply(edit, lock);
    }

    @Test
    public void testLogAndApplyNewWritableFileError() {
        Options options = new Options();
        Env spyEnv = spy(options.getEnv());
        doReturn(new Pair<>(Status.Corruption("force create file error"), null)).when(spyEnv).newWritableFile(anyString());
        options.setEnv(spyEnv);

        VersionSet versionSet = new VersionSet("", options, null, null);
        versionSet.setManifestFileNumber(10);
        ReentrantLock lock = new ReentrantLock();
        lock.lock();

        VersionEdit edit = new VersionEdit();
        Status status = versionSet.logAndApply(edit, lock);
        assertTrue(status.isNotOk());
        assertEquals("force create file error", status.getMessage());
    }

    @Test
    public void testLogAndApplyWriteSnapshotError() {
        Options options = new Options();
        String dbname = options.getEnv().getTestDirectory().getValue();

        VersionSet versionSet = new VersionSet(dbname, options, null, null);
        versionSet.setManifestFileNumber(10);
        VersionSet spyVersionSet = spy(versionSet);
        doReturn(Status.Corruption("force write snapshot error")).when(spyVersionSet).writeSnapshot(any());

        ReentrantLock lock = new ReentrantLock();
        lock.lock();

        VersionEdit edit = new VersionEdit();
        Status status = spyVersionSet.logAndApply(edit, lock);
        assertTrue(status.isNotOk());
        assertEquals("force write snapshot error", status.getMessage());
        verifyLogAndApplyCleanupWhenStatusNotOk(options, dbname, spyVersionSet);
    }

    @Test
    public void testLogAndApplyAddLogRecordError() {
        Options options = new Options();
        String dbname = options.getEnv().getTestDirectory().getValue();

        VersionSet versionSet = new VersionSet(dbname, options, null, new InternalKeyComparator(new BytewiseComparator()));
        versionSet.setManifestFileNumber(10);
        VersionSet spyVersionSet = spy(versionSet);
        doReturn(Status.Corruption("force add log record error")).when(spyVersionSet).addRecord(anyString());

        ReentrantLock lock = new ReentrantLock();
        lock.lock();

        VersionEdit edit = new VersionEdit();
        Status status = spyVersionSet.logAndApply(edit, lock);
        assertTrue(status.isNotOk());
        assertEquals("force add log record error", status.getMessage());
        verifyLogAndApplyCleanupWhenStatusNotOk(options, dbname, spyVersionSet);
    }

    @Test
    public void testLogAndApplySyncError() {
        Options options = new Options();
        String dbname = options.getEnv().getTestDirectory().getValue();

        VersionSet versionSet = new VersionSet(dbname, options, null, new InternalKeyComparator(new BytewiseComparator()));
        versionSet.setManifestFileNumber(10);
        VersionSet spyVersionSet = spy(versionSet);
        doReturn(Status.Corruption("force sync error")).when(spyVersionSet).sync();

        ReentrantLock lock = new ReentrantLock();
        lock.lock();

        VersionEdit edit = new VersionEdit();
        Status status = spyVersionSet.logAndApply(edit, lock);
        assertTrue(status.isNotOk());
        assertEquals("force sync error", status.getMessage());
        verifyLogAndApplyCleanupWhenStatusNotOk(options, dbname, spyVersionSet);
    }

    @Test
    public void testLogAndApplySetCurrentError() {
        Options options = new Options();
        String dbname = options.getEnv().getTestDirectory().getValue();

        VersionSet versionSet = new VersionSet(dbname, options, null, new InternalKeyComparator(new BytewiseComparator()));
        versionSet.setManifestFileNumber(10);
        VersionSet spyVersionSet = spy(versionSet);
        doReturn(Status.Corruption("force set current file error")).when(spyVersionSet).setCurrentFile();

        ReentrantLock lock = new ReentrantLock();
        lock.lock();

        VersionEdit edit = new VersionEdit();
        Status status = spyVersionSet.logAndApply(edit, lock);
        assertTrue(status.isNotOk());
        assertEquals("force set current file error", status.getMessage());
        verifyLogAndApplyCleanupWhenStatusNotOk(options, dbname, spyVersionSet);
    }

    @Test
    public void testLogAndApply() {
        Options options = new Options();
        String dbname = options.getEnv().getTestDirectory().getValue();
        newDb(options, dbname);

        VersionSet versionSet = new VersionSet(dbname, options, null, new InternalKeyComparator(new BytewiseComparator()));
        Pair<Status, Boolean> pair = versionSet.recover();
        assertTrue(pair.getKey().isOk());

        ReentrantLock lock = new ReentrantLock();
        lock.lock();

        VersionEdit edit = new VersionEdit();
        Version current = versionSet.getCurrent();

        assertNull(versionSet.getDescriptorLog());
        assertNull(versionSet.getDescriptorFile());

        // set state
        versionSet.setPrevLogNumber(100);
        versionSet.setLogNumber(1000);
        versionSet.setNextFileNumber(2000);
        versionSet.setLastSequence(3000);
        edit.getCompactPointers().add(new Pair<>(0, new InternalKey("a", 1L)));
        edit.getCompactPointers().add(new Pair<>(1, new InternalKey("d", 1L)));
        edit.getCompactPointers().add(new Pair<>(2, new InternalKey("g", 1L)));
        versionSet.getCurrent().files.get(0).add(new FileMetaData(1L, 1000L,
                new InternalKey("a", 1L),
                new InternalKey("c", 1L)));
        versionSet.getCurrent().files.get(0).add(new FileMetaData(1L, 1000L,
                new InternalKey("b", 1L),
                new InternalKey("d", 1L)));
        versionSet.getCurrent().files.get(1).add(new FileMetaData(1L, 1000L,
                new InternalKey("a", 1L),
                new InternalKey("d", 1L)));
        versionSet.getCurrent().files.get(2).add(new FileMetaData(1L, 1000L,
                new InternalKey("b", 1L),
                new InternalKey("e", 1L)));
        // end set state

        Status status = versionSet.logAndApply(edit, lock);

        assertTrue(status.isOk());
        assertNotEquals(current, versionSet.getCurrent());
        assertNotEquals(Version.DEFAULT_COMPACTION_SCORE, versionSet.getCurrent().compactionScore);
        assertNotEquals(Version.DEFAULT_COMPACTION_LEVEL, versionSet.getCurrent().compactionLevel);
        assertNotNull(versionSet.getDescriptorLog());
        assertNotNull(versionSet.getDescriptorFile());
        assertTrue(lock.isHeldByCurrentThread());

        // write snapshot record
        LogReader logReader = new LogReader(options.getEnv().newSequentialFile(
                FileName.descriptorFileName(dbname, versionSet.getManifestFileNumber())).getValue(),
                null, true, 0);
        String record = lastSecondRecord(logReader);
        assertNotNull(record);

        // update current file content
        assertEquals(String.format("MANIFEST-%d", versionSet.getManifestFileNumber())+ "\n",
                Env.readFileToString(options.getEnv(), FileName.currentFileName(dbname)).getValue());

        VersionEdit snapshot = new VersionEdit();
        status = snapshot.decodeFrom(record.toCharArray());
        assertTrue(status.isOk());
        verifySnapshot(versionSet, snapshot);

        VersionSet instance = new VersionSet(dbname, options, null, new InternalKeyComparator(new BytewiseComparator()));
        pair = instance.recover();
        assertTrue(pair.getKey().isOk());
        assertEquals(100, instance.getPrevLogNumber());
        assertEquals(1000, instance.getLogNumber());
        assertEquals(2000, instance.getManifestFileNumber());
        assertEquals(2001, instance.getNextFileNumber());
        assertEquals(3000, instance.getLastSequence());
        assertEquals(new InternalKey("a", 1L).encode(), instance.compactPointer[0]);
        assertEquals(new InternalKey("d", 1L).encode(), instance.compactPointer[1]);
        assertEquals(new InternalKey("g", 1L).encode(), instance.compactPointer[2]);
        assertEquals(2, instance.getCurrent().files.get(0).size());
        assertEquals(1, instance.getCurrent().files.get(1).size());
        assertEquals(1, instance.getCurrent().files.get(2).size());

        assertEquals(new FileMetaData(1L, 1000L, new InternalKey("a", 1L), new InternalKey("c", 1L)), instance.getCurrent().files.get(0).get(0));
        assertEquals(new FileMetaData(1L, 1000L, new InternalKey("b", 1L), new InternalKey("d", 1L)), instance.getCurrent().files.get(0).get(1));
        assertEquals(new FileMetaData(1L, 1000L, new InternalKey("a", 1L), new InternalKey("d", 1L)), instance.getCurrent().files.get(1).get(0));
        assertEquals(new FileMetaData(1L, 1000L, new InternalKey("b", 1L), new InternalKey("e", 1L)), instance.getCurrent().files.get(2).get(0));

        // logAndApply not first call scenario
        ILogWriter beforeLog = versionSet.getDescriptorLog();
        WritableFile beforeFile = versionSet.getDescriptorFile();
        int beforeLogCount = logCount(new LogReader(options.getEnv().newSequentialFile(
                FileName.descriptorFileName(dbname, versionSet.getManifestFileNumber())).getValue(),
                null, true, 0));

        status = versionSet.logAndApply(new VersionEdit(), lock);

        assertTrue(status.isOk());
        assertNotEquals(current, versionSet.getCurrent());
        assertNotEquals(Version.DEFAULT_COMPACTION_SCORE, versionSet.getCurrent().compactionScore);
        assertNotEquals(Version.DEFAULT_COMPACTION_LEVEL, versionSet.getCurrent().compactionLevel);
        assertTrue(lock.isHeldByCurrentThread());

        // descriptor log and file not change
        assertEquals(beforeLog, versionSet.getDescriptorLog());
        assertEquals(beforeFile, versionSet.getDescriptorFile());

        // current file content not change
        assertEquals(String.format("MANIFEST-%d", versionSet.getManifestFileNumber())+ "\n",
                Env.readFileToString(options.getEnv(), FileName.currentFileName(dbname)).getValue());

        // no snapshot record
        int logCount = logCount(new LogReader(options.getEnv().newSequentialFile(
                FileName.descriptorFileName(dbname, versionSet.getManifestFileNumber())).getValue(),
                null, true, 0));
        assertEquals(logCount, beforeLogCount + 1);
    }

    private void verifySnapshot(VersionSet versionSet, VersionEdit edit) {
        // comparator name
        assertEquals(versionSet.getInternalKeyComparator().getUserComparator().name(), edit.getComparatorName());

        // compact point
        List<Integer> indexList = Lists.newArrayList(0, 1, 2, 3, 4, 5, 6);
        for(Pair<Integer, InternalKey> pair : edit.getCompactPointers()) {
            indexList.remove(pair.getKey());
            assertNotNull(versionSet.compactPointer[pair.getKey()]);
            assertEquals(versionSet.compactPointer[pair.getKey()], pair.getValue().encode());
        }

        for(Integer index : indexList) {
            assertNull(versionSet.compactPointer[index]);
        }

        // files
        List<Vector<FileMetaData>> files = new ArrayList<>();
        for(Pair<Integer, FileMetaData> pair : edit.getNewFiles()) {
            if (files.size() <= pair.getKey()) {
                files.add(new Vector<>());
            }

            files.get(pair.getKey()).add(pair.getValue());
        }

        for (int i = 0; i < Config.kNumLevels; i++) {
            if (files.size() > i) {
                assertEquals(files.get(i), versionSet.getCurrent().files.get(i));
            } else {
                assertTrue(versionSet.getCurrent().files.get(i).isEmpty());
            }
        }
    }

    private String lastSecondRecord(LogReader logReader) {
        String second = null;
        String last = null;
        for (Pair<Boolean, String> pair = logReader.readRecord(); pair.getKey(); pair = logReader.readRecord()) {
            second = last;
            last = pair.getValue();
        }

        return second;
    }

    private void newDb(Options options, String dbname) {
        // create manifest file
        Pair<Status, WritableFile> tmp = options.getEnv().newWritableFile(FileName.descriptorFileName(dbname, 1));
        assertTrue(tmp.getKey().isOk());

        // init manifest file content
        VersionEdit newDb = new VersionEdit();
        newDb.setComparatorName(new BytewiseComparator().name());
        newDb.setLogNumber(0);
        newDb.setNextFileNumber(2);
        newDb.setLastSequence(0);

        LogWriter logWriter = new LogWriter(tmp.getValue());
        StringBuilder builder = new StringBuilder();
        newDb.encodeTo(builder);
        Status status = logWriter.addRecord(builder.toString());
        assertTrue(status.isOk());
        status = tmp.getValue().close();
        assertTrue(status.isOk());

        // create current file
        status = FileName.setCurrentFile(options.getEnv(), dbname, 1);
        assertTrue(status.isOk());
    }

    private void verifyLogAndApplyCleanupWhenStatusNotOk(Options options, String dbname, VersionSet versionSet) {
        assertNull(versionSet.getDescriptorFile());
        assertNull(versionSet.getDescriptorLog());
        assertTrue(options.getEnv().isFileExists(FileName.descriptorFileName(dbname, 10)).getKey().isOk());
        assertFalse(options.getEnv().isFileExists(FileName.descriptorFileName(dbname, 10)).getValue());
    }

    private int logCount(ILogReader logReader) {
        int result = 0;
        for(Pair<Boolean, String> pair = logReader.readRecord(); pair.getKey(); pair = logReader.readRecord()) {
            result ++;
        }

        return result;
    }
}