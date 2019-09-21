package com.farmerworking.leveldb.in.java.data.structure.version;

import com.farmerworking.leveldb.in.java.api.*;
import com.farmerworking.leveldb.in.java.api.Comparator;
import com.farmerworking.leveldb.in.java.api.Iterator;
import com.farmerworking.leveldb.in.java.common.TestUtils;
import com.farmerworking.leveldb.in.java.data.structure.cache.TableCache;
import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKey;
import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKeyComparator;
import com.farmerworking.leveldb.in.java.data.structure.memory.ValueType;
import com.farmerworking.leveldb.in.java.data.structure.table.GetSaver;
import com.farmerworking.leveldb.in.java.data.structure.table.GetState;
import com.farmerworking.leveldb.in.java.data.structure.table.ITableBuilder;
import com.farmerworking.leveldb.in.java.file.FileName;
import com.farmerworking.leveldb.in.java.file.WritableFile;
import com.google.common.collect.Lists;
import javafx.util.Pair;
import org.junit.Test;

import java.util.*;
import java.util.function.Predicate;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class VersionTest {
    InternalKeyComparator internalKeyComparator = new InternalKeyComparator(new BytewiseComparator());

    @Test
    public void testEmpty() {
        Version version = new Version(new VersionSet());
        assertEquals(0, version.numFiles(0));

        Vector<Iterator<String, String>> iterators = version.iterators(new ReadOptions());
        assertTrue(iterators.isEmpty());

        Pair<Status, String> pair = version.get(new ReadOptions(), new InternalKey("a", 1L, ValueType.kTypeValue), new GetStats());
        assertTrue(pair.getKey().isNotFound());
    }

    @Test
    public void testGetFilesToSearchForLevel() {
        VersionSet versionSet = new VersionSet();
        versionSet.setOptions(new Options());
        versionSet.setTableCache(new TableCache("tmp", versionSet.getOptions(), 1024));
        versionSet.setInternalKeyComparator(internalKeyComparator);

        Version version = new Version(versionSet);

        List<FileMetaData> result = version.getFilesToSearchForLevel(0, new InternalKey("a", 1L));
        assertEquals(0, version.numFiles(0));
        assertTrue(result.isEmpty());

        version.files.get(0).add(new FileMetaData(1L, 1024,
                new InternalKey("a", 1L, ValueType.kTypeValue),
                new InternalKey("g", 10L, ValueType.kTypeValue)));

        version.files.get(0).add(new FileMetaData(2L, 1024,
                new InternalKey("e", 20L, ValueType.kTypeValue),
                new InternalKey("k", 30L, ValueType.kTypeValue)));

        version.files.get(0).add(new FileMetaData(3L, 1024,
                new InternalKey("g", 40L, ValueType.kTypeValue),
                new InternalKey("n", 50L, ValueType.kTypeValue)));

        assertEquals(3, version.numFiles(0));
        result = version.getFilesToSearchForLevel(0, new InternalKey("h", 1L));
        assertEquals(2, result.size());

        // sort by newest
        assertEquals(3, result.get(0).getFileNumber());
        assertEquals(2, result.get(1).getFileNumber());

        result = version.getFilesToSearchForLevel(1, new InternalKey("a", 1L));
        assertEquals(0, version.numFiles(1));
        assertTrue(result.isEmpty());

        version.files.get(1).add(new FileMetaData(4L, 1024,
                new InternalKey("c", 1L, ValueType.kTypeValue),
                new InternalKey("g", 10L, ValueType.kTypeValue)));
        assertEquals(1, version.numFiles(1));

        result = version.getFilesToSearchForLevel(1, new InternalKey("z", 1L));
        assertTrue(result.isEmpty());

        result = version.getFilesToSearchForLevel(1, new InternalKey("a", 1L));
        assertTrue(result.isEmpty());

        result = version.getFilesToSearchForLevel(1, new InternalKey("d", 1L));
        assertEquals(1, result.size());
        assertEquals(4, result.get(0).getFileNumber());
    }

    @Test
    public void testGetFilesToSearchForLevel0WithSameUserKeyAndLargerSequence() {
        VersionSet versionSet = new VersionSet();
        versionSet.setOptions(new Options());
        versionSet.setTableCache(new TableCache("tmp", versionSet.getOptions(), 1024));
        versionSet.setInternalKeyComparator(internalKeyComparator);

        Version version = new Version(versionSet);
        version.files.get(0).add(new FileMetaData(1L, 1024,
                new InternalKey("a", 100L, ValueType.kTypeValue),
                new InternalKey("g", 200L, ValueType.kTypeValue)));

        version.files.get(1).add(new FileMetaData(1L, 1024,
                new InternalKey("a", 100L, ValueType.kTypeValue),
                new InternalKey("g", 200L, ValueType.kTypeValue)));

        assertEquals(1, version.files.get(0).size());
        assertEquals(1, version.getFilesToSearchForLevel(0, new InternalKey("a", 100L)).size());
        assertEquals(1, version.getFilesToSearchForLevel(0, new InternalKey("a", 101L)).size());
        assertEquals(1, version.getFilesToSearchForLevel(0, new InternalKey("g", 200L)).size());
        assertEquals(1, version.getFilesToSearchForLevel(0, new InternalKey("g", 199L)).size());

        assertEquals(1, version.files.get(1).size());
        assertEquals(1, version.getFilesToSearchForLevel(1, new InternalKey("a", 100L)).size());
        assertEquals(1, version.getFilesToSearchForLevel(1, new InternalKey("a", 101L)).size());
        assertEquals(1, version.getFilesToSearchForLevel(1, new InternalKey("g", 200L)).size());
        assertEquals(0, version.getFilesToSearchForLevel(1, new InternalKey("g", 199L)).size());
    }

    @Test(expected = AssertionError.class)
    public void testRefs() {
        Version version = new Version(new VersionSet());
        assertEquals(0, version.refs);

        version.ref();
        assertEquals(1, version.refs);

        version.unref();
        assertEquals(0, version.refs);

        version.unref();
    }

    @Test(expected = AssertionError.class)
    public void testGetOverlappingInputsAssertionError1() {
        Version version = new Version(new VersionSet());
        version.getOverlappingInputs(-1, null, null);
    }

    @Test(expected = AssertionError.class)
    public void testGetOverlappingInputsAssertionError2() {
        Version version = new Version(new VersionSet());
        version.getOverlappingInputs(10, null, null);
    }

    @Test
    public void testGetOverlappingInputs() {
        VersionSet versionSet = new VersionSet();
        versionSet.setOptions(new Options());
        versionSet.setTableCache(new TableCache("tmp", versionSet.getOptions(), 1024));
        versionSet.setInternalKeyComparator(internalKeyComparator);

        Version version = new Version(versionSet);

        version.files.get(1).add(new FileMetaData(1L, 1024,
                new InternalKey("a", 1L, ValueType.kTypeValue),
                new InternalKey("g", 10L, ValueType.kTypeValue)));

        version.files.get(1).add(new FileMetaData(2L, 1024,
                new InternalKey("h", 20L, ValueType.kTypeValue),
                new InternalKey("n", 30L, ValueType.kTypeValue)));

        version.files.get(1).add(new FileMetaData(3L, 1024,
                new InternalKey("o", 40L, ValueType.kTypeValue),
                new InternalKey("t", 50L, ValueType.kTypeValue)));

        Vector<FileMetaData> result = version.getOverlappingInputs(2, null, null);
        assertTrue(result.isEmpty());

        result = version.getOverlappingInputs(1, null, null);
        assertEquals(3, result.size());

        result = version.getOverlappingInputs(1, new InternalKey("k", 1L, ValueType.kTypeValue), null);
        assertEquals(2, result.size());
        assertEquals(2, result.get(0).getFileNumber());
        assertEquals(3, result.get(1).getFileNumber());

        result = version.getOverlappingInputs(1, null, new InternalKey("k", 1L, ValueType.kTypeValue));
        assertEquals(2, result.size());
        assertEquals(1, result.get(0).getFileNumber());
        assertEquals(2, result.get(1).getFileNumber());

        result = version.getOverlappingInputs(1,
                new InternalKey("k", 1L, ValueType.kTypeValue),
                new InternalKey("m", 1L, ValueType.kTypeValue));
        assertEquals(1, result.size());
        assertEquals(2, result.get(0).getFileNumber());

        version.files.get(0).add(new FileMetaData(1L, 1024,
                new InternalKey("a", 1L, ValueType.kTypeValue),
                new InternalKey("g", 10L, ValueType.kTypeValue)));
        version.files.get(0).add(new FileMetaData(2L, 1024,
                new InternalKey("e", 20L, ValueType.kTypeValue),
                new InternalKey("n", 30L, ValueType.kTypeValue)));
        version.files.get(0).add(new FileMetaData(3L, 1024,
                new InternalKey("o", 40L, ValueType.kTypeValue),
                new InternalKey("t", 50L, ValueType.kTypeValue)));

        result = version.getOverlappingInputs(0,
                new InternalKey("k", 1L, ValueType.kTypeValue),
                new InternalKey("l", 1L, ValueType.kTypeValue));
        assertEquals(2, result.size());
        assertEquals(1, result.get(0).getFileNumber());
        assertEquals(2, result.get(1).getFileNumber());
    }

    @Test
    public void testUpdateStats() {
        Version version = new Version(new VersionSet());

        assertNull(version.fileToCompact);
        assertEquals(-1, version.fileToCompactLevel);

        GetStats stats = new GetStats();
        stats.setSeekFileLevel(1);
        stats.setSeekFile(new FileMetaData(1L, 1024,
                new InternalKey("a", 1L, ValueType.kTypeValue),
                new InternalKey("b", 2L, ValueType.kTypeValue)));

        assertFalse(version.updateStats(stats));
        assertNull(version.fileToCompact);
        assertEquals(-1, version.fileToCompactLevel);

        stats.getSeekFile().setAllowedSeeks(1);
        assertTrue(version.updateStats(stats));
        assertEquals(stats.getSeekFile(), version.fileToCompact);
        assertEquals(stats.getSeekFileLevel(), version.fileToCompactLevel);
    }

    @Test
    public void testGetNormalCase() {
        VersionSet versionSet = new VersionSet();
        versionSet.setOptions(new Options());

        Pair<Status, String> pair = versionSet.getOptions().getEnv().getTestDirectory();
        assertTrue(pair.getKey().isOk());
        String dbname = pair.getValue();

        versionSet.setTableCache(new TableCache(dbname, versionSet.getOptions(), 1024));
        versionSet.setInternalKeyComparator(internalKeyComparator);

        Version version = new Version(versionSet);


        long fileNumber = 1L;
        long sequenceNumber = 1L;
        String fileName = FileName.tableFileName(dbname, fileNumber);
        Pair<Status, WritableFile> filePair = versionSet.getOptions().getEnv().newWritableFile(fileName);
        ITableBuilder builder = ITableBuilder.getDefaultImpl(versionSet.getOptions(), filePair.getValue());

        List<InternalKey> internalKeyList = new ArrayList<>();
        List<Pair<Pair<String, Long>, String>> pairList = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            String userKey = (char)i + TestUtils.randomKey(5);
            internalKeyList.add(new InternalKey(userKey, sequenceNumber, ValueType.kTypeValue));
            String key = internalKeyList.get(internalKeyList.size() - 1).encode();
            String value = TestUtils.randomString(10);

            pairList.add(new Pair<>(new Pair<>(userKey, sequenceNumber ++), value));
            builder.add(key, value);
        }

        Status status = builder.finish();
        assertTrue(status.isOk());

        InternalKey largest = max(internalKeyList);
        InternalKey smallest = min(internalKeyList);

        version.files.get(0).add(new FileMetaData(fileNumber, builder.fileSize(), smallest, largest));
        ReadOptions readOptions = new ReadOptions();
        for (int i = 0; i < 50; i++) {
            Pair<Pair<String, Long>, String> keyValuePair = pairList.get(i);
            GetStats stats = new GetStats();
            Pair<Status, String> getPair = version.get(
                    readOptions,
                    new InternalKey(keyValuePair.getKey().getKey(), keyValuePair.getKey().getValue(), ValueType.kValueTypeForSeek),
                    stats);
            assertTrue(getPair.getKey().isOk());
            assertEquals(keyValuePair.getValue(), getPair.getValue());
        }
    }

    @Test
    public void testGetTableCacheGetStatusError() {
        VersionSet versionSet = new VersionSet();
        versionSet.setInternalKeyComparator(internalKeyComparator);
        TableCache mockTableCache = mock(TableCache.class);
        when(mockTableCache.get(any(), anyLong(), anyLong(), anyString(), any())).thenReturn(Status.Corruption("table cache get force error"));
        versionSet.setTableCache(mockTableCache);

        Version version = spy(new Version(versionSet));
        doReturn(Lists.newArrayList(new FileMetaData(1L, 1L, null, null))).
                when(version).getFilesToSearchForLevel(anyInt(), any());

        Pair<Status, String> getPair = version.get(new ReadOptions(), new InternalKey("a", 0L, ValueType.kValueTypeForSeek), new GetStats());
        assertTrue(getPair.getKey().isCorruption());
        assertEquals("table cache get force error", getPair.getKey().getMessage());
    }

    @Test
    public void testGetTableCacheGetCorruption() {
        VersionSet versionSet = new VersionSet();
        Comparator userComparator = new BytewiseComparator();
        versionSet.setInternalKeyComparator(internalKeyComparator);
        TableCache mockTableCache = mock(TableCache.class);
        when(mockTableCache.get(any(), anyLong(), anyLong(), anyString(), any())).thenReturn(Status.OK());
        versionSet.setTableCache(mockTableCache);

        Version version = spy(new Version(versionSet));
        doReturn(Lists.newArrayList(new FileMetaData(1L, 1L, null, null))).
                when(version).getFilesToSearchForLevel(anyInt(), any());

        GetSaver saver = new GetSaver("a", userComparator);
        saver.setState(GetState.kCorrupt);
        doReturn(saver).when(version).newGetSaver(anyString());

        Pair<Status, String> getPair = version.get(new ReadOptions(), new InternalKey("a", 0L, ValueType.kValueTypeForSeek), new GetStats());
        assertTrue(getPair.getKey().isCorruption());
        assertTrue(getPair.getKey().getMessage().contains("corrupted key for"));
    }

    @Test
    public void testGetTableCacheGetDeleted() {
        VersionSet versionSet = new VersionSet();
        Comparator userComparator = new BytewiseComparator();
        versionSet.setInternalKeyComparator(internalKeyComparator);
        TableCache mockTableCache = mock(TableCache.class);
        when(mockTableCache.get(any(), anyLong(), anyLong(), anyString(), any())).thenReturn(Status.OK());
        versionSet.setTableCache(mockTableCache);

        Version version = spy(new Version(versionSet));
        doReturn(Lists.newArrayList(new FileMetaData(1L, 1L, null, null))).
                when(version).getFilesToSearchForLevel(anyInt(), any());

        GetSaver saver = new GetSaver("a", userComparator);
        saver.setState(GetState.kDeleted);
        doReturn(saver).when(version).newGetSaver(anyString());

        Pair<Status, String> getPair = version.get(new ReadOptions(), new InternalKey("a", 0L, ValueType.kValueTypeForSeek), new GetStats());
        assertTrue(getPair.getKey().isNotFound());
    }

    @Test
    public void testGetTableCacheGetFirstNotFoundSecondDeleted() {
        VersionSet versionSet = new VersionSet();
        Comparator userComparator = new BytewiseComparator();
        versionSet.setInternalKeyComparator(internalKeyComparator);
        TableCache mockTableCache = mock(TableCache.class);
        when(mockTableCache.get(any(), anyLong(), anyLong(), anyString(), any())).thenReturn(Status.OK());
        versionSet.setTableCache(mockTableCache);

        Version version = spy(new Version(versionSet));
        doReturn(Lists.newArrayList(
                new FileMetaData(1L, 1L, null, null),
                new FileMetaData(2L, 1L, null, null))).
                when(version).getFilesToSearchForLevel(anyInt(), any());

        GetSaver saver = new GetSaver("a", userComparator);
        saver.setState(GetState.kNotFound);

        GetSaver saver2 = new GetSaver("a", userComparator);
        saver2.setState(GetState.kDeleted);
        doReturn(saver, saver2).when(version).newGetSaver(anyString());

        Pair<Status, String> getPair = version.get(new ReadOptions(), new InternalKey("a", 0L, ValueType.kValueTypeForSeek), new GetStats());
        assertTrue(getPair.getKey().isNotFound());
    }

    @Test
    public void testGetGetStatsUpdate() {
        VersionSet versionSet = new VersionSet();
        Comparator userComparator = new BytewiseComparator();
        versionSet.setInternalKeyComparator(internalKeyComparator);
        TableCache mockTableCache = mock(TableCache.class);
        when(mockTableCache.get(any(), anyLong(), anyLong(), anyString(), any())).thenReturn(Status.OK());
        versionSet.setTableCache(mockTableCache);

        Version version = spy(new Version(versionSet));
        doReturn(Lists.newArrayList(
                new FileMetaData(1L, 1L, null, null),
                new FileMetaData(2L, 1L, null, null))).
                when(version).getFilesToSearchForLevel(anyInt(), any());

        GetSaver saver = new GetSaver("a", userComparator);
        saver.setState(GetState.kNotFound);

        GetSaver saver2 = new GetSaver("a", userComparator);
        saver2.setState(GetState.kDeleted);
        doReturn(saver, saver2).when(version).newGetSaver(anyString());

        GetStats getStats = new GetStats();
        Pair<Status, String> getPair = version.get(new ReadOptions(), new InternalKey("a", 0L, ValueType.kValueTypeForSeek), getStats);
        assertEquals(0, getStats.getSeekFileLevel());
        assertEquals(1, getStats.getSeekFile().getFileNumber());
        assertTrue(getPair.getKey().isNotFound());
    }

    @Test
    public void testIterators() {
        VersionSet versionSet = new VersionSet();
        versionSet.setInternalKeyComparator(internalKeyComparator);
        versionSet.setOptions(new Options());

        Pair<Status, String> pair = versionSet.getOptions().getEnv().getTestDirectory();
        assertTrue(pair.getKey().isOk());
        String dbname = pair.getValue();

        versionSet.setTableCache(new TableCache(dbname, versionSet.getOptions(), 1024));

        List<Long> fileNumberList = Lists.newArrayList(1L, 2L, 3L, 4L);
        List<ITableBuilder> builderList = new ArrayList<>();
        for(Long fileNumber : fileNumberList) {
            String fileName = FileName.tableFileName(dbname, fileNumber);
            Pair<Status, WritableFile> filePair = versionSet.getOptions().getEnv().newWritableFile(fileName);
            assertTrue(filePair.getKey().isOk());
            builderList.add(ITableBuilder.getDefaultImpl(versionSet.getOptions(), filePair.getValue()));
        }

        long sequenceNumber = 1L;
        List<List<Pair<InternalKey, String>>> lists = new ArrayList<>();
        List<List<InternalKey>> internalKeyLists = new ArrayList<>();
        for (ITableBuilder builder : builderList) {
            List<Pair<InternalKey, String>> list = new ArrayList<>();
            List<InternalKey> internalKeyList = new ArrayList<>();
            internalKeyLists.add(internalKeyList);
            lists.add(list);

            for (int i = 0; i < 50; i++) {
                String userKey = (char)i + TestUtils.randomKey(5);
                InternalKey internalKey = new InternalKey(userKey, sequenceNumber++, ValueType.kTypeValue);

                internalKeyList.add(internalKey);
                String key = internalKey.encode();
                String value = TestUtils.randomString(10);

                list.add(new Pair<>(internalKey, value));
                builder.add(key, value);
            }

            Status status = builder.finish();
            assertTrue(status.isOk());
        }

        Version version = new Version(versionSet);
        version.files.get(0).add(new FileMetaData(1L, builderList.get(0).fileSize(), min(internalKeyLists.get(0)), max(internalKeyLists.get(0))));
        version.files.get(0).add(new FileMetaData(2L, builderList.get(1).fileSize(), min(internalKeyLists.get(1)), max(internalKeyLists.get(1))));
        version.files.get(1).add(new FileMetaData(3L, builderList.get(2).fileSize(), min(internalKeyLists.get(2)), max(internalKeyLists.get(2))));
        version.files.get(1).add(new FileMetaData(4L, builderList.get(3).fileSize(), min(internalKeyLists.get(3)), max(internalKeyLists.get(3))));

        Vector<Iterator<String, String>> iterators = version.iterators(new ReadOptions());
        assertEquals(3, iterators.size());

        for (int i = 0; i < 2; i++) {
            Iterator<String, String> iterator = iterators.get(i);
            iterator.seekToFirst();
            assertTrue(iterator.valid());

            for(Pair<InternalKey, String> item : lists.get(i)) {
                assertEquals(item.getKey().encode(), iterator.key());
                assertEquals(item.getValue(), iterator.value());
                iterator.next();
            }
            assertFalse(iterator.valid());
        }

        Iterator<String, String> iterator = iterators.get(2);
        iterator.seekToFirst();
        assertTrue(iterator.valid());

        List<Pair<InternalKey, String>> tmp = lists.get(2);
        tmp.addAll(lists.get(3));
        for(Pair<InternalKey, String> item : tmp) {
            assertEquals(item.getKey().encode(), iterator.key());
            assertEquals(item.getValue(), iterator.value());
            iterator.next();
        }
        assertFalse(iterator.valid());
    }

    @Test
    public void testForEachOverlapping() {
        Version version = spy(new Version(new VersionSet()));

        doReturn(Lists.newArrayList(
                new FileMetaData(1L, 1L, null, null),
                new FileMetaData(1L, 1L, null, null),
                new FileMetaData(1L, 1L, null, null),
                new FileMetaData(1L, 1L, null, null)
        )).when(version).getFilesToSearchForLevel(anyInt(), any());

        final int[] count = {0};
        version.forEachOverlapping(new InternalKey("a", 1L, ValueType.kValueTypeForSeek), new Predicate<Pair<Integer, FileMetaData>>() {
            @Override
            public boolean test(Pair<Integer, FileMetaData> integerFileMetaDataPair) {
                count[0]++;
                return true;
            }
        });
        assertEquals(4 * Config.kNumLevels, count[0]);

        count[0] = 0;
        version.forEachOverlapping(new InternalKey("a", 1L, ValueType.kValueTypeForSeek), new Predicate<Pair<Integer, FileMetaData>>() {
            @Override
            public boolean test(Pair<Integer, FileMetaData> integerFileMetaDataPair) {
                count[0]++;
                return false;
            }
        });
        assertEquals(1, count[0]);
    }

    @Test
    public void testMatchObj() {
        Version.MatchObj matchObj = new Version.MatchObj();
        assertTrue(
                matchObj.test(new Pair<>(0, new FileMetaData(1L, 1L, null, null)))
        );
        assertFalse(
                matchObj.test(new Pair<>(1, new FileMetaData(2L, 1L, null, null)))
        );

        assertEquals(0, matchObj.stats.getSeekFileLevel());
        assertEquals(1, matchObj.stats.getSeekFile().getFileNumber());
    }

    @Test
    public void testPickLevelForMemTableOutput() {
        VersionSet versionSet = new VersionSet();
        versionSet.setOptions(new Options());
        Version version = new Version(versionSet);
        Version spyVersion = spy(version);

        doReturn(true).when(spyVersion).overlapInLevel(anyInt(), anyString(), anyString());
        assertEquals(0, spyVersion.pickLevelForMemTableOutput("", ""));

        doReturn(false, true).when(spyVersion).overlapInLevel(anyInt(), anyString(), anyString());
        assertEquals(0, spyVersion.pickLevelForMemTableOutput("", ""));

        doReturn(false).when(spyVersion).overlapInLevel(anyInt(), anyString(), anyString());
        doReturn(new Vector<>()).when(spyVersion).getOverlappingInputs(anyInt(), any(InternalKey.class), any(InternalKey.class));
        assertEquals(Config.kMaxMemCompactLevel, spyVersion.pickLevelForMemTableOutput("", ""));

        doReturn(false).when(spyVersion).overlapInLevel(anyInt(), anyString(), anyString());
        doReturn(new Vector<>()).when(spyVersion).getOverlappingInputs(anyInt(), any(InternalKey.class), any(InternalKey.class));
        doReturn(100 * VersionUtils.targetFileSize(versionSet.getOptions())).when(spyVersion).totalFileSize(any());
        assertEquals(0, spyVersion.pickLevelForMemTableOutput("", ""));
    }

    private InternalKey min(Collection<InternalKey> list) {
        return Collections.min(list, new java.util.Comparator<InternalKey>() {
            @Override
            public int compare(InternalKey o1, InternalKey o2) {
                return internalKeyComparator.compare(o1, o2);
            }
        });
    }

    private InternalKey max(Collection<InternalKey> list) {
        return Collections.max(list, new java.util.Comparator<InternalKey>() {
            @Override
            public int compare(InternalKey o1, InternalKey o2) {
                return internalKeyComparator.compare(o1, o2);
            }
        });
    }
}