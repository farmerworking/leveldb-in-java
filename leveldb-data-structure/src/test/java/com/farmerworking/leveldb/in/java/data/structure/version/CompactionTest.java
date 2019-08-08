package com.farmerworking.leveldb.in.java.data.structure.version;

import com.farmerworking.leveldb.in.java.api.BytewiseComparator;
import com.farmerworking.leveldb.in.java.api.Options;
import com.farmerworking.leveldb.in.java.data.structure.cache.TableCache;
import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKey;
import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKeyComparator;
import com.farmerworking.leveldb.in.java.data.structure.memory.ValueType;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class CompactionTest {
    @Test
    public void testInit() {
        Compaction compaction = new Compaction(new Options(), 1);

        assertEquals(1, compaction.getLevel());
        assertNull(compaction.inputVersion);
        assertEquals(0, compaction.numInputFiles(0));
        assertEquals(0, compaction.numInputFiles(1));
        assertTrue(compaction.maxOutputFileSize() > 0);
        assertTrue(compaction.grandparents.isEmpty());
        assertNotNull(compaction.getEdit());
    }

    @Test
    public void testTrivialMove() {
        VersionSet versionSet = new VersionSet();
        versionSet.setOptions(new Options());

        Compaction compaction = new Compaction(versionSet.getOptions(), 1);

        compaction.inputVersion = new Version(versionSet);
        compaction.inputs[0].add(new FileMetaData(0L, 0L, null, null));
        assertTrue(compaction.isTrivialMove());
    }

    @Test
    public void testNotTrivialMoveCase1() {
        VersionSet versionSet = new VersionSet();
        versionSet.setOptions(new Options());

        Compaction compaction = new Compaction(versionSet.getOptions(), 1);

        compaction.inputVersion = new Version(versionSet);
        compaction.inputs[0].add(new FileMetaData(0L, 0L, null, null));
        compaction.inputs[0].add(new FileMetaData(0L, 0L, null, null));
        assertFalse(compaction.isTrivialMove());
    }

    @Test
    public void testNotTrivialMoveCase2() {
        VersionSet versionSet = new VersionSet();
        versionSet.setOptions(new Options());

        Compaction compaction = new Compaction(versionSet.getOptions(), 1);

        compaction.inputVersion = new Version(versionSet);
        compaction.inputs[0].add(new FileMetaData(0L, 0L, null, null));
        compaction.inputs[1].add(new FileMetaData(0L, 0L, null, null));
        assertFalse(compaction.isTrivialMove());
    }

    @Test
    public void testNotTrivialMoveCase3() {
        VersionSet versionSet = new VersionSet();
        versionSet.setOptions(new Options());

        Compaction spyCompaction = spy(new Compaction(versionSet.getOptions(), 1));

        spyCompaction.inputVersion = new Version(versionSet);
        spyCompaction.inputs[0].add(new FileMetaData(0L, 0L, null, null));
        assertTrue(spyCompaction.isTrivialMove());

        doReturn(-1L).when(spyCompaction).maxGrandParentOverlapBytes();
        assertEquals(-1, spyCompaction.maxGrandParentOverlapBytes());

        assertFalse(spyCompaction.isTrivialMove());
    }

    @Test
    public void testAddInputDeletion() {
        VersionEdit edit = new VersionEdit();
        assertEquals(0, edit.deletedFiles.size());

        Compaction compaction = new Compaction(new Options(), 1);
        compaction.inputs[0].add(new FileMetaData(0L, 0L, null, null));
        compaction.inputs[1].add(new FileMetaData(1L, 0L, null, null));
        compaction.inputs[1].add(new FileMetaData(2L, 0L, null, null));
        compaction.addInputDeletions(edit);

        assertEquals(3, edit.deletedFiles.size());
    }

    @Test
    public void testIsBaseLevelForKeyFalseCase() {
        VersionSet versionSet = new VersionSet();
        versionSet.setInternalKeyComparator(new InternalKeyComparator(new BytewiseComparator()));

        Version version = new Version(versionSet);
        version.files.get(1 + 4).add(new FileMetaData(1L, 0L,
                new InternalKey("a", 1L, ValueType.kTypeValue),
                new InternalKey("c", 2L, ValueType.kTypeValue)));

        Compaction compaction = new Compaction(new Options(), 1);
        compaction.inputVersion = version;

        assertFalse(compaction.isBaseLevelForKey("b"));
    }

    @Test
    public void testIsBaseLevelForKeyTrueCase1() {
        VersionSet versionSet = new VersionSet();
        versionSet.setInternalKeyComparator(new InternalKeyComparator(new BytewiseComparator()));

        Version version = new Version(versionSet);

        Compaction compaction = new Compaction(new Options(), 1);
        compaction.inputVersion = version;

        assertTrue(compaction.isBaseLevelForKey("a"));
    }

    @Test
    public void testIsBaseLevelForKeyTrueCase2() {
        VersionSet versionSet = new VersionSet();
        versionSet.setInternalKeyComparator(new InternalKeyComparator(new BytewiseComparator()));

        Version version = new Version(versionSet);
        version.files.get(1 + 4).add(new FileMetaData(1L, 0L,
                new InternalKey("b", 1L, ValueType.kTypeValue),
                new InternalKey("c", 2L, ValueType.kTypeValue)));

        Compaction compaction = new Compaction(new Options(), 1);
        compaction.inputVersion = version;

        assertTrue(compaction.isBaseLevelForKey("a"));
    }

    @Test
    public void testIsBaseLevelForKeyTrueCase3() {
        VersionSet versionSet = new VersionSet();
        versionSet.setInternalKeyComparator(new InternalKeyComparator(new BytewiseComparator()));

        Version version = new Version(versionSet);
        version.files.get(1 + 4).add(new FileMetaData(1L, 0L,
                new InternalKey("a", 1L, ValueType.kTypeValue),
                new InternalKey("c", 2L, ValueType.kTypeValue)));

        Compaction compaction = new Compaction(new Options(), 1);
        compaction.inputVersion = version;

        assertTrue(compaction.isBaseLevelForKey("d"));
        assertTrue(compaction.isBaseLevelForKey("e"));
    }

    @Test
    public void testShouldShopBefore() {
        VersionSet versionSet = new VersionSet();
        versionSet.setInternalKeyComparator(new InternalKeyComparator(new BytewiseComparator()));
        versionSet.setOptions(new Options());

        Version version = new Version(versionSet);

        Compaction compaction = new Compaction(new Options(), 0);
        compaction.inputVersion = version;

        Compaction spyCompaction = spy(compaction);

        spyCompaction.grandparents.add(new FileMetaData(1L, 10L,
                new InternalKey("a", 1L, ValueType.kTypeValue),
                new InternalKey("b", 2L, ValueType.kTypeValue)));
        spyCompaction.grandparents.add(new FileMetaData(2L, 10L,
                new InternalKey("c", 3L, ValueType.kTypeValue),
                new InternalKey("d", 4L, ValueType.kTypeValue)));
        spyCompaction.grandparents.add(new FileMetaData(3L, 10L,
                new InternalKey("e", 5L, ValueType.kTypeValue),
                new InternalKey("f", 6L, ValueType.kTypeValue)));

        assertFalse(spyCompaction.seenKey);
        assertEquals(0, spyCompaction.overlappedBytes);
        assertEquals(0, spyCompaction.grandparentIndex);
        assertFalse(spyCompaction.shouldStopBefore(new InternalKey("c", 3L, ValueType.kTypeValue)));
        assertTrue(spyCompaction.seenKey);
        assertEquals(0, spyCompaction.overlappedBytes);
        assertEquals(1, spyCompaction.grandparentIndex);

        assertFalse(spyCompaction.shouldStopBefore(new InternalKey("c", 4L, ValueType.kTypeValue)));
        assertEquals(0, spyCompaction.overlappedBytes);
        assertEquals(1, spyCompaction.grandparentIndex);

        assertFalse(spyCompaction.shouldStopBefore(new InternalKey("e", 5L, ValueType.kTypeValue)));
        assertEquals(10, spyCompaction.overlappedBytes);
        assertEquals(2, spyCompaction.grandparentIndex);

        // state not change if invoke multiple times
        assertFalse(spyCompaction.shouldStopBefore(new InternalKey("e", 5L, ValueType.kTypeValue)));
        assertEquals(10, spyCompaction.overlappedBytes);
        assertEquals(2, spyCompaction.grandparentIndex);

        doReturn(9L).when(spyCompaction).maxGrandParentOverlapBytes();
        assertTrue(spyCompaction.shouldStopBefore(new InternalKey("e", 5L, ValueType.kTypeValue)));
        assertEquals(0, spyCompaction.overlappedBytes);
        assertEquals(2, spyCompaction.grandparentIndex);
    }

    @Test
    public void testReleaseInputs() {
        Options options = new Options();
        VersionSet versionSet = new VersionSet("", options, new TableCache("", options, 1024), new InternalKeyComparator(new BytewiseComparator()));
        Version version = new Version(versionSet);
        Compaction compaction = new Compaction(options, 0);
        compaction.inputVersion = version;

        version.ref();
        version.ref();
        version.ref();

        int refCount = version.refs;

        assertNotNull(compaction.inputVersion);
        compaction.releaseInputs();
        assertNull(compaction.inputVersion);
        assertEquals(refCount, version.refs + 1);
    }
}