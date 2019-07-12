package com.farmerworking.leveldb.in.java.data.structure.version;

import com.farmerworking.leveldb.in.java.api.BytewiseComparator;
import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKey;
import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKeyComparator;
import com.farmerworking.leveldb.in.java.data.structure.memory.ValueType;
import javafx.util.Pair;
import org.junit.Test;

import static org.junit.Assert.*;

public class VersionBuilderTest {
    @Test
    public void test() {
        VersionSet versionSet = new VersionSet();
        versionSet.setInternalKeyComparator(new InternalKeyComparator(new BytewiseComparator()));

        Version version = new Version(versionSet);
        version.files.get(1).add(new FileMetaData(
                1L, 1L,
                new InternalKey("h", 1L, ValueType.kTypeValue), new InternalKey("i", 1L, ValueType.kTypeValue)));
        version.files.get(1).add(new FileMetaData(
                1L, 1L,
                new InternalKey("p", 1L, ValueType.kTypeValue), new InternalKey("t", 1L, ValueType.kTypeValue)));
        version.files.get(1).add(new FileMetaData(
                1L, 1L,
                new InternalKey("z", 1L, ValueType.kTypeValue), null));

        VersionBuilder builder = new VersionBuilder(versionSet.getInternalKeyComparator(), version);

        VersionEdit edit1 = new VersionEdit();
        edit1.getCompactPointers().add(new Pair<>(0, new InternalKey("a", 1L, ValueType.kTypeValue)));
        edit1.deleteFile(0, 2L);
        edit1.addFile(0, 1L, 16384, new InternalKey("a", 1L, ValueType.kTypeValue), null);
        edit1.addFile(0, 99L, 16384 * 200, new InternalKey("g", 1L, ValueType.kTypeValue), null);

        VersionEdit edit2 = new VersionEdit();
        edit2.getCompactPointers().add(new Pair<>(1, new InternalKey("c", 2L, ValueType.kTypeValue)));
        edit2.deleteFile(1, 3L);
        edit2.addFile(1, 100L, 16384, new InternalKey("n", 1L, ValueType.kTypeValue), new InternalKey("o", 1L, ValueType.kTypeValue));

        VersionEdit edit3 = new VersionEdit();
        edit3.getCompactPointers().add(new Pair<>(1, new InternalKey("g", 3L, ValueType.kTypeValue)));
        edit3.deleteFile(0, 1L);
        edit2.addFile(1, 3L, 16384 * 333, new InternalKey("x", 1L, ValueType.kTypeValue), new InternalKey("y", 1L, ValueType.kTypeValue));

        // compact point
        for (int level = 0; level < Config.kNumLevels; level++) {
            assertNull(versionSet.getCompactPointer()[level]);
        }

        builder.apply(edit1);
        builder.apply(edit2);
        builder.apply(edit3);

        Version result = new Version(versionSet);
        for (int level = 0; level < Config.kNumLevels; level++) {
            assertEquals(0, result.files.get(level).size());
        }
        builder.saveTo(versionSet, result);
        
        // compact point
        assertNotNull(versionSet.getCompactPointer()[0]);
        assertNotNull(versionSet.getCompactPointer()[1]);
        assertEquals('g', versionSet.getCompactPointer()[1].charAt(0));
        for (int level = 2; level < Config.kNumLevels; level++) {
            assertNull(versionSet.getCompactPointer()[level]);
        }

        // level 0
        assertEquals(1, result.files.get(0).size());
        assertEquals(200, result.files.get(0).get(0).getAllowedSeeks());

        // level 1
        assertEquals(5, result.files.get(1).size());
        assertEquals(100, result.files.get(1).get(1).getFileNumber());
        assertEquals(100, result.files.get(1).get(1).getAllowedSeeks());
        assertEquals(3, result.files.get(1).get(3).getFileNumber());
        assertEquals(333, result.files.get(1).get(3).getAllowedSeeks());

        for (int level = 2; level < Config.kNumLevels; level++) {
            assertEquals(0, result.files.get(level).size());
        }
    }
}