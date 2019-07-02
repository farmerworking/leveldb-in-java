package com.farmerworking.leveldb.in.java.data.structure.version;

import com.farmerworking.leveldb.in.java.api.BytewiseComparator;
import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKey;
import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKeyComparator;
import com.farmerworking.leveldb.in.java.data.structure.memory.ValueType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class SmallestKeyComparatorTest {
    @Test
    public void test1() {
        SmallestKeyComparator comparator = new SmallestKeyComparator(
                new InternalKeyComparator(new BytewiseComparator()));

        List<FileMetaData> lists = new ArrayList<>();
        lists.add(new FileMetaData(1L, 0L, new InternalKey("z", 1L, ValueType.kTypeValue), null));
        lists.add(new FileMetaData(2L, 0L, new InternalKey("x", 2L, ValueType.kTypeValue), null));
        lists.add(new FileMetaData(3L, 0L, new InternalKey("x", 3L, ValueType.kTypeValue), null));

        Collections.sort(lists, comparator);

        assertEquals(3L, lists.get(0).getFileNumber());
        assertEquals(2L, lists.get(1).getFileNumber());
        assertEquals(1L, lists.get(2).getFileNumber());
    }
}