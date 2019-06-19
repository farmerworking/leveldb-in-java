package com.farmerworking.leveldb.in.java.data.structure.version;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;

import static org.junit.Assert.*;

public class NewestFileComparatorTest {
    @Test
    public void test() {
        ArrayList<FileMetaData> list = new ArrayList<>();

        FileMetaData first = new FileMetaData(1, 0, null, null);
        FileMetaData second = new FileMetaData(2, 0, null, null);
        FileMetaData third = new FileMetaData(3, 0, null, null);

        list.add(first);
        list.add(second);
        list.add(third);

        Collections.sort(list, new NewestFileComparator());

        assertEquals(third, list.get(0));
        assertEquals(second, list.get(1));
        assertEquals(first, list.get(2));
    }
}