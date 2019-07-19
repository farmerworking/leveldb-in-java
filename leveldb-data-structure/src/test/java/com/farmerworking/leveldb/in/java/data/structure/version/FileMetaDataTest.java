package com.farmerworking.leveldb.in.java.data.structure.version;

import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKey;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class FileMetaDataTest {
    FileMetaData metaData1;
    FileMetaData metaData2;

    @Before
    public void setUp() throws Exception {
        metaData1 = new FileMetaData(1L, 1000L,
                new InternalKey("a", 1L),
                new InternalKey("g", 1L));
        metaData1.setRefs(1);
        metaData1.setAllowedSeeks(1);

        metaData2 = new FileMetaData(1L, 1000L,
                new InternalKey("a", 1L),
                new InternalKey("g", 1L));
        metaData2.setRefs(2);
        metaData2.setAllowedSeeks(2);
    }

    @Test
    public void testHash() {
        assertEquals(metaData1.hashCode(), metaData2.hashCode());
    }

    @Test
    public void testEqual() {
        assertEquals(metaData1, metaData2);
    }
}