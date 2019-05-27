package com.farmerworking.leveldb.in.java.data.structure.version;

import com.farmerworking.leveldb.in.java.api.BytewiseComparator;
import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKey;
import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKeyComparator;
import com.farmerworking.leveldb.in.java.data.structure.memory.ValueType;
import org.junit.Before;
import org.junit.Test;

import java.util.Vector;

import static org.junit.Assert.*;

public class FileRangeHelperTest {
    private InternalKeyComparator comparator;
    private FileRangeHelper fileRangeHelper;
    private boolean disjointSortedFiles;
    private Vector<FileMetaData> files;

    private void Add(String smallest, String largest) {
        Add(smallest, largest, 100L, 100L);
    }

    private void Add(String smallest, String largest, long smallSequence, long largeSequence) {
        FileMetaData metaData = new FileMetaData(
                files.size() + 1,
                0,
                new InternalKey(smallest, smallSequence, ValueType.kTypeValue),
                new InternalKey(largest, largeSequence, ValueType.kTypeValue)
        );

        files.add(metaData);
    }

    private int Find(String key) {
        return fileRangeHelper.findFile(comparator, files, new InternalKey(key, 100, ValueType.kTypeValue).encode());
    }

    private boolean Overlaps(String smallest, String largest) {
        String s = (smallest != null ? smallest : "");
        String l = (largest != null ? largest : "");
        return fileRangeHelper.isSomeFileOverlapsRange(comparator, disjointSortedFiles, files, smallest, largest);
    }

    @Before
    public void setUp() throws Exception {
        disjointSortedFiles = true;
        files = new Vector<>();
        fileRangeHelper = new FileRangeHelper();
        comparator = new InternalKeyComparator(new BytewiseComparator());
    }

    @Test
    public void testEmpty() {
        assertEquals(0, Find("foo"));
        assertFalse(Overlaps("a", "z"));
        assertFalse(Overlaps(null, "z"));
        assertFalse(Overlaps("a", null));
        assertFalse(Overlaps(null, null));
    }

    @Test
    public void testSingle() {
        Add("p", "q");
        assertEquals(0, Find("a"));
        assertEquals(0, Find("p"));
        assertEquals(0, Find("p1"));
        assertEquals(0, Find("q"));
        assertEquals(1, Find("q1"));
        assertEquals(1, Find("z"));

        assertFalse(Overlaps("a", "b"));
        assertFalse(Overlaps("z1", "z2"));
        assertTrue(Overlaps("a", "p"));
        assertTrue(Overlaps("a", "q"));
        assertTrue(Overlaps("a", "z"));
        assertTrue(Overlaps("p", "p1"));
        assertTrue(Overlaps("p", "q"));
        assertTrue(Overlaps("p", "z"));
        assertTrue(Overlaps("p1", "p2"));
        assertTrue(Overlaps("p1", "z"));
        assertTrue(Overlaps("q", "q"));
        assertTrue(Overlaps("q", "q1"));

        assertFalse(Overlaps(null, "j"));
        assertFalse(Overlaps("r", null));
        assertTrue(Overlaps(null, "p"));
        assertTrue(Overlaps(null, "p1"));
        assertTrue(Overlaps("q", null));
        assertTrue(Overlaps(null, null));
    }

    @Test
    public void testMultiple() {
        Add("150", "200");
        Add("200", "250");
        Add("300", "350");
        Add("400", "450");
        assertEquals(0, Find("100"));
        assertEquals(0, Find("150"));
        assertEquals(0, Find("151"));
        assertEquals(0, Find("199"));
        assertEquals(0, Find("200"));
        assertEquals(1, Find("201"));
        assertEquals(1, Find("249"));
        assertEquals(1, Find("250"));
        assertEquals(2, Find("251"));
        assertEquals(2, Find("299"));
        assertEquals(2, Find("300"));
        assertEquals(2, Find("349"));
        assertEquals(2, Find("350"));
        assertEquals(3, Find("351"));
        assertEquals(3, Find("400"));
        assertEquals(3, Find("450"));
        assertEquals(4, Find("451"));

        assertFalse(Overlaps("100", "149"));
        assertFalse(Overlaps("251", "299"));
        assertFalse(Overlaps("451", "500"));
        assertFalse(Overlaps("351", "399"));

        assertTrue(Overlaps("100", "150"));
        assertTrue(Overlaps("100", "200"));
        assertTrue(Overlaps("100", "300"));
        assertTrue(Overlaps("100", "400"));
        assertTrue(Overlaps("100", "500"));
        assertTrue(Overlaps("375", "400"));
        assertTrue(Overlaps("450", "450"));
        assertTrue(Overlaps("450", "500"));
    }

    @Test
    public void testMultipleNullBoundaries() {
        Add("150", "200");
        Add("200", "250");
        Add("300", "350");
        Add("400", "450");
        assertFalse(Overlaps(null, "149"));
        assertFalse(Overlaps("451", null));
        assertTrue(Overlaps(null, null));
        assertTrue(Overlaps(null, "150"));
        assertTrue(Overlaps(null, "199"));
        assertTrue(Overlaps(null, "200"));
        assertTrue(Overlaps(null, "201"));
        assertTrue(Overlaps(null, "400"));
        assertTrue(Overlaps(null, "800"));
        assertTrue(Overlaps("100", null));
        assertTrue(Overlaps("200", null));
        assertTrue(Overlaps("449", null));
        assertTrue(Overlaps("450", null));
    }

    @Test
    public void testOverlapSequenceChecks() {
        Add("200", "200", 5000, 3000);
        assertFalse(Overlaps("199", "199"));
        assertFalse(Overlaps("201", "300"));
        assertTrue(Overlaps("200", "200"));
        assertTrue(Overlaps("190", "200"));
        assertTrue(Overlaps("200", "210"));
    }

    @Test
    public void testOverlappingFiles() {
        Add("150", "600");
        Add("400", "500");
        disjointSortedFiles = false;
        assertFalse(Overlaps("100", "149"));
        assertFalse(Overlaps("601", "700"));
        assertTrue(Overlaps("100", "150"));
        assertTrue(Overlaps("100", "200"));
        assertTrue(Overlaps("100", "300"));
        assertTrue(Overlaps("100", "400"));
        assertTrue(Overlaps("100", "500"));
        assertTrue(Overlaps("375", "400"));
        assertTrue(Overlaps("450", "450"));
        assertTrue(Overlaps("450", "500"));
        assertTrue(Overlaps("450", "700"));
        assertTrue(Overlaps("600", "700"));
    }
}