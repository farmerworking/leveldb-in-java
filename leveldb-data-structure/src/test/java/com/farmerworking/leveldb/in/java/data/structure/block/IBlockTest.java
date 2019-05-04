package com.farmerworking.leveldb.in.java.data.structure.block;

import com.farmerworking.leveldb.in.java.api.BytewiseComparator;
import com.farmerworking.leveldb.in.java.api.Options;
import com.farmerworking.leveldb.in.java.common.ICoding;
import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import com.farmerworking.leveldb.in.java.api.Iterator;

public abstract class IBlockTest {
    private ICoding coding;

    @Before
    public void setUp() throws Exception {
        coding = getCodingImpl();
    }

    @Test(expected = AssertionError.class)
    public void testAddAfterFinish() throws Exception {
        Options options = new Options();
        IBlockBuilder builder = getBlockBuilder(options);
        builder.finish();
        builder.add("", "");
    }

    @Test(expected = AssertionError.class)
    public void testAddErrorKey() throws Exception {
        Options options = new Options();
        IBlockBuilder builder = getBlockBuilder(options);
        builder.add("2", "two");
        builder.add("1", "two");
    }

    @Test
    public void testSizeEstimate() {
        Options options = new Options();
        IBlockBuilder builder = getBlockBuilder(options);
        int emptySize = builder.memoryUsage();

        builder.add("one", "two");
        int oneSize = builder.memoryUsage();
        assertTrue(oneSize > emptySize);

        builder.add("two", "three");
        int twoSize = builder.memoryUsage();
        assertTrue(twoSize > oneSize);

        builder.add("txree", "four");
        int threeSize = builder.memoryUsage();
        assertTrue(threeSize > twoSize);
    }

    @Test
    public void testMalformBlock() {
        Options options = new Options();
        IBlockReader block = getBlockReader("");
        Iterator iter = block.iterator(options.getComparator());
        assertTrue(iter.status().IsCorruption());
    }

    @Test
    public void testMalformBlock2() {
        char[] buffer = new char[4];
        coding.encodeFixed32(buffer, 0, 1);
        Options options = new Options();
        IBlockReader block = getBlockReader(new String(buffer));
        Iterator iter = block.iterator(options.getComparator());
        assertTrue(iter.status().IsCorruption());
    }

    @Test
    public void testZeroRestarts() {
        char[] buffer = new char[4];
        Options options = new Options();
        coding.encodeFixed32(buffer, 0, 0);
        IBlockReader block = getBlockReader(new String(buffer));
        Iterator iter = block.iterator(options.getComparator());
        assertTrue(iter.status().isOk());
        assertFalse(iter.valid());

        iter.seekToFirst();
        assertFalse(iter.valid());
    }

    @Test
    public void testIterNext() throws Exception {
        Options options = new Options();
        IBlockBuilder builder = getBlockBuilder(options);
        for (int i = 0; i < options.getBlockRestartInterval() * 10; i++) {
            builder.add("test" + String.valueOf((char)i), String.valueOf(i));
        }

        String content = builder.finish();
        IBlockReader block = getBlockReader(content);
        Iterator iterator = block.iterator(options.getComparator());

        iterator.seekToFirst();
        for (int i = 0; i < options.getBlockRestartInterval() * 10; i++) {
            assertTrue(iterator.valid());
            assertEquals("test" + String.valueOf((char)i), iterator.key());
            assertEquals(String.valueOf(i), iterator.value());
            iterator.next();
        }
    }

    @Test
    public void testIterPrev() throws Exception {
        Options options = new Options();
        IBlockBuilder builder = getBlockBuilder(options);
        for (int i = 0; i < options.getBlockRestartInterval() * 10; i++) {
            builder.add("test" + String.valueOf((char)i), String.valueOf(i));
        }

        String content = builder.finish();
        IBlockReader block = getBlockReader(content);
        Iterator iterator = block.iterator(options.getComparator());

        iterator.seekToLast();
        for (int i = options.getBlockRestartInterval() * 10 - 1; i >= 0; i--) {
            assertTrue(iterator.valid());
            assertEquals("test" + String.valueOf((char)i), iterator.key());
            assertEquals(String.valueOf(i), iterator.value());
            iterator.prev();
        }
    }

    @Test
    public void testIterSeekNext() throws Exception {
        Options options = new Options();
        IBlockBuilder builder = getBlockBuilder(options);
        for (int i = 0; i < options.getBlockRestartInterval() * 10; i++) {
            builder.add("test" + String.valueOf((char)i), String.valueOf(i));
        }

        String content = builder.finish();
        IBlockReader block = getBlockReader(content);
        Iterator iterator = block.iterator(options.getComparator());

        iterator.seek("test" + String.valueOf((char)100));
        for (int i = 100; i < options.getBlockRestartInterval() * 10; i++) {
            assertTrue(iterator.valid());
            assertEquals("test" + String.valueOf((char)i), iterator.key());
            assertEquals(String.valueOf(i), iterator.value());
            iterator.next();
        }
    }

    @Test
    public void testIterSeekPrev() throws Exception {
        Options options = new Options();
        IBlockBuilder builder = getBlockBuilder(options);
        for (int i = 0; i < options.getBlockRestartInterval() * 10; i++) {
            builder.add("test" + String.valueOf((char)i), String.valueOf(i));
        }

        String content = builder.finish();
        IBlockReader block = getBlockReader(content);
        Iterator iterator = block.iterator(options.getComparator());

        iterator.seek("test" + String.valueOf((char)100));
        for (int i = 100; i >= 0; i--) {
            assertTrue(iterator.valid());
            assertEquals("test" + String.valueOf((char)i), iterator.key());
            assertEquals(String.valueOf(i), iterator.value());
            iterator.prev();
        }
    }

    @Test
    public void testNextPrev() throws Exception {
        Options options = new Options();
        IBlockBuilder builder = getBlockBuilder(options);
        for (int i = 0; i < options.getBlockRestartInterval() * 10; i++) {
            builder.add("test" + String.valueOf((char)i), String.valueOf(i));
        }

        String content = builder.finish();
        IBlockReader block = getBlockReader(content);
        Iterator iterator = block.iterator(options.getComparator());

        iterator.seek("test" + String.valueOf((char)100));

        assertTrue(iterator.valid());
        assertEquals("test" + String.valueOf((char)100), iterator.key());
        assertEquals("100", iterator.value());
        iterator.next();

        assertTrue(iterator.valid());
        assertEquals("test" + String.valueOf((char)101), iterator.key());
        assertEquals("101", iterator.value());
        iterator.next();

        assertTrue(iterator.valid());
        assertEquals("test" + String.valueOf((char)102), iterator.key());
        assertEquals("102", iterator.value());
        iterator.prev();

        assertTrue(iterator.valid());
        assertEquals("test" + String.valueOf((char)101), iterator.key());
        assertEquals("101", iterator.value());
        iterator.prev();

        assertTrue(iterator.valid());
        assertEquals("test" + String.valueOf((char)100), iterator.key());
        assertEquals("100", iterator.value());
        iterator.prev();

        assertTrue(iterator.valid());
        assertEquals("test" + String.valueOf((char)99), iterator.key());
        assertEquals("99", iterator.value());
    }

    @Test
    public void testEmpty() throws Exception {
        Options options = new Options();
        IBlockBuilder builder = getBlockBuilder(options);
        String content = builder.finish();
        IBlockReader block = getBlockReader(content);
        Iterator iterator = block.iterator(options.getComparator());
        iterator.seekToFirst();
        assertFalse(iterator.valid());
        assertTrue(iterator.status().isOk());
    }

    @Test
    public void testZeroRestartPointsInBlock() {
        String contents = StringUtils.repeat((char)0, 4);
        IBlockReader block = IBlockReader.getDefaultImpl(contents);
        Iterator iter = block.iterator(new BytewiseComparator());
        iter.seekToFirst();
        assertTrue(!iter.valid());
        iter.seekToLast();
        assertTrue(!iter.valid());
        iter.seek("foo");
        assertTrue(!iter.valid());
    }

    protected abstract IBlockBuilder getBlockBuilder(Options options);

    protected abstract IBlockReader getBlockReader(String content);

    protected abstract ICoding getCodingImpl();
}

