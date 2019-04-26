package com.farmerworking.leveldb.in.java.data.structure;

import com.farmerworking.leveldb.in.java.api.BytewiseComparator;
import org.junit.Test;

import static org.junit.Assert.*;

public class BytewiseComparatorTest {
    @Test
    public void testCompare() throws Exception {
        BytewiseComparator comparator = new BytewiseComparator();
        assertEquals(0, comparator.compare(new char[]{BytewiseComparator.UNSIGNED_CHAR_MAX_VALUE}, new char[]{BytewiseComparator.UNSIGNED_CHAR_MAX_VALUE}));
        assertTrue(comparator.compare(new char[]{BytewiseComparator.UNSIGNED_CHAR_MIN_VALUE}, new char[]{BytewiseComparator.UNSIGNED_CHAR_MAX_VALUE}) < 0);
        assertTrue(comparator.compare(new char[]{BytewiseComparator.UNSIGNED_CHAR_MAX_VALUE}, new char[]{BytewiseComparator.UNSIGNED_CHAR_MIN_VALUE}) > 0);
    }

    @Test
    public void testFindShortSuccessor() throws Exception {
        BytewiseComparator comparator = new BytewiseComparator();
        assertArrayEquals(new char[]{'b'}, comparator.findShortSuccessor(new char[]{'a', 'b', 'c', 'd', 'e'}));

        assertArrayEquals(new char[]{BytewiseComparator.UNSIGNED_CHAR_MAX_VALUE}, comparator.findShortSuccessor(new char[]{BytewiseComparator.UNSIGNED_CHAR_MAX_VALUE}));
        assertArrayEquals(new char[]{(char) (BytewiseComparator.UNSIGNED_CHAR_MAX_VALUE + 1)}, comparator.findShortSuccessor(new char[]{(char) (BytewiseComparator.UNSIGNED_CHAR_MAX_VALUE + 1)}));
        assertArrayEquals(new char[]{1}, comparator.findShortSuccessor(new char[]{BytewiseComparator.UNSIGNED_CHAR_MIN_VALUE}));
        assertArrayEquals(new char[]{Character.MAX_VALUE}, comparator.findShortSuccessor(new char[]{Character.MAX_VALUE}));
        assertArrayEquals(new char[]{Character.MAX_VALUE, BytewiseComparator.UNSIGNED_CHAR_MAX_VALUE, 'b'},
                comparator.findShortSuccessor(new char[]{Character.MAX_VALUE, BytewiseComparator.UNSIGNED_CHAR_MAX_VALUE, 'a', 'b'}));
    }

    @Test
    public void testFindShortestSeparator() throws Exception {
        BytewiseComparator comparator = new BytewiseComparator();

        // prefix
        assertArrayEquals(new char[]{'a', 'b', 'c', 'd', 'e', 'f'},
                comparator.findShortestSeparator(new char[]{'a', 'b', 'c', 'd', 'e', 'f'}, new char[]{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k'}));

        assertArrayEquals(new char[]{'a', 'b', 'c', 'd', 'e', 'f'},
                comparator.findShortestSeparator(new char[]{'a', 'b', 'c', 'd', 'e', 'f'}, new char[]{'a', 'b', 'c'}));

        // normal case
        assertArrayEquals(new char[]{'a', 'b', 'd'}, comparator.findShortestSeparator(new char[]{'a', 'b', 'c', 'd', 'e', 'f'}, new char[]{'a', 'b', 'g', 'h', 'i', 'j', 'k'}));
        assertArrayEquals(new char[]{'y'}, comparator.findShortestSeparator(new char[]{'x', 'c', 'v', 'm', 'n', 'z'}, new char[]{'z'}));
        assertArrayEquals(new char[]{'a', 'b', 'c', 'd', 'e', 'f'}, comparator.findShortestSeparator(new char[]{'a', 'b', 'c', 'd', 'e', 'f'}, new char[]{'a', 'b', 'd', 'x', 'c', 'v', 'z', 'x', 'c', 'v'}));

        assertArrayEquals(new char[]{'a', Character.MIN_VALUE, Character.MAX_VALUE, BytewiseComparator.UNSIGNED_CHAR_MAX_VALUE, 'x', 'c', 'v', 'z', 'x', 'c'},
                comparator.findShortestSeparator(new char[]{'a', Character.MIN_VALUE, Character.MAX_VALUE, BytewiseComparator.UNSIGNED_CHAR_MAX_VALUE, 'x', 'c', 'v', 'z', 'x', 'c'} , new char[]{'a', Character.MIN_VALUE, Character.MAX_VALUE, 'g'}));
    }
}