package com.farmerworking.leveldb.in.java.data.structure.memory;

import com.farmerworking.leveldb.in.java.data.structure.BytewiseComparator;
import org.junit.Test;

import static org.junit.Assert.*;

public class InternalKeyComparatorTest {
    private static InternalKeyComparator comparator = new InternalKeyComparator(new BytewiseComparator());

    static InternalKey IKey(String userKey, long seq, ValueType vt) {
        return new InternalKey(userKey, seq, vt);
    }

    static InternalKey Shorten(InternalKey s, InternalKey l) {
        return comparator.findShortestSeparator(s, l);
    }

    static InternalKey ShortSuccessor(InternalKey s) {
        return comparator.findShortSuccessor(s);
    }

    @Test
    public void testCompare() {
        // equal
        assertEquals(0, comparator.compare(
                IKey("a", 1L, ValueType.kTypeValue),
                IKey("a", 1L, ValueType.kTypeValue)));
        assertEquals(0, comparator.compare(
                IKey("a", 1L, ValueType.kTypeDeletion),
                IKey("a", 1L, ValueType.kTypeDeletion)));

        // userKey
        assertTrue( comparator.compare(
                IKey("b", 1L, ValueType.kTypeValue),
                IKey("a", 1L, ValueType.kTypeValue)) > 0);

        // sequence
        assertTrue( comparator.compare(
                IKey("a", 1L, ValueType.kTypeValue),
                IKey("a", 2L, ValueType.kTypeValue)) > 0);
    }

    @Test
    public void testInternalKeyShortestSuccessor() throws Exception {
        assertEquals(IKey("g", InternalKey.kMaxSequenceNumber, ValueType.kTypeValue),
                ShortSuccessor(IKey("foo", 100, ValueType.kTypeValue)));
        assertEquals(IKey(new String(new char[]{Character.MAX_VALUE, Character.MAX_VALUE}), 100, ValueType.kTypeValue),
                ShortSuccessor(IKey(new String(new char[]{Character.MAX_VALUE, Character.MAX_VALUE}), 100, ValueType.kTypeValue)));
    }

    @Test
    public void testInternalKeyShortSeparator() throws Exception {
        // When user keys are same
        assertEquals(IKey("foo", 100, ValueType.kTypeValue),
                Shorten(IKey("foo", 100, ValueType.kTypeValue),
                        IKey("foo", 99, ValueType.kTypeValue)));
        assertEquals(IKey("foo", 100, ValueType.kTypeValue),
                Shorten(IKey("foo", 100, ValueType.kTypeValue),
                        IKey("foo", 101, ValueType.kTypeValue)));
        assertEquals(IKey("foo", 100, ValueType.kTypeValue),
                Shorten(IKey("foo", 100, ValueType.kTypeValue),
                        IKey("foo", 100, ValueType.kTypeValue)));
        assertEquals(IKey("foo", 100, ValueType.kTypeValue),
                Shorten(IKey("foo", 100, ValueType.kTypeValue),
                        IKey("foo", 100, ValueType.kTypeDeletion)));

        // When user keys are misordered
        assertEquals(IKey("foo", 100, ValueType.kTypeValue),
                Shorten(IKey("foo", 100, ValueType.kTypeValue),
                        IKey("bar", 99, ValueType.kTypeValue)));

        // When user keys are different, but correctly ordered
        assertEquals(IKey("g", InternalKey.kMaxSequenceNumber, ValueType.kTypeValue),
                Shorten(IKey("foo", 100, ValueType.kTypeValue),
                        IKey("hello", 200, ValueType.kTypeValue)));

        // When start user key is prefix of limit user key
        assertEquals(IKey("foo", 100, ValueType.kTypeValue),
                Shorten(IKey("foo", 100, ValueType.kTypeValue),
                        IKey("foobar", 200, ValueType.kTypeValue)));

        // When limit user key is prefix of start user key
        assertEquals(IKey("foobar", 100, ValueType.kTypeValue),
                Shorten(IKey("foobar", 100, ValueType.kTypeValue),
                        IKey("foo", 200, ValueType.kTypeValue)));
    }
}