package com.farmerworking.leveldb.in.java.data.structure.memory;

import com.farmerworking.leveldb.in.java.api.BytewiseComparator;
import org.junit.Test;
import static org.junit.Assert.*;

public class FormatTest {
    InternalKeyComparator comparator = new InternalKeyComparator(new BytewiseComparator());

    @Test
    public void testInternalKeyShortSeparator() {
        assertEquals(new InternalKey("foo", 100, ValueType.kTypeValue),
                Shorten(new InternalKey("foo", 100, ValueType.kTypeValue),
                        new InternalKey("foo", 99, ValueType.kTypeValue)));
        assertEquals(new InternalKey("foo", 100, ValueType.kTypeValue),
                Shorten(new InternalKey("foo", 100, ValueType.kTypeValue),
                        new InternalKey("foo", 101, ValueType.kTypeValue)));
        assertEquals(new InternalKey("foo", 100, ValueType.kTypeValue),
                Shorten(new InternalKey("foo", 100, ValueType.kTypeValue),
                        new InternalKey("foo", 100, ValueType.kTypeValue)));
        assertEquals(new InternalKey("foo", 100, ValueType.kTypeValue),
                Shorten(new InternalKey("foo", 100, ValueType.kTypeValue),
                        new InternalKey("foo", 100, ValueType.kTypeDeletion)));

        // When user keys are misordered
        assertEquals(new InternalKey("foo", 100, ValueType.kTypeValue),
                Shorten(new InternalKey("foo", 100, ValueType.kTypeValue),
                        new InternalKey("bar", 99, ValueType.kTypeValue)));

        // When user keys are different, but correctly ordered
        assertEquals(new InternalKey("g", InternalKey.kMaxSequenceNumber, ValueType.kTypeValue),
                Shorten(new InternalKey("foo", 100, ValueType.kTypeValue),
                        new InternalKey("hello", 200, ValueType.kTypeValue)));

        // When start user key is prefix of limit user key
        assertEquals(new InternalKey("foo", 100, ValueType.kTypeValue),
                Shorten(new InternalKey("foo", 100, ValueType.kTypeValue),
                        new InternalKey("foobar", 200, ValueType.kTypeValue)));

        // When limit user key is prefix of start user key
        assertEquals(new InternalKey("foobar", 100, ValueType.kTypeValue),
                Shorten(new InternalKey("foobar", 100, ValueType.kTypeValue),
                        new InternalKey("foo", 200, ValueType.kTypeValue)));
    }

    @Test
    public void testInternalKeyShortestSuccessor() {
        assertEquals(new InternalKey("g", InternalKey.kMaxSequenceNumber, ValueType.kTypeValue),
                comparator.findShortSuccessor(new InternalKey("foo", 100, ValueType.kTypeValue)));
        assertEquals(new InternalKey(new char[]{255, 255}, 100, ValueType.kTypeValue),
                comparator.findShortSuccessor(new InternalKey(new char[]{255, 255}, 100, ValueType.kTypeValue)));
    }

    private InternalKey Shorten(InternalKey a, InternalKey b) {
        return comparator.findShortestSeparator(a, b);
    }
}
