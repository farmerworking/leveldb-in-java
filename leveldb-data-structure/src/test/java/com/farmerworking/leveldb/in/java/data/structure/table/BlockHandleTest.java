package com.farmerworking.leveldb.in.java.data.structure.table;

import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.common.ICoding;
import javafx.util.Pair;
import org.junit.Test;

import static org.junit.Assert.*;

public class BlockHandleTest {
    @Test(expected = AssertionError.class)
    public void testEncodeWithoutSet() {
        BlockHandle handle = new BlockHandle();
        handle.encodeTo(new StringBuilder());
    }

    @Test
    public void testEncodeDecode() {
        StringBuilder builder = new StringBuilder();
        BlockHandle handle1 = new BlockHandle();
        handle1.setSize(Long.MAX_VALUE);
        handle1.setOffset(Long.MIN_VALUE);
        handle1.encodeTo(builder);

        BlockHandle handle2 = new BlockHandle();
        Pair<Status, Integer> pair = handle2.decodeFrom(builder.toString().toCharArray(), 0);

        assertNotNull(pair);
        assertTrue(pair.getKey().isOk());
        assertEquals(ICoding.getInstance().varintLength(Long.MAX_VALUE) +
                ICoding.getInstance().varintLength(Long.MIN_VALUE), pair.getValue().intValue());
        assertEquals(handle2.getOffset(), handle1.getOffset());
        assertEquals(handle2.getSize(), handle1.getSize());
    }

    @Test
    public void testBadBlockHandle() {
        char[] input = new char[] {0x81, 0x82, 0x83, 0x84, 0x85, 0x81, 0x82, 0x83, 0x84, 0x85, 0x11};
        BlockHandle handle = new BlockHandle();
        Pair<Status, Integer> pair = handle.decodeFrom(input, 0);
        assertNotNull(pair);
        assertFalse(pair.getKey().isOk());
        assertTrue(pair.getKey().isCorruption());
    }
}