package com.farmerworking.leveldb.in.java.data.structure;

import javafx.util.Pair;
import org.junit.Test;

import java.util.Vector;

import static org.junit.Assert.*;

public abstract class ICodingTest {
    protected abstract ICoding getImpl();

    @Test
    public void testFixed32() throws Exception {
        StringBuilder s = new StringBuilder();
        for (int v = 0; v < 100000; v++) {
            getImpl().putFixed32(s, v);
        }
        getImpl().putFixed32(s, Integer.MAX_VALUE);
        getImpl().putFixed32(s, Integer.MIN_VALUE);

        char[] p = s.toString().toCharArray();
        int offset = 0;
        for (int v = 0; v < 100000; v++) {
            assertEquals(v, getImpl().decodeFixed32(p, offset));
            offset += 4;
        }
        assertEquals(Integer.MAX_VALUE, getImpl().decodeFixed32(p, offset));
        offset += 4;
        assertEquals(Integer.MIN_VALUE, getImpl().decodeFixed32(p, offset));
    }

    @Test(expected = AssertionError.class)
    public void testFixed32EncodeWithSmallBuffer() {
        getImpl().encodeFixed32(new char[3], 0, Integer.MIN_VALUE);
    }

    @Test(expected = AssertionError.class)
    public void testFixed32decodeWithSmallBuffer() {
        getImpl().decodeFixed32(new char[3], 0);
    }

    @Test
    public void testFixed64() throws Exception {
        StringBuilder s = new StringBuilder();
        for (int power = 0; power <= 63; power++) {
            long v = 1 << power;
            getImpl().putFixed64(s, v - 1);
            getImpl().putFixed64(s, v);
            getImpl().putFixed64(s, v + 1);
        }
        getImpl().putFixed64(s, Long.MAX_VALUE);
        getImpl().putFixed64(s, Long.MIN_VALUE);

        char[] p = s.toString().toCharArray();
        int offset = 0;
        for (int power = 0; power <= 63; power++) {
            long v = 1 << power;
            assertEquals(v - 1, getImpl().decodeFixed64(p, offset));
            offset += 8;

            assertEquals(v, getImpl().decodeFixed64(p, offset));
            offset += 8;

            assertEquals(v + 1, getImpl().decodeFixed64(p, offset));
            offset += 8;
        }
        assertEquals(Long.MAX_VALUE, getImpl().decodeFixed64(p, offset));
        offset += 8;
        assertEquals(Long.MIN_VALUE, getImpl().decodeFixed64(p, offset));
    }

    @Test(expected = AssertionError.class)
    public void testFixed64EncodeWithSmallBuffer() {
        getImpl().encodeFixed64(new char[7], 0, Integer.MIN_VALUE);
    }

    @Test(expected = AssertionError.class)
    public void testFixed64decodeWithSmallBuffer() {
        getImpl().decodeFixed64(new char[7], 0);
    }

    @Test
    public void testEncodingOutputIsLittleEndianEncoding() throws Exception {
        StringBuilder dst = new StringBuilder();
        getImpl().putFixed32(dst, 0x04030201);
        char[] chars = dst.toString().toCharArray();
        assertEquals(4, chars.length);
        assertEquals(0x01, chars[0]);
        assertEquals(0x02, chars[1]);
        assertEquals(0x03, chars[2]);
        assertEquals(0x04, chars[3]);

        dst = new StringBuilder();
        getImpl().putFixed64(dst, 0x0807060504030201L);
        chars = dst.toString().toCharArray();
        assertEquals(8, chars.length);
        assertEquals(0x01, chars[0]);
        assertEquals(0x02, chars[1]);
        assertEquals(0x03, chars[2]);
        assertEquals(0x04, chars[3]);
        assertEquals(0x05, chars[4]);
        assertEquals(0x06, chars[5]);
        assertEquals(0x07, chars[6]);
        assertEquals(0x08, chars[7]);
    }
}