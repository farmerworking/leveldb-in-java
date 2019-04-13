package com.farmerworking.leveldb.in.java.data.structure;

import javafx.util.Pair;
import org.apache.commons.lang3.StringUtils;
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

    @Test
    public void testVarint32() throws Exception {
        StringBuilder s = new StringBuilder();
        for (int i = 0; i < 32 * 32; i++) {
            int v = (i / 32) << (i % 32);
            getImpl().putVarint32(s, v);
        }
        getImpl().putVarint32(s, Integer.MAX_VALUE);
        getImpl().putVarint32(s, Integer.MIN_VALUE);

        char[] chars = s.toString().toCharArray();
        int offset = 0;
        for (int i = 0; i < 32 * 32; i++) {
            int expected = ((i / 32) << (i % 32));
            Pair<Integer, Integer> result = getImpl().decodeVarint32(chars, offset);
            assertNotNull(result);
            assertEquals(expected, result.getKey().intValue());
            assertEquals(getImpl().varintLength(result.getKey()), result.getValue() - offset);
            offset = result.getValue();
        }

        Pair<Integer, Integer> result = getImpl().decodeVarint32(chars, offset);
        assertNotNull(result);
        assertEquals(Integer.MAX_VALUE, result.getKey().intValue());
        assertEquals(getImpl().varintLength(result.getKey()), result.getValue() - offset);
        offset = result.getValue();

        result = getImpl().decodeVarint32(chars, offset);
        assertNotNull(result);
        assertEquals(Integer.MIN_VALUE, result.getKey().intValue());
        assertEquals(getImpl().varintLength(result.getKey()), result.getValue() - offset);
        offset = result.getValue();

        assertEquals(offset, chars.length);
    }

    @Test
    public void testVarint64() {
        // Construct the list of values to check
        Vector<Long> values = new Vector<>();
        // Some special values
        values.add(0L);
        values.add(100L);
        values.add(Long.MIN_VALUE);
        values.add(Long.MIN_VALUE + 1);
        values.add(Long.MAX_VALUE);
        values.add(Long.MAX_VALUE - 1);
        for (int k = 0; k < 64; k++) {
            // Test values near powers of two
            long power = 1 << k;
            values.add(power);
            values.add(power - 1);
            values.add(power+1);
        }

        StringBuilder s = new StringBuilder();
        for (int i = 0; i < values.size(); i++) {
            getImpl().putVarint64(s, values.get(i));
        }

        char[] chars = s.toString().toCharArray();
        int index = 0;
        for (int i = 0; i < values.size(); i++) {
            assertTrue(index < s.length());
            Pair<Long, Integer> pair = getImpl().decodeVarint64(chars, index);
            assertNotNull(pair);
            assertEquals(values.get(i), pair.getKey());
            assertEquals(getImpl().varintLength(pair.getKey()), pair.getValue() - index);
            index = pair.getValue();
        }
        assertEquals(index, s.length());
    }

    @Test
    public void testVarint32Overflow() {
        char[] input = new char[] {0x81, 0x82, 0x83, 0x84, 0x85, 0x11};
        assertNull(getImpl().decodeVarint32(input, 0));
    }

    @Test
    public void testVarint32Truncation() {
        StringBuilder s = new StringBuilder();
        getImpl().putVarint32(s, Integer.MIN_VALUE);
        char[] chars = s.toString().toCharArray();
        for (int len = 0; len < s.length() - 1; len++) {
            assertNull(getImpl().decodeVarint32(chars, 0, len));
        }

        Pair<Integer, Integer> pair = getImpl().decodeVarint32(chars, 0);
        assertNotNull(pair);
        assertEquals(Integer.MIN_VALUE, pair.getKey().intValue());
        assertEquals(chars.length, pair.getValue().intValue());
    }

    @Test
    public void testVarint64Overflow() {
        char[] input = new char[] {0x81, 0x82, 0x83, 0x84, 0x85, 0x81, 0x82, 0x83, 0x84, 0x85, 0x11};
        assertNull(getImpl().decodeVarint64(input, 0));
    }

    @Test
    public void testVarint64Truncation() {
        StringBuilder s = new StringBuilder();
        getImpl().putVarint64(s, Long.MIN_VALUE);
        char[] chars = s.toString().toCharArray();
        for (int len = 0; len < s.length() - 1; len++) {
            assertNull(getImpl().decodeVarint64(chars, 0, len));
        }

        Pair<Long, Integer> pair = getImpl().decodeVarint64(chars, 0);
        assertNotNull(pair);
        assertEquals(Long.MIN_VALUE, pair.getKey().longValue());
        assertEquals(chars.length, pair.getValue().intValue());
    }

    @Test
    public void testStrings() {
        StringBuilder s = new StringBuilder();
        getImpl().putLengthPrefixedString(s, "");
        getImpl().putLengthPrefixedString(s, "foo");
        getImpl().putLengthPrefixedString(s, "bar");
        getImpl().putLengthPrefixedString(s, StringUtils.repeat("x", 200));

        char[] chars = s.toString().toCharArray();
        Pair<String, Integer> result = getImpl().getLengthPrefixedString(chars, 0);
        assertNotNull(result);
        assertEquals("", result.getKey());

        result = getImpl().getLengthPrefixedString(chars, result.getValue());
        assertNotNull(result);
        assertEquals("foo", result.getKey());

        result = getImpl().getLengthPrefixedString(chars, result.getValue());
        assertNotNull(result);
        assertEquals("bar", result.getKey());

        result = getImpl().getLengthPrefixedString(chars, result.getValue());
        assertNotNull(result);
        assertEquals(StringUtils.repeat("x", 200), result.getKey());

        assertEquals(result.getValue().intValue(), chars.length);
    }

    @Test
    public void testStringLengthOverflow() {
        char[] input = new char[] {0x81, 0x82, 0x83, 0x84, 0x85, 0x11};
        assertNull(getImpl().decodeVarint32(input, 0));
        assertNull(getImpl().getLengthPrefixedString(input, 0));
    }

    @Test
    public void testStringLengthTruncation() {
        StringBuilder s = new StringBuilder();
        int strLength = 100000; // varintLength is 3
        getImpl().putLengthPrefixedString(s, StringUtils.repeat("x", strLength));
        char[] chars = s.toString().toCharArray();
        for (int len = 0; len < getImpl().varintLength(strLength) - 1; len++) {
            assertNull(getImpl().decodeVarint32(chars, 0, len));
            assertNull(getImpl().getLengthPrefixedString(chars, 0, len));
        }

        assertNotNull(getImpl().decodeVarint32(chars, 0));
        Pair<String, Integer> pair = getImpl().getLengthPrefixedString(chars, 0);
        assertNotNull(pair);
        assertEquals(StringUtils.repeat("x", strLength), pair.getKey());
        assertEquals(pair.getValue().intValue(), chars.length);
    }

    @Test
    public void testStringBufferTooSmall() {
        StringBuilder s = new StringBuilder();
        int strLength = 100000; // varintLength is 3
        getImpl().putLengthPrefixedString(s, StringUtils.repeat("x", strLength));
        char[] chars = s.toString().toCharArray();
        for (int len = getImpl().varintLength(strLength); len < chars.length - 1; len++) {
            assertNotNull(getImpl().decodeVarint32(chars, 0, len));
            assertNull(getImpl().getLengthPrefixedString(chars, 0, len));
        }

        Pair<String, Integer> pair = getImpl().getLengthPrefixedString(chars, 0);
        assertNotNull(pair);
        assertEquals(StringUtils.repeat("x", strLength), pair.getKey());
        assertEquals(pair.getValue().intValue(), chars.length);
    }
}