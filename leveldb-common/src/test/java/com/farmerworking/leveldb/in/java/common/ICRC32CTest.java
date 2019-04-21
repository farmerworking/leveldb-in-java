package com.farmerworking.leveldb.in.java.common;

import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

public abstract class ICRC32CTest {
    protected abstract ICRC32C getImpl();

    @Test
    public void testValue1() throws Exception {
        //// From rfc3720 section B.4.
        byte[] data = new byte[32];

        for (int i = 0; i < 32; i++) {
            data[i] = (char) 0;
        }
        assertEquals(0x8a9136aa, getImpl().value(data, 0, data.length));

        for (int i = 0; i < 32; i++) {
            data[i] = (byte) i;
        }
        assertEquals(0x46dd794e, getImpl().value(data, 0, data.length));

        for (int i = 0; i < 32; i++) {
            data[i] = (byte) (31 - i);
        }
        assertEquals(0x113fdb5c, getImpl().value(data, 0, data.length));

        for (int i = 0; i < 32; i++) {
            data[i] = (byte) 0xff;
        }
        assertEquals(0x62a8ab43, getImpl().value(data, 0, data.length));

        data = new byte[]{
                0x01, (byte) 0xc0, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00,
                0x14, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x04, 0x00,
                0x00, 0x00, 0x00, 0x14,
                0x00, 0x00, 0x00, 0x18,
                0x28, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00,
                0x02, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00
        };
        assertEquals(0xd9963a56, getImpl().value(data, 0, data.length));
    }

    @Test
    public void testValue2() throws Exception {
        assertNotEquals(value("a"), value("foo"));
    }

    @Test
    public void testMask() throws Exception {
        int crc = value("foo");
        assertNotEquals(crc, getImpl().mask(crc));
        assertNotEquals(crc, getImpl().mask(getImpl().mask(crc)));
        assertEquals(crc, getImpl().unmask(getImpl().mask(crc)));
        assertEquals(crc, getImpl().unmask(getImpl().unmask(getImpl().mask(getImpl().mask(crc)))));
    }

    @Test
    public void testExtend() throws Exception {
        assertEquals(value("hello world"), extend(value("hello"), " world"));
        assertEquals(value("hello world"), extend(value("hello "), "world"));
        assertEquals(value("hello world"), extend(value("hello w"), "orld"));
        assertEquals(value("hello world"), extend(value("hello wo"), "rld"));
    }

    @Test
    public void testExtend2() throws Exception {
        assertNotEquals(value("hello world"), extend(value("hello "), "1world"));
    }

    private int value(String s) {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        return getImpl().value(bytes, 0, bytes.length);
    }

    private int extend(int crc, String s) {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        return getImpl().extend(crc, bytes, 0, bytes.length);
    }
}