package com.farmerworking.leveldb.in.java.common;

import org.junit.Test;

import static org.junit.Assert.*;

public abstract class IHashTest {
    protected abstract IHash getImpl();

    @Test
    public void testHashSignedUnsignedIssue() {
        char[] data1 = {0x62};
        char[] data2 = {0xc3, 0x97};
        char[] data3 = {0xe2, 0x99, 0xa5};
        char[] data4 = {0xe1, 0x80, 0xb9, 0x32};
        char[] data5 = {
                0x01, 0xc0, 0x00, 0x00,
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
                0x00, 0x00, 0x00, 0x00,
        };

        IHash hash = getImpl();
        assertEquals(hash.hash(new char[0], 0xbc9f1d34), 0xbc9f1d34);
        assertEquals(hash.hash(data1, 0xbc9f1d34), 0xef1345c4);
        assertEquals(hash.hash(data2, 0xbc9f1d34), 0x5b663814);
        assertEquals(hash.hash(data3, 0xbc9f1d34), 0x323c078f);
        assertEquals(hash.hash(data4, 0xbc9f1d34), 0xed21633a);
        assertEquals(hash.hash(data5, 0x12345678), 0xf333dabb);
    }
}