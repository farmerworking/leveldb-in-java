package com.farmerworking.leveldb.in.java.data.structure.memory;

import org.junit.Test;

import static org.junit.Assert.*;

public class InternalKeyTest {
    @Test
    public void testDecodeEncode() {
        InternalKey internalKey = new InternalKey("tmp", 888, ValueType.kTypeValue);
        String req = internalKey.encode();

        InternalKey decode = InternalKey.decode(req);
        assertEquals(decode, internalKey);
    }

    @Test
    public void testDecodeEncode2() {
        InternalKey internalKey = new InternalKey("what", 999, ValueType.kTypeDeletion);
        String req = internalKey.encode();

        InternalKey decode = InternalKey.decode(req);
        assertEquals(decode, internalKey);
    }
}