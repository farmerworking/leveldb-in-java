package com.farmerworking.leveldb.in.java.data.structure.memory;

import com.farmerworking.leveldb.in.java.common.ICoding;
import javafx.util.Pair;
import org.junit.Test;

import static org.junit.Assert.*;

public class InternalKeyTest {
    @Test
    public void testDecodeEncode() {
        InternalKey internalKey = new InternalKey("tmp", 888, ValueType.kTypeValue);
        String req = internalKey.encode();

        InternalKey decode = new InternalKey();
        decode.decodeFrom(req);
        assertEquals(decode, internalKey);
    }

    @Test
    public void testDecodeEncode2() {
        InternalKey internalKey = new InternalKey("what", 999, ValueType.kTypeDeletion);
        String req = internalKey.encode();

        InternalKey decode = new InternalKey();
        decode.decodeFrom(req);
        assertEquals(decode, internalKey);
    }

    @Test
    public void testSimple() {
        InternalKey internalKey1 = new InternalKey("what", 999, ValueType.kTypeDeletion);
        InternalKey internalKey2 = new InternalKey("tmp", 888, ValueType.kTypeValue);

        assertEquals("what", internalKey1.userKey());
        assertEquals("tmp", internalKey2.userKey());

        assertEquals(999, internalKey1.sequence());
        assertEquals(888, internalKey2.sequence());

        assertEquals(ValueType.kTypeDeletion, internalKey1.type());
        assertEquals(ValueType.kTypeValue, internalKey2.type());
    }

    @Test(expected = AssertionError.class)
    public void testInit1() {
        InternalKey internalKey = new InternalKey();
        internalKey.userKey();
    }

    @Test(expected = AssertionError.class)
    public void testInit2() {
        InternalKey internalKey = new InternalKey();
        internalKey.sequence();
    }

    @Test(expected = AssertionError.class)
    public void testInit3() {
        InternalKey internalKey = new InternalKey();
        internalKey.type();
    }

    @Test
    public void testConvert() {
        InternalKey internalKey = new InternalKey("what", 999, ValueType.kTypeDeletion);
        ParsedInternalKey parsedInternalKey = internalKey.convert();

        assertEquals("what", parsedInternalKey.getUserKey());
        assertEquals(999, parsedInternalKey.getSequence());
        assertEquals(ValueType.kTypeDeletion, parsedInternalKey.getValueType());
    }

    @Test
    public void testParseInternalKey() {
        Pair<Boolean, ParsedInternalKey> pair = InternalKey.parseInternalKey("");
        assertFalse(pair.getKey());

        char[] buffer = new char[9];
        buffer[0] = 'a';
        ICoding.getInstance().encodeFixed64(buffer, 1, (100 << 8) | 3);
        pair = InternalKey.parseInternalKey(new String(buffer));
        assertFalse(pair.getKey());

        pair = InternalKey.parseInternalKey(new InternalKey("what", 999, ValueType.kTypeDeletion).encode());
        assertTrue(pair.getKey());
        ParsedInternalKey parsedInternalKey = pair.getValue();

        assertEquals("what", parsedInternalKey.getUserKey());
        assertEquals(999, parsedInternalKey.getSequence());
        assertEquals(ValueType.kTypeDeletion, parsedInternalKey.getValueType());
    }
}