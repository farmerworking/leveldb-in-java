package com.farmerworking.leveldb.in.java.data.structure.version;

import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKey;
import com.farmerworking.leveldb.in.java.data.structure.memory.ValueType;
import org.junit.Test;

import static org.junit.Assert.*;

public class VersionEditTest {
    @Test
    public void testVersionEditEncodeDecode() {
        long kBig = 1L << 50;

        VersionEdit edit = new VersionEdit();
        for (int i = 0; i < 4; i++) {
            testEncodeDecode(edit);
            edit.addFile(3, kBig + 300 + i, kBig + 400 + i,
                    new InternalKey("foo", kBig + 500 + i, ValueType.kTypeValue),
                    new InternalKey("zoo", kBig + 600 + i, ValueType.kTypeDeletion));
            edit.deleteFile(4, kBig + 700 + i);
            edit.addCompactPoint(i, new InternalKey("x", kBig + 900 + i, ValueType.kTypeValue));
        }

        edit.setComparatorName("foo");
        edit.setLogNumber(kBig + 100);
        edit.setNextFileNumber(kBig + 200);
        edit.setLastSequence(kBig + 1000);
        testEncodeDecode(edit);
    }

    private void testEncodeDecode(VersionEdit edit) {
        StringBuilder builder = new StringBuilder();
        StringBuilder builder2 = new StringBuilder();

        edit.encodeTo(builder);

        VersionEdit parsed = new VersionEdit();
        Status s = parsed.decodeFrom(builder.toString().toCharArray());
        assertTrue(s.isOk());
        parsed.encodeTo(builder2);
        assertEquals(builder.toString(), builder2.toString());
    }
}