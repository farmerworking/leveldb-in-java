package com.farmerworking.leveldb.in.java.data.structure.writebatch;

import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.api.BytewiseComparator;
import com.farmerworking.leveldb.in.java.api.Iterator;
import com.farmerworking.leveldb.in.java.common.ICoding;
import com.farmerworking.leveldb.in.java.data.structure.memory.IMemtable;
import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKey;
import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKeyComparator;
import com.farmerworking.leveldb.in.java.data.structure.memory.Memtable;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

public class WriteBatchTest {
    static String printContents(WriteBatch batch) {
        InternalKeyComparator cmp = new InternalKeyComparator(new BytewiseComparator());
        IMemtable mem = new Memtable(cmp);
        String state = "";
        MemTableInserter memTableInserter = new MemTableInserter(batch.getSequence(), mem);
        Status status = batch.iterate(memTableInserter);
        int count = 0;
        Iterator<String, String> iter = mem.iterator();
        InternalKey ikey = new InternalKey();
        for (iter.seekToFirst(); iter.valid(); iter.next()) {
            ikey.decodeFrom(iter.key());
            switch (ikey.type()) {
                case kTypeValue:
                    state = state.concat("Put(");
                    state = state.concat(ikey.userKey());
                    state = state.concat(", ");
                    state = state.concat(iter.value());
                    state = state.concat(")");
                    count++;
                    break;
                case kTypeDeletion:
                    state = state.concat("Delete(");
                    state = state.concat(ikey.userKey());
                    state = state.concat(")");
                    count++;
                    break;
            }
            state = state.concat("@");
            state = state.concat(String.valueOf(ikey.sequence()));
        }
        if (!status.isOk()) {
            state = state.concat("ParseError()");
        } else if (count != batch.getCount()) {
            state = state.concat("CountMismatch()");
        }
        return state;
    }

    @Test
    public void testEmpty() {
        WriteBatch batch = new WriteBatch();
        assertEquals("", printContents(batch));
        assertEquals(0, batch.getCount());
        assertEquals(0, batch.getSequence());
    }

    @Test
    public void testMultiple() {
        WriteBatch batch = new WriteBatch();
        batch.put("foo", "bar");
        batch.delete("box");
        batch.put("baz", "boo");
        batch.setSequence(100);
        assertEquals(100, batch.getSequence());
        assertEquals(3, batch.getCount());
        assertEquals("Put(baz, boo)@102" +
                        "Delete(box)@101" +
                        "Put(foo, bar)@100",
                printContents(batch));
    }

    @Test
    public void testAppend() {
        WriteBatch b1 = new WriteBatch(), b2 = new WriteBatch();
        b1.setSequence(200);
        b2.setSequence(300);
        b1.append(b2);
        assertEquals("", printContents(b1));
        b2.put("a", "va");
        b1.append(b2);
        assertEquals("Put(a, va)@200", printContents(b1));
        b2.clear();
        b2.put("b", "vb");
        b1.append(b2);
        assertEquals("Put(a, va)@200" +
                        "Put(b, vb)@201",
                printContents(b1));
        b2.delete("foo");
        b1.append(b2);
        assertEquals("Put(a, va)@200" +
                        "Put(b, vb)@202" +
                        "Put(b, vb)@201" +
                        "Delete(foo)@203",
                printContents(b1));
    }

    @Test
    public void testApproximateSize() {
        WriteBatch batch = new WriteBatch();
        int empty_size = batch.approximateSize();

        batch.put("foo", "bar");
        int one_key_size = batch.approximateSize();
        assertTrue(one_key_size > empty_size);

        batch.put("baz", "boo");
        int two_keys_size = batch.approximateSize();
        assertTrue(two_keys_size > one_key_size);

        batch.delete("box");
        int post_delete_size = batch.approximateSize();
        assertTrue(post_delete_size > two_keys_size);
    }

    @Test(expected = AssertionError.class)
    public void testDecodeException() {
        WriteBatch batch = new WriteBatch();
        batch.decode(new char[8]);
    }

    @Test
    public void testEncodeDecode() {
        WriteBatch batch = new WriteBatch();
        batch.put("foo", "bar");
        batch.delete("box");
        batch.put("baz", "boo");
        batch.setSequence(100);

        WriteBatch tmp = new WriteBatch();
        tmp.decode(batch.encode());

        assertEquals(100, tmp.getSequence());
        assertEquals(3, tmp.getCount());
        assertEquals("Put(baz, boo)@102" +
                        "Delete(box)@101" +
                        "Put(foo, bar)@100",
                printContents(tmp));
    }

    @Test
    public void testIterateExceptionCase() {
        WriteBatch batch = new WriteBatch();

        WriteBatch spyBatch = spy(batch);

        doReturn(new char[4]).when(spyBatch).encode();
        Status status = spyBatch.iterate(null);
        assertEquals("malformed WriteBatch (too small)", status.getMessage());

        doReturn(StringUtils.repeat('a', 13).toCharArray()).when(spyBatch).encode();
        status = spyBatch.iterate(null);
        assertEquals("unknown WriteBatch tag", status.getMessage());

        doReturn(StringUtils.repeat((char)1, 13).toCharArray()).when(spyBatch).encode();
        doReturn(null).when(spyBatch).getLengthPrefixedString(any(), anyInt());
        status = spyBatch.iterate(null);
        assertEquals("bad WriteBatch Put", status.getMessage());

        doReturn(StringUtils.repeat((char)0, 13).toCharArray()).when(spyBatch).encode();
        doReturn(null).when(spyBatch).getLengthPrefixedString(any(), anyInt());
        status = spyBatch.iterate(null);
        assertEquals("bad WriteBatch Delete", status.getMessage());

        char[] tmp = new char[12];
        ICoding.getInstance().encodeFixed64(tmp, 0, 1L);
        ICoding.getInstance().encodeFixed32(tmp, 8, 10);
        doReturn(tmp).when(spyBatch).encode();
        doReturn(10).when(spyBatch).getCount();
        doCallRealMethod().when(spyBatch).getLengthPrefixedString(any(), anyInt());
        status = spyBatch.iterate(null);
        assertEquals("WriteBatch has wrong count", status.getMessage());
    }
}