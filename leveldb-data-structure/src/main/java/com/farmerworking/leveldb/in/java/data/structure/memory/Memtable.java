package com.farmerworking.leveldb.in.java.data.structure.memory;

import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.api.Iterator;
import com.farmerworking.leveldb.in.java.common.ICoding;
import com.farmerworking.leveldb.in.java.data.structure.skiplist.ISkipList;
import com.farmerworking.leveldb.in.java.data.structure.skiplist.ISkipListIterator;
import com.farmerworking.leveldb.in.java.data.structure.skiplist.JDKSkipList;
import javafx.util.Pair;

import java.util.Arrays;

public class Memtable implements IMemtable {
    private static ICoding coding = ICoding.getInstance();

    private ISkipList<char[]> table;
    private MemtableEntryComparator comparator;

    public Memtable(InternalKeyComparator comparator) {
        this.comparator = new MemtableEntryComparator(comparator);
        this.table = new JDKSkipList<>(this.comparator);
    }

    @Override
    public Iterator iterator() {
        return new MemtableIterator(table);
    }

    @Override
    public void add(long sequence, ValueType type, String key, String value) {
        int keySize = key.length();
        int valueSize = value.length();
        int internalKeySize = keySize + coding.getFixed64Length();
        int encodedSize = coding.varintLength(internalKeySize) + internalKeySize + coding.varintLength(valueSize) + valueSize;

        char[] buffer = new char[encodedSize];
        int offset = 0;
        offset = coding.encodeVarint32(buffer, offset, internalKeySize);
        System.arraycopy(key.toCharArray(), 0, buffer, offset, key.length());
        offset += key.length();
        coding.encodeFixed64(buffer, offset, InternalKey.packSequenceAndType(sequence, type));
        offset += coding.getFixed64Length();
        offset += coding.encodeVarint32(buffer, offset, valueSize);
        System.arraycopy(value.toCharArray(), 0, buffer, offset, value.length());
        offset += value.length();
        assert offset == encodedSize;
        table.insert(buffer);
    }

    @Override
    public Pair<Boolean, Pair<Status, String>> get(String userKey, long sequence) {
        ISkipListIterator<char[]> iter = table.iterator();
        iter.seek(seekKey(userKey, sequence));

        if (iter.valid()) {
            // Check that it belongs to same user key.  We do not check the
            // sequence number since the Seek() call above should have skipped
            // all entries with overly large sequence numbers.
            char[] entry = iter.key();
            Pair<Integer, Integer> pair = coding.decodeVarint32(entry, 0);
            Integer userKeyStartOffset = pair.getValue();
            Integer userKeyLength = pair.getKey();

            if (comparator.comparator.userComparator.compare(Arrays.copyOfRange(entry, userKeyStartOffset, userKeyStartOffset + userKeyLength - coding.getFixed64Length()), userKey.toCharArray()) == 0) {
                long tag = coding.decodeFixed64(entry, userKeyStartOffset + userKeyLength - coding.getFixed64Length());
                ValueType type = ValueType.valueOf((int) tag & 0xff);
                if (type == ValueType.kTypeValue) {
                    Pair<String, Integer> tmp = coding.getLengthPrefixedString(entry, userKeyStartOffset + userKeyLength);
                    return new Pair<>(true, new Pair<>(Status.OK(), tmp.getKey()));
                } else {
                    return new Pair<>(true, new Pair<>(Status.NotFound(""), ""));
                }
            } else {
                return new Pair<>(false, null);
            }
        } else {
            return new Pair<>(false, null);
        }
    }

    private char[] seekKey(String userKey, long sequence) {
        int internalKeySize = userKey.length() + coding.getFixed64Length();
        int encodedSize = coding.varintLength(internalKeySize) + internalKeySize;
        char[] buffer = new char[encodedSize];
        int offset = 0;
        offset = coding.encodeVarint32(buffer, offset, internalKeySize);
        System.arraycopy(userKey.toCharArray(), 0, buffer, offset, userKey.length());
        offset += userKey.length();
        coding.encodeFixed64(buffer, offset, InternalKey.packSequenceAndType(sequence, ValueType.kValueTypeForSeek));
        return buffer;
    }

    @Override
    public int approximateMemoryUsage() {
        return table.approximateMemoryUsage();
    }
}
