package com.farmerworking.leveldb.in.java.data.structure.memory;

import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.common.ICoding;
import com.farmerworking.leveldb.in.java.data.structure.iterator.AbstractIterator;
import com.farmerworking.leveldb.in.java.data.structure.skiplist.ISkipList;
import com.farmerworking.leveldb.in.java.data.structure.skiplist.ISkipListIterator;
import javafx.util.Pair;

public class MemtableIterator extends AbstractIterator<String, String> {
    private static ICoding coding = ICoding.getInstance();

    private ISkipListIterator<char[]> iter;

    public MemtableIterator(ISkipList<char[]> skipList) {
        this.iter = skipList.iterator();
    }

    @Override
    public boolean valid() {
        assert !this.closed;
        return iter.valid();
    }

    @Override
    public void seekToFirst() {
        assert !this.closed;
        iter.seekToFirst();
    }

    @Override
    public void seekToLast() {
        assert !this.closed;
        iter.seekToLast();
    }

    @Override
    public void seek(String target) {
        assert !this.closed;
        char[] buffer = new char[coding.varintLength(target.length()) + target.length()];
        int offset = coding.encodeVarint32(buffer, 0, target.length());
        System.arraycopy(target.toCharArray(), 0, buffer, offset, target.length());
        iter.seek(buffer);
    }

    @Override
    public void next() {
        assert !this.closed;
        iter.next();
    }

    @Override
    public void prev() {
        assert !this.closed;
        iter.prev();
    }

    @Override
    public String key() {
        assert !this.closed;
        Pair<String, Integer> tmp = coding.getLengthPrefixedString(iter.key(), 0);
        return tmp.getKey();
    }

    @Override
    public String value() {
        assert !this.closed;
        char[] buffer = iter.key();
        Pair<String, Integer> tmp = coding.getLengthPrefixedString(buffer, 0);
        tmp = coding.getLengthPrefixedString(buffer, tmp.getValue());
        return tmp.getKey();
    }

    @Override
    public Status status() {
        assert !this.closed;
        return Status.OK();
    }
}
