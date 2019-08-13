package com.farmerworking.leveldb.in.java.data.structure.memory;

import com.farmerworking.leveldb.in.java.api.Iterator;
import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.data.structure.iterator.AbstractIterator;
import javafx.util.Pair;

public class KeyConvertingIterator extends AbstractIterator<String, String> {
    private final Iterator<String, String> iterator;
    private Status status = Status.OK();

    public KeyConvertingIterator(Iterator<String, String> iterator) {
        this.iterator = iterator;
    }

    @Override
    public boolean valid() {
        assert !this.closed;
        return iterator.valid();
    }

    @Override
    public void seekToFirst() {
        assert !this.closed;
        iterator.seekToFirst();
    }

    @Override
    public void seekToLast() {
        assert !this.closed;
        iterator.seekToLast();
    }

    @Override
    public void seek(String target) {
        assert !this.closed;
        iterator.seek(new InternalKey(target, InternalKey.kMaxSequenceNumber, ValueType.kTypeValue).encode());
    }

    @Override
    public void next() {
        assert !this.closed;
        iterator.next();
    }

    @Override
    public void prev() {
        assert !this.closed;
        iterator.prev();
    }

    @Override
    public String key() {
        assert !this.closed;

        assert valid();
        Pair<Boolean, ParsedInternalKey> pair = InternalKey.parseInternalKey(iterator.key());
        if (!pair.getKey()) {
            this.status = Status.Corruption("malformed internal key");
            return "corrupted key";
        } else {
            return pair.getValue().getUserKey();
        }
    }

    @Override
    public String value() {
        assert !this.closed;
        return iterator.value();
    }

    @Override
    public Status status() {
        assert !this.closed;
        return this.status.isOk() ? iterator.status() : this.status;
    }
}
