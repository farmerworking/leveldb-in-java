package com.farmerworking.leveldb.in.java.data.structure.db;

import com.farmerworking.leveldb.in.java.api.Comparator;
import com.farmerworking.leveldb.in.java.api.Iterator;
import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.data.structure.iterator.AbstractIterator;
import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKey;
import com.farmerworking.leveldb.in.java.data.structure.memory.ParsedInternalKey;
import com.farmerworking.leveldb.in.java.data.structure.memory.ValueType;
import com.farmerworking.leveldb.in.java.data.structure.version.Config;
import javafx.util.Pair;

import java.util.Random;

public class DBIterator extends AbstractIterator<String, String> {
    enum Direction {
        forward, backward
    }

    private Direction direction;
    private final Comparator userComparator;
    private final Iterator<String, String> iter;
    private final long sequence;
    private DBImpl db;
    private boolean valid;
    private int bytesCounter;
    private Random random;
    private Status status = Status.OK();

    private String savedKey;
    private String savedValue;

    public DBIterator(DBImpl db, Comparator comparator, Iterator<String, String> iter, long sequence, long seed) {
        this.db = db;
        this.userComparator = comparator;
        this.iter = iter;
        this.sequence = sequence;
        this.random = new Random(seed);
        this.direction = Direction.forward;
        this.valid = false;
        this.bytesCounter = randomPeriod();
    }

    private int randomPeriod() {
        return random.nextInt(2 * Config.kReadBytesPeriod);
    }

    @Override
    public boolean valid() {
        return this.valid;
    }

    @Override
    public void seekToFirst() {
        this.direction = Direction.forward;
        clearSavedValue();
        this.iter.seekToFirst();
        if (this.iter.valid()) {
            findNextUserEntry(false, this.savedKey);
        } else {
            this.valid = false;
        }
    }

    private void findNextUserEntry(boolean skipping, String skip) {
        assert iter.valid();
        assert this.direction == Direction.forward;

        do {
            Pair<Boolean, ParsedInternalKey> pair = parseKey();
            ParsedInternalKey parsedInternalKey = pair.getValue();
            if (pair.getKey() && parsedInternalKey.getSequence() <= this.sequence) {
                switch (pair.getValue().getValueType()) {
                    case kTypeDeletion:
                        // Arrange to skip all upcoming entries for this key since
                        // they are hidden by this deletion.
                        skip = parsedInternalKey.getUserKey();
                        skipping = true;
                        break;
                    case kTypeValue:
                        if (skipping && userComparator.compare(pair.getValue().getUserKeyChar(), skip.toCharArray()) <= 0) {
                            // entry hidden
                        } else {
                            this.valid = true;
                            this.savedKey = null;
                            return;
                        }
                        break;
                    default:
                        break;

                }
            }
            iter.next();
        } while (this.iter.valid());

        savedKey = null;
        this.valid = false;
    }

    private Pair<Boolean, ParsedInternalKey> parseKey() {
        String key = iter.key();

        int n = key.length() + iter.value().length();
        bytesCounter -= n;
        while(bytesCounter < 0) {
            bytesCounter += randomPeriod();
            this.db.recordReadSample(key);
        }

        Pair<Boolean, ParsedInternalKey> pair = InternalKey.parseInternalKey(key);
        if (!pair.getKey()) {
            this.status = Status.Corruption("corrupted internal key in DBIter");
            return new Pair<>(false, null);
        } else {
            return pair;
        }
    }

    private void clearSavedValue() {
        this.savedValue = null;
    }

    @Override
    public void seekToLast() {
        this.direction = Direction.backward;
        clearSavedValue();
        iter.seekToLast();
        findPrevUserEntry();
    }

    private void findPrevUserEntry() {
        assert this.direction == Direction.backward;

        ValueType valueType = ValueType.kTypeDeletion;
        if (iter.valid()) {
            do {
                Pair<Boolean, ParsedInternalKey> parse = parseKey();
                ParsedInternalKey parsedInternalKey = parse.getValue();
                if (parse.getKey() && parsedInternalKey.getSequence() <= this.sequence) {
                    if ((valueType != ValueType.kTypeDeletion) &&
                        this.userComparator.compare(parsedInternalKey.getUserKeyChar(), this.savedKey.toCharArray()) < 0) {
                        // We encountered a non-deleted value in entries for previous keys,
                        break;
                    }
                    valueType = parsedInternalKey.getValueType();
                    if (valueType == ValueType.kTypeDeletion) {
                        savedKey = null;
                        clearSavedValue();
                    } else {
                        savedKey = InternalKey.extractUserKey(iter.key());
                        savedValue = iter.value();
                    }
                }
                iter.prev();
            } while (iter.valid());
        }

        if (valueType == ValueType.kTypeDeletion) {
            valid = false;
            savedKey = null;
            clearSavedValue();
            direction = Direction.forward;
        } else {
            valid = true;
        }
    }

    @Override
    public void seek(String target) {
        direction = Direction.forward;
        clearSavedValue();
        this.savedKey = null;
        this.savedKey = new InternalKey(target, this.sequence, ValueType.kValueTypeForSeek).encode();
        iter.seek(this.savedKey);
        if (iter.valid()) {
            findNextUserEntry(false, this.savedKey);
        } else {
            this.valid = false;
        }
    }

    @Override
    public void next() {
        assert this.valid;

        // Switch directions?
        if (this.direction == Direction.backward) {
           this.direction = Direction.forward;
            // iter_ is pointing just before the entries for this->key(),
            // so advance into the range of entries for this->key() and then
            // use the normal skipping code below.
            if (!iter.valid()) {
                iter.seekToFirst();
            } else {
                iter.next();
            }

            if (!iter.valid()) {
                this.valid = false;
                this.savedKey = null;
                return;
            }
            // saved_key_ already contains the key to skip past.
        } else {
            this.savedKey = InternalKey.extractUserKey(iter.key());
        }

        findNextUserEntry(true, this.savedKey);
    }

    @Override
    public void prev() {
        assert this.valid;

        // Switch directions?
        if (this.direction == Direction.forward) {
            // iter_ is pointing at the current entry.  Scan backwards until
            // the key changes so we can use the normal reverse scanning code.
            assert iter.valid();
            this.savedKey = InternalKey.extractUserKey(iter.key());
            while(true) {
                iter.prev();
                if (!iter.valid()) {
                    valid = false;
                    this.savedKey = null;
                    clearSavedValue();
                    return;
                }
                if (this.userComparator.compare(InternalKey.extractUserKey(iter.key()).toCharArray(), this.savedKey.toCharArray()) < 0) {
                    break;
                }
            }
            this.direction = Direction.backward;
        }

        findPrevUserEntry();
    }

    @Override
    public String key() {
        assert valid;
        return direction == Direction.forward ? InternalKey.extractUserKey(iter.key()) : savedKey;
    }

    @Override
    public String value() {
        assert valid;
        return direction == Direction.forward ? iter.value() : savedValue;
    }

    @Override
    public Status status() {
        if (status.isOk()) {
            return this.iter.status();
        } else {
            return this.status;
        }
    }
}
