package com.farmerworking.leveldb.in.java.data.structure.block;

import com.farmerworking.leveldb.in.java.api.Comparator;
import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.common.ICoding;
import com.farmerworking.leveldb.in.java.data.structure.iterator.AbstractIterator;
import javafx.util.Pair;

public class BlockIterator extends AbstractIterator<String, String> {
    private Comparator comparator;
    private char[] data;

    private int restartOffset; // Offset of restart array (list of fixed32)
    private int numRestarts; // Number of uint32_t entries in restart array

    // current_ is offset in data of current entry.  >= restarts_ if !Valid
    private int current;
    // Index of restart block in which current_ falls
    private int restartIndex;

    private Status status;
    private char[] key;
    private Pair<Integer, Integer> value; // offset + size;

    public BlockIterator(Comparator comparator, char[] data, int restartOffset, int numRestarts) {
        assert numRestarts > 0;
        this.comparator = comparator;
        this.data = data;
        this.restartOffset = restartOffset;
        this.numRestarts = numRestarts;

        // put point to the end of region block(data region, restart region)
        this.current = restartOffset;
        this.restartIndex = numRestarts;

        this.status = Status.OK();
        this.key = null;
        this.value = null;
    }

    @Override
    public boolean valid() {
        assert !this.closed;
        return current < restartOffset;
    }

    @Override
    public void seekToFirst() {
        assert !this.closed;
        seekToRestartPoint(0);
        parseNextKey();
    }

    @Override
    public void seekToLast() {
        assert !this.closed;
        seekToRestartPoint(numRestarts - 1);
        while (parseNextKey() && nextEntryOffset() < restartOffset) {
            // Keep skipping
        }
    }

    @Override
    public void seek(String target) {
        assert !this.closed;
        // Binary search in restart array to find the last restart point
        // with a key < target
        int left = 0;
        int right = numRestarts - 1;
        while (left < right) {
            int mid = (left + right + 1) / 2;
            int regionOffset = getRestartPoint(mid);
            Pair<Integer, Pair<Integer, Pair<Integer, Integer>>> pair = decodeEntry(data, regionOffset, restartOffset);
            if (pair == null) {
                corruptionError();
                return;
            }

            int offset = pair.getKey();
            int shared = pair.getValue().getKey();
            int nonShared = pair.getValue().getValue().getKey();
            if (shared != 0) {
                corruptionError();
                return;
            }

            char[] middleKey = new char[nonShared];
            System.arraycopy(data, offset, middleKey, 0, nonShared);
            if (this.comparator.compare(middleKey, target.toCharArray()) < 0) {
                // Key at "mid" is smaller than "target".  Therefore all
                // blocks before "mid" are uninteresting.
                left = mid;
            } else {
                // Key at "mid" is >= "target".  Therefore all blocks at or
                // after "mid" are uninteresting.
                right = mid - 1;
            }
        }

        // Linear search (within restart block) for first key >= target
        seekToRestartPoint(left);
        while (true) {
            if (!parseNextKey()) {
                return;
            }
            if (this.comparator.compare(key, target.toCharArray()) >= 0) {
                return;
            }
        }
    }

    @Override
    public void next() {
        assert !this.closed;
        assert valid();
        parseNextKey();
    }

    @Override
    public void prev() {
        assert !this.closed;
        assert valid();

        // Scan backwards to a restart point before current_
        int original = current;
        while(getRestartPoint(restartIndex) >= original) {
            if (restartIndex == 0) {
                current = restartOffset;
                restartIndex = numRestarts;
                return;
            }

            restartIndex --;
        }

        seekToRestartPoint(restartIndex);
        while(parseNextKey() && nextEntryOffset() < original) {
            // nothing
        }
    }

    @Override
    public String key() {
        assert !this.closed;
        assert(valid());
        return new String(key);
    }

    @Override
    public String value() {
        assert !this.closed;
        assert(valid());
        return new String(data, value.getKey(), value.getValue());
    }

    @Override
    public Status status() {
        assert !this.closed;
        return status;
    }

    private void seekToRestartPoint(int restartIndex) {
        key = null;
        this.restartIndex = restartIndex;
        // current will be fixed by parseNextKey();

        // parseNextKey() starts at the end of value, so set value accordingly
        int offset = getRestartPoint(restartIndex);
        value = new Pair<>(offset, 0);
    }
    
    private int getRestartPoint(int index) {
        assert index < numRestarts;
        return ICoding.getInstance().decodeFixed32(
                data, restartOffset + index * ICoding.getInstance().getFixed32Length());
    }
    
    private boolean parseNextKey() {
        current = nextEntryOffset();
        if (current >= restartOffset) {
            // No more entries to return.  Mark as invalid.
            current = restartOffset;
            restartIndex = numRestarts;
            return false;
        }

        // Decode next entry
        Pair<Integer, Pair<Integer, Pair<Integer, Integer>>> pair = decodeEntry(data, current, restartOffset);

        if (pair == null) {
            corruptionError();
            return false;
        }

        Integer offset = pair.getKey();
        Integer shared = pair.getValue().getKey();
        Integer nonShared = pair.getValue().getValue().getKey();
        Integer valueLength = pair.getValue().getValue().getValue();

        if (getKeyLength() < shared) {
            corruptionError();
            return false;
        }

        char[] keyChars = new char[shared + nonShared];
        if (shared > 0) {
            System.arraycopy(key, 0, keyChars, 0, shared);
        }
        System.arraycopy(data, offset, keyChars, shared, nonShared);
        key = keyChars;
        value = new Pair<>(offset + nonShared, valueLength);
        while (restartIndex + 1 < numRestarts && getRestartPoint(restartIndex + 1) < current) {
            ++restartIndex;
        }
        return true;
    }

    private void corruptionError() {
        current = restartOffset;
        restartIndex = numRestarts;
        status = Status.Corruption("bad entry in block");
        key = null;
        value = null;
    }

    private Pair<Integer, Pair<Integer, Pair<Integer, Integer>>> decodeEntry(char[] buffer, int offset, int limit) {
        if (limit - offset < 3) {
            return null;
        }

        int shared = buffer[offset];
        int nonShared = buffer[offset + 1];
        int valueLength = buffer[offset + 2];
        if ((shared | nonShared | valueLength) < 128) {
            // Fast path: all three values are encoded in one byte each
            offset += 3;
        } else {
            Pair<Integer, Integer> pair = ICoding.getInstance().decodeVarint32(buffer, offset, limit);
            if (pair == null) { return null; }
            offset = pair.getValue();
            shared = pair.getKey();

            pair = ICoding.getInstance().decodeVarint32(buffer, offset, limit);
            if (pair == null) { return null; }
            offset = pair.getValue();
            nonShared = pair.getKey();

            pair = ICoding.getInstance().decodeVarint32(buffer, offset, limit);
            if (pair == null) { return null; }
            offset = pair.getValue();
            valueLength = pair.getKey();
        }

        if ((limit - offset) < (nonShared + valueLength)) {
            return null;
        }
        return new Pair<>(offset, new Pair<>(shared, new Pair<>(nonShared, valueLength)));
    }

    // Return the offset in data just past the end of the current entry.
    private int nextEntryOffset() {
        return value.getKey() + value.getValue();
    }

    private int getKeyLength() {
        return key == null ? 0 : key.length;
    }
}
