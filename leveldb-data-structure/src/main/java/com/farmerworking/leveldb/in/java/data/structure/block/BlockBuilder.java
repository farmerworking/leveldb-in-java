package com.farmerworking.leveldb.in.java.data.structure.block;

import com.farmerworking.leveldb.in.java.api.Options;
import com.farmerworking.leveldb.in.java.common.ICoding;
import com.google.common.base.Strings;

import java.nio.CharBuffer;
import java.util.Vector;

public class BlockBuilder implements IBlockBuilder {
    private final Options options;

    private Vector<Integer> restartOffsets;
    private int counter;
    private boolean finished;
    private StringBuilder buffer;
    private char[] laskKey;

    public BlockBuilder(Options options) {
        assert options.getBlockRestartInterval() >= 1;
        this.options = options;
        reset();
    }

    @Override
    public void add(String key, String value) {
        assert !finished;
        assert counter <= options.getBlockRestartInterval();
        char[] keyChars = key.toCharArray();
        assert buffer.length() == 0 || options.getComparator().compare(keyChars, laskKey) > 0;
        int shared = 0;

        if (counter < options.getBlockRestartInterval()) {
            shared = laskKey == null ? 0 : Strings.commonPrefix(key, CharBuffer.wrap(laskKey)).length();
        } else {
            restartOffsets.add(buffer.length());
            counter = 0;
        }

        int nonShared = key.length() - shared;
        ICoding.getInstance().putVarint32(buffer, shared);
        ICoding.getInstance().putVarint32(buffer, nonShared);
        ICoding.getInstance().putVarint32(buffer, value.length());

        buffer.append(keyChars, shared, nonShared);
        buffer.append(value);

        // Update state
        laskKey = keyChars;
        counter ++;
    }

    @Override
    public String finish() {
        for (int i = 0; i < restartOffsets.size(); i++) {
            ICoding.getInstance().putFixed32(buffer, restartOffsets.get(i));
        }
        ICoding.getInstance().putFixed32(buffer, restartOffsets.size());
        finished = true;
        return buffer.toString();
    }

    @Override
    public void reset() {
        this.restartOffsets = new Vector<>();
        this.restartOffsets.add(0); // data block start at offset 0
        this.counter = 0;
        this.finished = false;
        this.buffer = new StringBuilder();
        this.laskKey = null;
    }

    @Override
    public boolean isEmpty() {
        return this.buffer.length() == 0;
    }

    @Override
    public int memoryUsage() {
        return (buffer.length() +                                                           // Raw data buffer
                restartOffsets.size() * ICoding.getInstance().getFixed32Length() +          // Restart array
                ICoding.getInstance().getFixed32Length());                                  // Restart array length
    }
}
