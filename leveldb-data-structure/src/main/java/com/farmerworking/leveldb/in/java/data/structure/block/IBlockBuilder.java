package com.farmerworking.leveldb.in.java.data.structure.block;

import com.farmerworking.leveldb.in.java.api.Options;
import com.farmerworking.leveldb.in.java.data.structure.skiplist.Sizable;

public interface IBlockBuilder extends Sizable {
    // REQUIRES: Finish() has not been called since the last call to Reset().
    // REQUIRES: key is larger than any previously added key
    void add(String key, String value);

    // Finish building the block and return block contents.
    String finish();

    // Reset the contents as if the BlockBuilder was just constructed.
    void reset();

    boolean isEmpty();

    static IBlockBuilder getDefaultImpl(Options options) {
        return new BlockBuilder(options);
    }
}
