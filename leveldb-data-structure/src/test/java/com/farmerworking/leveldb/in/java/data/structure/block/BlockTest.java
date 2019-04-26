package com.farmerworking.leveldb.in.java.data.structure.block;

import com.farmerworking.leveldb.in.java.api.Options;
import com.farmerworking.leveldb.in.java.common.Coding;
import com.farmerworking.leveldb.in.java.common.ICoding;

public class BlockTest extends IBlockTest {

    @Override
    protected IBlockBuilder getBlockBuilder(Options options) {
        return new BlockBuilder(options);
    }

    @Override
    protected IBlockReader getBlockReader(String content) {
        return new BlockReader(content);
    }

    @Override
    protected ICoding getCodingImpl() {
        return new Coding();
    }
}

