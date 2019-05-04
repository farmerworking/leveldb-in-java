package com.farmerworking.leveldb.in.java.data.structure.block;

import com.farmerworking.leveldb.in.java.api.Comparator;
import com.farmerworking.leveldb.in.java.api.Iterator;
import com.farmerworking.leveldb.in.java.api.Options;
import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.data.structure.harness.Constructor;
import com.farmerworking.leveldb.in.java.data.structure.harness.StringComparator;

import java.util.Map;
import java.util.TreeMap;

class BlockConstructor extends Constructor {
    private IBlockReader iBlockReader;

    public BlockConstructor(Comparator comparator) {
        super(comparator);
    }

    @Override
    public Iterator<String, String> iterator() {
        return iBlockReader.iterator(this.comparator);
    }

    @Override
    public Status finishImpl(Options options, Map<String, String> data) {
        IBlockBuilder blockBuilder = IBlockBuilder.getDefaultImpl(options);
        for(String key : data.keySet()) {
            blockBuilder.add(key, data.get(key));
        }
        String content = blockBuilder.finish();
        iBlockReader = IBlockReader.getDefaultImpl(content);
        return Status.OK();
    }
}
