package com.farmerworking.leveldb.in.java.data.structure.table;

import com.farmerworking.leveldb.in.java.api.Comparator;
import com.farmerworking.leveldb.in.java.api.Options;
import com.farmerworking.leveldb.in.java.data.structure.cache.ShardedLRUCache;
import com.farmerworking.leveldb.in.java.data.structure.harness.Constructor;
import com.farmerworking.leveldb.in.java.data.structure.harness.HarnessTest;
import com.farmerworking.leveldb.in.java.data.structure.harness.TestArg;
import com.google.common.collect.Lists;

import java.util.List;

public class TableWithBlockCacheHarnessTest extends HarnessTest {
    @Override
    protected List<TestArg> getTestArgList() {
        return Lists.newArrayList(
                new TestArg(false, 16),
                new TestArg(false, 1),
                new TestArg(false, 1024),
                new TestArg(true, 16),
                new TestArg(true, 1),
                new TestArg(true, 1024)
        );
    }

    @Override
    protected Constructor getConstructor(Comparator comparator) {
        Options options = new Options();
        options.setBlockCache(new ShardedLRUCache(2560));
        return new TableConstructor(comparator, options);
    }
}
