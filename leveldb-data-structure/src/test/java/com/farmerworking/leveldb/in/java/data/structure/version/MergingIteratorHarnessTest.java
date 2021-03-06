package com.farmerworking.leveldb.in.java.data.structure.version;

import com.farmerworking.leveldb.in.java.api.Comparator;
import com.farmerworking.leveldb.in.java.data.structure.harness.Constructor;
import com.farmerworking.leveldb.in.java.data.structure.harness.HarnessTest;
import com.farmerworking.leveldb.in.java.data.structure.harness.TestArg;
import com.google.common.collect.Lists;

import java.util.List;

public class MergingIteratorHarnessTest extends HarnessTest {
    @Override
    protected List<TestArg> getTestArgList() {
        return Lists.newArrayList(new TestArg(false, 16));
    }

    @Override
    protected Constructor getConstructor(Comparator comparator) {
        return new MergingIteratorConstructor(comparator);
    }
}
