package com.farmerworking.leveldb.in.java.data.structure.memory;

import com.farmerworking.leveldb.in.java.api.Comparator;
import com.farmerworking.leveldb.in.java.data.structure.harness.Constructor;
import com.farmerworking.leveldb.in.java.data.structure.harness.HarnessTestRunner;
import com.farmerworking.leveldb.in.java.data.structure.harness.TestArg;
import com.google.common.collect.Lists;

import java.util.List;

public class IMemtableHarnessTestRunner extends HarnessTestRunner {
    @Override
    protected List<TestArg> getTestArgList() {
        return Lists.newArrayList(
                new TestArg(false, 16),
                new TestArg(true, 16)
        );
    }

    @Override
    protected Constructor getConstructor(Comparator comparator) {
        return new MemtableConstructor(comparator);
    }
}
