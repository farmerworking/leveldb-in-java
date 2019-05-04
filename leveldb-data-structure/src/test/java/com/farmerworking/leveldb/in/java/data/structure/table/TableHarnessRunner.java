package com.farmerworking.leveldb.in.java.data.structure.table;

import com.farmerworking.leveldb.in.java.api.*;
import com.farmerworking.leveldb.in.java.data.structure.harness.*;
import com.google.common.collect.Lists;

import java.util.List;

public class TableHarnessRunner extends HarnessTestRunner {
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
        return new TableConstructor(comparator);
    }
}
