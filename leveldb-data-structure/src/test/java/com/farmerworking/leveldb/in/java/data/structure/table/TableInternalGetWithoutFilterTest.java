package com.farmerworking.leveldb.in.java.data.structure.table;

import com.farmerworking.leveldb.in.java.api.Comparator;
import com.farmerworking.leveldb.in.java.api.Options;

public class TableInternalGetWithoutFilterTest extends TableInternalGetTest{
    @Override
    TableConstructor getTableConstructor(Comparator userComparator, Options options) {
        return new TableConstructor(userComparator, options);
    }
}
