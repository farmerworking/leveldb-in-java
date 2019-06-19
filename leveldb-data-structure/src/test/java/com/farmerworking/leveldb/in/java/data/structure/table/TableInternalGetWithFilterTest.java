package com.farmerworking.leveldb.in.java.data.structure.table;

import com.farmerworking.leveldb.in.java.api.Comparator;
import com.farmerworking.leveldb.in.java.api.Options;
import com.farmerworking.leveldb.in.java.data.structure.filter.BloomFilterPolicy;

public class TableInternalGetWithFilterTest extends TableInternalGetTest{
    @Override
    TableConstructor getTableConstructor(Comparator userComparator, Options options) {
        options.setFilterPolicy(new BloomFilterPolicy(10));
        return new TableConstructor(userComparator, options);
    }
}
