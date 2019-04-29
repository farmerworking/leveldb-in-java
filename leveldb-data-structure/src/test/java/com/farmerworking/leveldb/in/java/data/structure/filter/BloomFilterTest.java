package com.farmerworking.leveldb.in.java.data.structure.filter;

import com.farmerworking.leveldb.in.java.api.FilterPolicy;

public class BloomFilterTest extends FilterPolicyTest {
    @Override
    protected FilterPolicy getFilterPolicyImpl() {
        return new BloomFilterPolicy(10);
    }
}
