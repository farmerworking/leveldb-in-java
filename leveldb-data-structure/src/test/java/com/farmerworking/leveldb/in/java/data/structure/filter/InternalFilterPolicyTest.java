package com.farmerworking.leveldb.in.java.data.structure.filter;

import com.farmerworking.leveldb.in.java.api.FilterPolicy;
import com.farmerworking.leveldb.in.java.data.structure.db.InternalFilterPolicy;
import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKey;

public class InternalFilterPolicyTest extends FilterPolicyTest {
    @Override
    protected FilterPolicy getFilterPolicyImpl() {
        return new InternalFilterPolicy(new BloomFilterPolicy(10));
    }

    @Override
    protected String enhanceKey(String key) {
        return new InternalKey(key, 1L).encode();
    }
}
