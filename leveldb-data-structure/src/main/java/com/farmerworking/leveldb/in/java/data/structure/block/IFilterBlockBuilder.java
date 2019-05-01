package com.farmerworking.leveldb.in.java.data.structure.block;

import com.farmerworking.leveldb.in.java.api.FilterPolicy;

public interface IFilterBlockBuilder {
    void startBlock(long blockOffset);

    void addKey(String key);

    String finish();

    public static IFilterBlockBuilder getDefaultImpl(FilterPolicy policy) {
        if (policy != null) {
            return new FilterBlockBuilder(policy);
        } else {
            return null;
        }
    }
}
