package com.farmerworking.leveldb.in.java.data.structure.block;

import com.farmerworking.leveldb.in.java.api.FilterPolicy;

public interface IFilterBlockReader {
    static IFilterBlockReader getDefaultImpl(FilterPolicy filterPolicy, String filterData) {
        return new FilterBlockReader(filterPolicy, filterData);
    }

    boolean keyMayMatch(long blockOffset, String key);
}
