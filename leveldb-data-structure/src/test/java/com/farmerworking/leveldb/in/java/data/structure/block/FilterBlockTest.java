package com.farmerworking.leveldb.in.java.data.structure.block;

import com.farmerworking.leveldb.in.java.api.FilterPolicy;

public class FilterBlockTest extends IFilterBlockTest {
    @Override
    protected IFilterBlockBuilder getBuilderImpl(FilterPolicy filterPolicy) {
        return new FilterBlockBuilder(filterPolicy);
    }

    @Override
    protected IFilterBlockReader getReaderImpl(FilterPolicy filterPolicy, String blockContent) {
        return new FilterBlockReader(filterPolicy, blockContent);
    }
}
