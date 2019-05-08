package com.farmerworking.leveldb.in.java.data.structure.cache;

import com.farmerworking.leveldb.in.java.api.Cache;
import com.farmerworking.leveldb.in.java.api.Deleter;

public class ShardLRUCacheTest extends CacheTest {
    @Override
    protected Cache<Integer> getImpl(int capacity) {
        return new ShardedLRUCache<>(capacity);
    }
}
