package com.farmerworking.leveldb.in.java.data.structure.table;

import com.farmerworking.leveldb.in.java.api.Cache;
import com.farmerworking.leveldb.in.java.api.CacheHandle;

public class CacheHandleReleaser implements Runnable {
    private final Cache cache;
    private final CacheHandle cacheHandle;

    public CacheHandleReleaser(CacheHandle cacheHandle, Cache cache) {
        this.cacheHandle = cacheHandle;
        this.cache = cache;
    }

    @Override
    public void run() {
        cache.release(cacheHandle);
    }
}
