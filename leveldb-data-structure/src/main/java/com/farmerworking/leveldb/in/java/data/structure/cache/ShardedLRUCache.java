package com.farmerworking.leveldb.in.java.data.structure.cache;

import com.farmerworking.leveldb.in.java.api.Cache;
import com.farmerworking.leveldb.in.java.api.CacheHandle;
import com.farmerworking.leveldb.in.java.api.Deleter;
import com.farmerworking.leveldb.in.java.common.Hash;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class ShardedLRUCache<T> implements Cache<T> {
    private static int kNumShardBits = 4;
    private static int kNumShards = 1 << kNumShardBits;

    private AtomicLong lastId = new AtomicLong(0);
    private List<LRUCache< T>> shard = new ArrayList<>(kNumShards);
    private Hash hash = new Hash();

    public ShardedLRUCache(int capacity) {
        int capacityPerShard = (capacity + kNumShards - 1) / kNumShards;
        for (int i = 0; i < kNumShards; i++) {
            shard.add(new LRUCache<>(capacityPerShard));
        }
    }

    @Override
    public CacheHandle<T> insert(String key, T value, int charge, Deleter<T> deleter) {
        return shard.get(shard(key)).insert(key, value, charge, deleter);
    }

    @Override
    public CacheHandle<T> lookup(String key) {
        return shard.get(shard(key)).lookup(key);
    }

    @Override
    public T value(CacheHandle<T> cacheHandle) {
        LRUCacheNode<T> handle = (LRUCacheNode<T>) cacheHandle;
        return handle.getValue();
    }

    @Override
    public void release(CacheHandle<T> cacheHandle) {
        LRUCacheNode<T> handle = (LRUCacheNode<T>) cacheHandle;
        shard.get(shard(handle.getKey())).release(handle);
    }

    @Override
    public void erase(String key) {
        shard.get(shard(key)).erase(key);
    }

    @Override
    public long newId() {
        return lastId.incrementAndGet();
    }

    @Override
    public void prune() {
        for(LRUCache cache : shard) {
            cache.prune();
        }
    }

    @Override
    public int totalCharge() {
        int total = 0;
        for (LRUCache cache : shard) {
            total += cache.totalCharge();
        }
        return total;
    }

    private int shard(String key) {
        return hash.hash(key.toCharArray(), 0) >>> (32 - kNumShardBits);
    }
}
