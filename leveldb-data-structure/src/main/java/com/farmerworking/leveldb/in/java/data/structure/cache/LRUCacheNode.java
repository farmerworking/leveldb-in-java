package com.farmerworking.leveldb.in.java.data.structure.cache;

import com.farmerworking.leveldb.in.java.api.CacheHandle;
import com.farmerworking.leveldb.in.java.api.Deleter;
import lombok.Data;

// LRU cache implementation
//
// Cache entries have an "inCache" boolean indicating whether the cache has a
// reference on the entry.  The only ways that this can become false without the
// entry being passed to its "deleter" are:
// 1. via Erase()
// 2. via Insert() when an element with a duplicate key is inserted
@Data
class LRUCacheNode<V> implements CacheHandle<V> {
    private String key;
    private V value;
    private Integer charge;
    private Deleter<V> deleter;
    private boolean inCache = false;

    private LRUCacheNode<V> next;
    private LRUCacheNode<V> previous;

    private int pins;

    public LRUCacheNode(LRUCacheNode<V> previous, LRUCacheNode<V> next, String key, V value, Integer charge, Deleter<V> deleter){
        this.key = key;
        this.value = value;
        this.charge = charge;
        this.deleter = deleter;

        this.previous = previous;
        this.next = next;

        this.pins = 0;
    }

    public void unpin() {
        assert this.pins > 0;
        this.pins --;
    }

    public void pin() {
        this.pins ++;
    }

    public void delete() {
        assert !this.inCache;
        if (deleter != null) {
            deleter.delete(key, value);
        }
    }

    public boolean inLRU() {
        return this.inCache && this.pins == 1;
    }
}
