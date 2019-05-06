package com.farmerworking.leveldb.in.java.data.structure.cache;

import com.farmerworking.leveldb.in.java.api.Deleter;

import java.util.HashMap;
import java.util.Map;

// The cache keeps two linked lists of items in the cache.  All items in the
// cache are in one list or the other, and never both.  Items still referenced
// by clients but erased from the cache are in neither list.  The lists are:
// - in-use:  contains the items currently referenced by clients, in no
//   particular order.  (This list is used for invariant checking.  If we
//   removed the check, elements that would otherwise be on this list could be
//   left as disconnected singleton lists.)
// - LRU:  contains the items not currently referenced by clients, in LRU order
// Elements are moved between these lists by the Ref() and Unref() methods,
// when they detect an element in the cache acquiring or losing its only
// external reference.
public class LRUCache<V>{
    private final int capacity;
    private int usage;

    private LRUCacheNode<V> lru;
    private LRUCacheNode<V> inUse;
    private Map<String, LRUCacheNode<V>> table;

    public LRUCache(int capacity) {
        this.capacity = capacity;
        this.usage = 0;

        lru = new LRUCacheNode<>(null, null, null, null, null, null);
        inUse = new LRUCacheNode<>(null, null, null, null, null, null);

        lru.setNext(lru);
        lru.setPrevious(lru);

        inUse.setNext(inUse);
        inUse.setPrevious(inUse);

        this.table = new HashMap<>();
    }

    public synchronized LRUCacheNode<V> lookup(String key) {
        LRUCacheNode<V> node = table.get(key);

        if (node != null) {
            ref(node);
        }

        return node;
    }

    public synchronized void release(LRUCacheNode<V> node) {
        unref(node);
    }

    public synchronized LRUCacheNode<V> insert(String key, V value, int charge, Deleter<V> deleter) {
        LRUCacheNode<V> node = new LRUCacheNode<>(null, null, key, value, charge, deleter);
        node.pin(); // for the returned handle

        if (capacity > 0) {
            node.pin(); // for the cache reference
            node.setInCache(true);
            append(inUse, node);
            usage += charge;
            finishErase(table.put(key, node));
        } else {
            // don't cache. (capacity == 0 is supported and turns off caching.)
            // todo: maybe set next to null
        }

        while(usage > capacity && lru.getNext() != lru) {
            eraseOldest();
        }

        return node;
    }

    public synchronized void erase(String key) {
        finishErase(table.remove(key));
    }

    public synchronized void prune() {
        while(lru.getNext() != lru) {
            eraseOldest();
        }
    }

    public synchronized int totalCharge() {
        return usage;
    }

    private void eraseOldest() {
        LRUCacheNode<V> oldest = lru.getNext();
        assert oldest.getPins() == 1;
        boolean erased = finishErase(table.remove(oldest.getKey()));
        assert erased;
    }

    private boolean finishErase(LRUCacheNode<V> node) {
        if (node != null) {
            assert node.isInCache();
            remove(node);
            node.setInCache(false);
            usage -= node.getCharge();
            unref(node);
        }
        return node != null;
    }

    private void ref(LRUCacheNode<V> node) {
        if (node.inLRU()) {
            remove(node);
            append(inUse, node);
        }
        node.pin();
    }

    private void unref(LRUCacheNode<V> node) {
        node.unpin();

        if (node.getPins() == 0) {
            node.delete();
        } else if (node.inLRU()) {
            remove(node);
            append(lru, node);
        }
    }

    private void remove(LRUCacheNode<V> node) {
        node.getNext().setPrevious(node.getPrevious());
        node.getPrevious().setNext(node.getNext());
    }

    private void append(LRUCacheNode<V> listHead, LRUCacheNode<V> node) {
        node.setPrevious(listHead.getPrevious());
        listHead.getPrevious().setNext(node);

        listHead.setPrevious(node);
        node.setNext(listHead);
    }
}
