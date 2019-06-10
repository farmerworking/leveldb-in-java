package com.farmerworking.leveldb.in.java.api;

public interface Cache<T> {
    // Insert a mapping from key->value into the cache and assign it
    // the specified charge against the total cache capacity.
    CacheHandle<T> insert(String key, T value, int charge, Deleter<T> deleter);

    // If the cache has no mapping for "key", returns nullptr.
    //
    // Else return the value corresponding
    CacheHandle<T> lookup(String key);

    // Release a mapping returned by a previous Lookup().
    void release(CacheHandle<T> cacheHandle);

    // Return the value encapsulated in a handle returned by a
    // successful Lookup().
    T value(CacheHandle<T> cacheHandle);

    // If the cache contains entry for key, erase it.
    void erase(String key);

    // Return a new numeric id.  May be used by multiple clients who are
    // sharing the same cache to partition the key space.  Typically the
    // client will allocate a new id at startup and prepend the id to
    // its cache keys.
    long newId();

    // Remove all cache entries that are not actively in use.  Memory-constrained
    // applications may wish to call this method to reduce memory usage.
    // Default implementation of Prune() does nothing.  Subclasses are strongly
    // encouraged to override the default implementation.  A future release of
    // leveldb may change Prune() to a pure abstract method.
    void prune();

    // Return an estimate of the combined charges of all elements stored in the
    // cache.
    int totalCharge();
}
