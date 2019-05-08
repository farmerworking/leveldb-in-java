package com.farmerworking.leveldb.in.java.data.structure.cache;

import com.farmerworking.leveldb.in.java.api.Cache;
import com.farmerworking.leveldb.in.java.api.CacheHandle;
import com.farmerworking.leveldb.in.java.common.ICoding;
import org.junit.Before;
import org.junit.Test;

import java.util.Vector;

import static org.junit.Assert.*;

public abstract class CacheTest {

    private static int kCacheSize = 1000;
    Cache<Integer> cache;
    TestDeleter<Integer> deleter;

    protected abstract Cache<Integer> getImpl(int capacity);

    @Before
    public void setUp() throws Exception {
        deleter = new TestDeleter<>();
        cache = getImpl(kCacheSize);
    }

    @Test
    public void testZeroSizeCache() {
        cache = getImpl(0);

        insert(1, 100);
        assertEquals(-1, lookup(1));
    }

    @Test
    public void testPrune() {
        insert(1, 100);
        insert(2, 200);

        CacheHandle<Integer> handle = cache.lookup(encodeKey(1));
        assertNotNull(handle);
        cache.prune();
        cache.release(handle);

        assertEquals(100, lookup(1));
        assertEquals(-1, lookup(2));
    }

    @Test
    public void testNewId() {
        long a = cache.newId();
        long b = cache.newId();
        assertNotEquals(a, b);
    }

    @Test
    public void testHeavyEntries() {
        // Add a bunch of light and heavy entries and then count the combined
        // size of items still in the cache, which must be approximately the
        // same as the total capacity.
        int kLight = 1;
        int kHeavy = 10;
        int added = 0;
        int index = 0;
        while (added < 2*kCacheSize) {
            int weight = ((index & 1) == 1) ? kLight : kHeavy;
            insert(index, 1000+index, weight);
            added += weight;
            index++;
        }

        int cached_weight = 0;
        for (int i = 0; i < index; i++) {
            int weight = ((i & 1) == 1) ? kLight : kHeavy;
            int r = lookup(i);
            if (r >= 0) {
                cached_weight += weight;
                assertEquals(1000+i, r);
            }
        }
        assertTrue(cached_weight <= kCacheSize + kCacheSize/10);
    }

    @Test
    public void testUseExceedsCacheSize() {
        // Overfill the cache, keeping handles on all inserted entries.
        Vector<CacheHandle<Integer>> h = new Vector<>();
        for (int i = 0; i < kCacheSize + 100; i++) {
            h.add(insertAndReturnHandle(1000+i, 2000+i));
        }

        // Check that all the entries can be found in the cache.
        for (int i = 0; i < h.size(); i++) {
            assertEquals(2000+i, lookup(1000+i));
        }

        for (int i = 0; i < h.size(); i++) {
            cache.release(h.get(i));
        }
    }

    @Test
    public void testEvictionPolicy() {
        insert(100, 101);
        insert(200, 201);
        insert(300, 301);
        CacheHandle<Integer> handle = cache.lookup(encodeKey(300));

        // Frequently used entry must be kept around,
        // as must things that are still in use.
        for (int i = 0; i < kCacheSize + 100; i++) {
            insert(1000+i, 2000+i);
            assertEquals(2000+i, lookup(1000+i));
            assertEquals(101, lookup(100));
        }
        assertEquals(101, lookup(100));
        assertEquals(-1, lookup(200));
        assertEquals(301, lookup(300));
        cache.release(handle);
    }

    @Test
    public void testEntriesArePinned() {
        insert(100, 101);
        CacheHandle<Integer> h1 = cache.lookup(encodeKey(100));
        assertEquals(101, cache.value(h1).intValue());

        insert(100, 102);
        CacheHandle<Integer> h2 = cache.lookup(encodeKey(100));
        assertEquals(102, cache.value(h2).intValue());
        assertEquals(0, deleter.deletedKeys.size());

        cache.release(h1);
        assertEquals(1, deleter.deletedKeys.size());
        assertEquals(100, deleter.deletedKeys.get(0).intValue());
        assertEquals(101, deleter.deletedValues.get(0).intValue());

        erase(100);
        assertEquals(-1, lookup(100));
        assertEquals(1, deleter.deletedKeys.size());

        cache.release(h2);
        assertEquals(2, deleter.deletedKeys.size());
        assertEquals(100, deleter.deletedKeys.get(1).intValue());
        assertEquals(102, deleter.deletedValues.get(1).intValue());
    }

    @Test
    public void testErase() {
        erase(200);
        assertEquals(0, deleter.deletedKeys.size());

        insert(100, 101);
        insert(200, 201);
        erase(100);
        assertEquals(-1,  lookup(100));
        assertEquals(201, lookup(200));
        assertEquals(1, deleter.deletedKeys.size());
        assertEquals(100, deleter.deletedKeys.get(0).intValue());
        assertEquals(101, deleter.deletedValues.get(0).intValue());

        erase(100);
        assertEquals(-1,  lookup(100));
        assertEquals(201, lookup(200));
        assertEquals(1, deleter.deletedKeys.size());
    }

    @Test
    public void testHitAndMiss() {
        assertEquals(-1, lookup(100));

        insert(100, 101);
        assertEquals(101, lookup(100));
        assertEquals(-1,  lookup(200));
        assertEquals(-1,  lookup(300));

        insert(200, 201);
        assertEquals(101, lookup(100));
        assertEquals(201, lookup(200));
        assertEquals(-1,  lookup(300));

        insert(100, 102);
        assertEquals(102, lookup(100));
        assertEquals(201, lookup(200));
        assertEquals(-1,  lookup(300));

        assertEquals(1, deleter.deletedKeys.size());
        assertEquals(100, deleter.deletedKeys.get(0).intValue());
        assertEquals(101, deleter.deletedValues.get(0).intValue());
    }

    private int lookup(int key) {
        CacheHandle<Integer> handle = cache.lookup(encodeKey(key));
        if (handle == null) {
            return -1;
        } else {
            cache.release(handle);
            return ((LRUCacheNode<Integer>) handle).getValue();
        }
    }

    private void insert(int key, int value) {
        cache.release(
                cache.insert(encodeKey(key), value, 1, deleter)
        );
    }

    private void insert(int key, int value, int charge) {
        cache.release(
                cache.insert(encodeKey(key), value, charge, deleter)
        );
    }

    private CacheHandle<Integer> insertAndReturnHandle(int key, int value) {
        return cache.insert(encodeKey(key), value, 1, deleter);
    }

    private String encodeKey(int key) {
        char[] buffer = new char[ICoding.getInstance().getFixed32Length()];
        ICoding.getInstance().encodeFixed32(buffer, 0, key);
        return new String(buffer);
    }

    public static int decodeKey(String s) {
        char[] buffer = s.toCharArray();
        return ICoding.getInstance().decodeFixed32(buffer, 0);
    }

    private void erase(int key) {
        cache.erase(encodeKey(key));
    }
}