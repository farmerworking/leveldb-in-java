package com.farmerworking.leveldb.in.java.data.structure.table;

import com.farmerworking.leveldb.in.java.api.*;
import com.farmerworking.leveldb.in.java.data.structure.cache.ShardedLRUCache;
import com.farmerworking.leveldb.in.java.data.structure.cache.TestDeleter;
import com.farmerworking.leveldb.in.java.data.structure.harness.Harness;
import com.farmerworking.leveldb.in.java.common.TestUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class ITableTest {
    @Test
    public void testApproximateOffsetOfPlain() {
        TableConstructor constructor = new TableConstructor(new BytewiseComparator());
        constructor.add("k01", "hello");
        constructor.add("k02", "hello2");
        constructor.add("k03", StringUtils.repeat('x', 10000));
        constructor.add("k04", StringUtils.repeat('x', 200000));
        constructor.add("k05", StringUtils.repeat('x', 300000));
        constructor.add("k06", "hello3");
        constructor.add("k07", StringUtils.repeat('x', 100000));
        
        Options options = new Options();
        options.setBlockSize(1024);
        options.setCompression(CompressionType.kNoCompression);
        constructor.finish(options);

        assertTrue(between(constructor.approximateOffsetOf("abc"),       0,      0));
        assertTrue(between(constructor.approximateOffsetOf("k01"),       0,      0));
        assertTrue(between(constructor.approximateOffsetOf("k01a"),      0,      0));
        assertTrue(between(constructor.approximateOffsetOf("k02"),       0,      0));
        assertTrue(between(constructor.approximateOffsetOf("k03"),       0,      0));
        assertTrue(between(constructor.approximateOffsetOf("k04"),   10000,  11000));
        assertTrue(between(constructor.approximateOffsetOf("k04a"), 210000, 211000));
        assertTrue(between(constructor.approximateOffsetOf("k05"),  210000, 211000));
        assertTrue(between(constructor.approximateOffsetOf("k06"),  510000, 511000));
        assertTrue(between(constructor.approximateOffsetOf("k07"),  510000, 511000));
        assertTrue(between(constructor.approximateOffsetOf("xyz"),  610000, 612000));
    }

    @Test
    public void testApproximateOffsetOfCompressed() {
        TableConstructor constructor = new TableConstructor(new BytewiseComparator());
        constructor.add("k01", "hello");
        constructor.add("k02", TestUtils.compressibleString(0.25, 10000));
        constructor.add("k03", "hello3");
        constructor.add("k04", TestUtils.compressibleString(0.25, 10000));

        Options options = new Options();
        options.setBlockSize(1024);
        options.setCompression(CompressionType.kSnappyCompression);
        constructor.finish(options);

        // Expected upper and lower bounds of space used by compressible strings.
        int kSlop = 1000;  // Compressor effectiveness varies.
        int expected = 2500;  // 10000 * compression ratio (0.25)
        int min_z = expected - kSlop;
        int max_z = expected + kSlop;

        assertTrue(between(constructor.approximateOffsetOf("abc"), 0, kSlop));
        assertTrue(between(constructor.approximateOffsetOf("k01"), 0, kSlop));
        assertTrue(between(constructor.approximateOffsetOf("k02"), 0, kSlop));
        // Have now emitted a large compressible string, so adjust expected offset.
        assertTrue(between(constructor.approximateOffsetOf("k03"), min_z, max_z));
        assertTrue(between(constructor.approximateOffsetOf("k04"), min_z, max_z));
        // Have now emitted two large compressible strings, so adjust expected offset.
        assertTrue(between(constructor.approximateOffsetOf("xyz"), 2 * min_z, 2 * max_z));
    }

    @Test
    // before bugfix, iterator will report corrupted compressed block contents
    public void testUncompressNotIncludeCrcAndType() {
        TableConstructor constructor = new TableConstructor(new BytewiseComparator());
        constructor.add("k04", TestUtils.compressibleString(0.25, 10000));

        Options options = new Options();
        options.setBlockSize(1024);
        options.setCompression(CompressionType.kSnappyCompression);
        constructor.finish(options);

        Iterator<String, String> iterator = constructor.iterator();
        iterator.seekToFirst();
        assertTrue(iterator.status().toString(), iterator.status().isOk());
    }

    @Test
    public void testBlockCacheHandleRelease() {
        Cache cache = new ShardedLRUCache(10 * 2 * 4);

        Options options = new Options();
        options.setBlockSize(10);
        options.setBlockCache(cache);

        TableConstructor constructor = new TableConstructor(new BytewiseComparator(), options);
        for (int i = 0; i < 100; i++) {
            constructor.add(TestUtils.randomKey(5), TestUtils.randomString(10));
        }

        List<String> keys = constructor.finish(options);
        Map<String, String> data = constructor.getData();

        TestDeleter<Object> deleter = new TestDeleter<>();
        Iterator<String, String> iterator = constructor.iterator(deleter);
        Harness.testForwardScan(keys, data, iterator);

        assertFalse(deleter.deletedKeys.isEmpty());
    }

    static boolean between(long value, long low, long high) {
        boolean result = (value >= low) && (value <= high);
        if (!result) {
            System.out.println(String.format("Value %d is not in range [%d, %d]", value, low, high));
        }
        return result;
    }
}
