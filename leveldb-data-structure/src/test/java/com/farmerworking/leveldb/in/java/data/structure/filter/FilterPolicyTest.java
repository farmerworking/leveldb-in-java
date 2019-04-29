package com.farmerworking.leveldb.in.java.data.structure.filter;

import com.farmerworking.leveldb.in.java.api.FilterPolicy;
import com.farmerworking.leveldb.in.java.common.ICoding;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class FilterPolicyTest {
    protected abstract FilterPolicy getFilterPolicyImpl();

    @Test
    public void testEmpty() throws Exception {
        FilterPolicy bloomFilterPolicy = getFilterPolicyImpl();

        assertFalse(bloomFilterPolicy.keyMayMatch("hello", null));
        assertFalse(bloomFilterPolicy.keyMayMatch("world", ""));
    }

    @Test
    public void testSmall() throws Exception {
        FilterPolicy bloomFilterPolicy = getFilterPolicyImpl();

        String filter = bloomFilterPolicy.createFilter(Lists.newArrayList("hello", "world"));

        assertTrue(bloomFilterPolicy.keyMayMatch("hello", filter));
        assertTrue(bloomFilterPolicy.keyMayMatch("world", filter));
        assertFalse(bloomFilterPolicy.keyMayMatch("x", filter));
        assertFalse(bloomFilterPolicy.keyMayMatch("foo", filter));
    }

    @Test
    public void testVaryingLengths() {
        // Count number of filters that significantly exceed the false positive rate
        int mediocre_filters = 0;
        int good_filters = 0;

        for (int length = 1; length <= 10000; length = nextLength(length)) {
            FilterPolicy bloomFilterPolicy = getFilterPolicyImpl();
            List<String> keys = new ArrayList<>();
            for (int i = 0; i < length; i++) {
                keys.add(key(i));
            }
            String filter = bloomFilterPolicy.createFilter(keys);

            // All added keys must match
            for (int i = 0; i < length; i++) {
                assertTrue(bloomFilterPolicy.keyMayMatch(key(i), filter));
            }

            // Check false positive rate
            double rate = falsePositiveRate(bloomFilterPolicy, filter);
            System.out.println(String.format("False positives: %f @ length = %d ; bytes = %d", rate, length, filter.length()));
            assertTrue(rate <= 0.02);   // Must not be over 2%
            if (rate > 0.0125) {
                mediocre_filters++;  // Allowed, but not too often
            } else {
                good_filters++;
            }
        }
        System.out.println(String.format("Filters: %d good, %d mediocre", good_filters, mediocre_filters));
        assertTrue(mediocre_filters <= good_filters/5);
    }

    private int nextLength(int length) {
        if (length < 10) {
            length += 1;
        } else if (length < 100) {
            length += 10;
        } else if (length < 1000) {
            length += 100;
        } else {
            length += 1000;
        }
        return length;
    }

    private String key(int i) {
        char[] buffer = new char[4];
        ICoding.getInstance().encodeFixed32(buffer, 0, i);
        return new String(buffer);
    }

    private double falsePositiveRate(FilterPolicy bloomFilterPolicy, String filter) {
        int result = 0;
        for (int i = 0; i < 10000; i++) {
            if (bloomFilterPolicy.keyMayMatch(key(i + 1000000000), filter)) {
                result++;
            }
        }
        return result / 10000.0;
    }
}
