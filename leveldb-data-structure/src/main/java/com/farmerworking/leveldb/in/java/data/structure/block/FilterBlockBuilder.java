package com.farmerworking.leveldb.in.java.data.structure.block;

import com.farmerworking.leveldb.in.java.api.FilterPolicy;
import com.farmerworking.leveldb.in.java.common.ICoding;

import java.util.Vector;

public class FilterBlockBuilder implements IFilterBlockBuilder {
    // Generate new filter every 2KB of data
    private static int kFilterBaseLg = 11;
    private static int kFilterBase = 1 << kFilterBaseLg;

    private FilterPolicy policy;
    private Vector<Integer> filterOffsets;
    private Vector<String> tmpKeys;     // policy->createFilter() argument
    private StringBuilder result;       // Filter data computed so far

    public FilterBlockBuilder(FilterPolicy policy) {
        this.policy = policy;
        this.filterOffsets = new Vector<>();
        this.tmpKeys = new Vector<>();
        this.result = new StringBuilder();
    }

    @Override
    public void startBlock(long blockOffset) {
        long filterIndex = (blockOffset / kFilterBase);
        assert filterIndex >= filterOffsets.size();

        while (filterIndex > filterOffsets.size()) {
            generateFilter();
        }
    }

    @Override
    public void addKey(String key) {
        tmpKeys.add(key);
    }

    @Override
    public String finish() {
        if (!tmpKeys.isEmpty()) {
            generateFilter();
        }

        // Append array of per-filter offsets
        int arrayOffset = result.length();
        for (int i = 0; i < filterOffsets.size(); i++) {
            ICoding.getInstance().putFixed32(result, filterOffsets.get(i));
        }

        ICoding.getInstance().putFixed32(result, arrayOffset);
        result.append((char)kFilterBaseLg); // Save encoding parameter in result
        return result.toString();
    }

    private void generateFilter() {
        if (tmpKeys.size() == 0) {
            // Fast path if there are no keys for this filter
            filterOffsets.add(result.length());
            return;
        }

        // Generate filter for current set of keys and append to result.
        filterOffsets.add(result.length());
        result.append(policy.createFilter(tmpKeys));

        tmpKeys.clear();
    }
}
