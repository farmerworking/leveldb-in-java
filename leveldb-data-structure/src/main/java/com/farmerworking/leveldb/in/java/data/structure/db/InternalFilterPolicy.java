package com.farmerworking.leveldb.in.java.data.structure.db;

import com.farmerworking.leveldb.in.java.api.FilterPolicy;

import java.util.List;
import java.util.stream.Collectors;

public class InternalFilterPolicy implements FilterPolicy {

    private final FilterPolicy userFilterPolicy;

    public InternalFilterPolicy(FilterPolicy userFilterPolicy) {
        this.userFilterPolicy = userFilterPolicy;
    }

    @Override
    public String name() {
        return userFilterPolicy.name();
    }

    @Override
    public String createFilter(List<String> keys) {
        List<String> userKeys = keys.stream().map(this::extractUserKey).collect(Collectors.toList());
        return userFilterPolicy.createFilter(userKeys);
    }

    @Override
    public boolean keyMayMatch(String key, String filter) {
        return userFilterPolicy.keyMayMatch(extractUserKey(key), filter);
    }

    private String extractUserKey(String key) {
        return key.substring(0, key.length() - 8);
    }
}
