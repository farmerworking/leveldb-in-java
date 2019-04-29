package com.farmerworking.leveldb.in.java.api;

import java.util.List;

public interface FilterPolicy {
    // Return the name of this policy.  Note that if the filter encoding
    // changes in an incompatible way, the name returned by this method
    // must be changed.  Otherwise, old incompatible filters may be
    // passed to methods of this type.
    String name();

    // keys[0,n-1] contains a list of keys (potentially with duplicates)
    // that are ordered according to the user supplied comparator.
    // return a filter that summarizes keys[0,n-1].
    String createFilter(List<String> keys);

    // "filter" contains the data appended by a preceding call to
    // CreateFilter() on this class.  This method must return true if
    // the key was in the list of keys passed to CreateFilter().
    // This method may return true or false if the key was not on the
    // list, but it should aim to return false with a high probability.
    boolean keyMayMatch(String key, String filter);
}
