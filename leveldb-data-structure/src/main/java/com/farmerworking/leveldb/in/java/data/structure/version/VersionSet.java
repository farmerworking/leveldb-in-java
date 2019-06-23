package com.farmerworking.leveldb.in.java.data.structure.version;

import com.farmerworking.leveldb.in.java.api.Options;
import com.farmerworking.leveldb.in.java.data.structure.cache.TableCache;
import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKeyComparator;

public class VersionSet {
    private InternalKeyComparator internalKeyComparator;
    private Options options;
    private TableCache tableCache;

    public InternalKeyComparator getInternalKeyComparator() {
        return internalKeyComparator;
    }

    public Options getOptions() {
        return options;
    }

    public TableCache getTableCache() {
        return tableCache;
    }
}
