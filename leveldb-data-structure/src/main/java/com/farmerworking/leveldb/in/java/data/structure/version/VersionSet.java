package com.farmerworking.leveldb.in.java.data.structure.version;

import com.farmerworking.leveldb.in.java.api.Options;
import com.farmerworking.leveldb.in.java.data.structure.cache.TableCache;
import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKeyComparator;
import lombok.Data;

@Data
public class VersionSet {
    private InternalKeyComparator internalKeyComparator;
    private Options options;
    private TableCache tableCache;
}
