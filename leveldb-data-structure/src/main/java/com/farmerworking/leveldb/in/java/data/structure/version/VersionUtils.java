package com.farmerworking.leveldb.in.java.data.structure.version;

import com.farmerworking.leveldb.in.java.api.Options;

import java.util.Vector;

class VersionUtils {
    static long totalFileSize(Vector<FileMetaData> files) {
        return files.stream().mapToLong(FileMetaData::getFileSize).sum();
    }

    static long targetFileSize(Options options) {
        return options.getMaxFileSize();
    }

    static long maxFileSizeForLevel(Options options, int level) {
        // We could vary per level to reduce number of files?
        return targetFileSize(options);
    }

    // Maximum bytes of overlaps in grandparent (i.e., level+2) before we
    // stop building a single file in a level->level+1 compaction.
    static long maxGrandParentOverlapBytes(Options options) {
        return 10 * targetFileSize(options);
    }
}
