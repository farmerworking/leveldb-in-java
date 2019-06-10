package com.farmerworking.leveldb.in.java.api;

import lombok.Data;

@Data
public class Options {
    // Number of keys between restart points for delta encoding of keys.
    // This parameter can be changed dynamically.  Most clients should
    // leave this parameter alone.
    //
    // Default: 16
    private int blockRestartInterval = 16;

    // Comparator used to define the order of keys in the table.
    // Default: a comparator that uses lexicographic byte-wise ordering
    //
    // REQUIRES: The client must ensure that the comparator supplied
    // here has the same name and orders keys *exactly* the same as the
    // comparator provided to previous open calls on the same DB.
    private Comparator comparator = new BytewiseComparator();

    // If non-null, use the specified filter policy to reduce disk reads.
    // Many applications will benefit from passing the result of
    // NewBloomFilterPolicy() here.
    //
    // Default: nullptr
    private FilterPolicy filterPolicy;

    // Approximate size of user data packed per block.  Note that the
    // block size specified here corresponds to uncompressed data.  The
    // actual size of the unit read from disk may be smaller if
    // compression is enabled.  This parameter can be changed dynamically.
    //
    // Default: 4K
    private int blockSize = 4096;

    // Compress blocks using the specified compression algorithm.  This
    // parameter can be changed dynamically.
    //
    // Default: kSnappyCompression, which gives lightweight but fast
    // compression.
    //
    // Typical speeds of kSnappyCompression on an Intel(R) Core(TM)2 2.4GHz:
    //    ~200-500MB/s compression
    //    ~400-800MB/s decompression
    // Note that these speeds are significantly faster than most
    // persistent storage speeds, and therefore it is typically never
    // worth switching to kNoCompression.  Even if the input data is
    // incompressible, the kSnappyCompression implementation will
    // efficiently detect that and will switch to uncompressed mode.
    private CompressionType compression = CompressionType.kSnappyCompression;

    // If true, the implementation will do aggressive checking of the
    // data it is processing and will stop early if it detects any
    // errors.  This may have unforeseen ramifications: for example, a
    // corruption of one DB entry may cause a large number of entries to
    // become unreadable or for the entire DB to become unopenable.
    // Default: false
    private boolean paranoidChecks = false;

    // Control over blocks (user data is stored in a set of blocks, and
    // a block is the unit of reading from disk).

    // If non-null, use the specified cache for blocks.
    // If null, leveldb will automatically create and use an 8MB internal cache.
    // Default: nullptr
    private Cache blockCache = null;

    public Options() {}

    public Options(Options options) {
        this.blockRestartInterval = options.blockRestartInterval;
        this.comparator = options.comparator;
        this.filterPolicy = options.filterPolicy;
        this.blockSize = options.blockSize;
        this.compression = options.compression;
    }
}
