package com.farmerworking.leveldb.in.java.api;

import com.farmerworking.leveldb.in.java.file.Env;
import com.farmerworking.leveldb.in.java.file.impl.DefaultEnv;
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

    // Use the specified object to interact with the environment,
    // e.g. to read/write files, schedule background work, etc.
    // Default: Env::Default()
    private Env env = new DefaultEnv();

    // Leveldb will write up to this amount of bytes to a file before
    // switching to a new one.
    // Most clients should leave this parameter alone.  However if your
    // filesystem is more efficient with larger files, you could
    // consider increasing the value.  The downside will be longer
    // compactions and hence longer latency/performance hiccups.
    // Another reason to increase this parameter might be when you are
    // initially populating a large database.
    //
    // Default: 2MB
    private int maxFileSize = 2 << 20;

    // Any internal progress/error information generated by the db will
    // be written to info_log if it is non-NULL, or to a file stored
    // in the same directory as the DB contents if info_log is NULL.
    // Default: NULL
    private Logger infoLog;

    // EXPERIMENTAL: If true, append to existing MANIFEST and log files
    // when a database is opened.  This can significantly speed up open.
    //
    // Default: currently false, but may become true later.
    private boolean reuseLogs;

    // Amount of data to build up in memory (backed by an unsorted log
    // on disk) before converting to a sorted on-disk file.
    //
    // Larger values increase performance, especially during bulk loads.
    // Up to two write buffers may be held in memory at the same time,
    // so you may wish to adjust this parameter to control memory usage.
    // Also, a larger write buffer will result in a longer recovery time
    // the next time the database is opened.
    //
    // Default: 4MB
    int writeBufferSize = 4 << 20;

    // Number of open files that can be used by the DB.  You may need to
    // increase this if your database has a large working set (budget
    // one open file per 2MB of working set).
    //
    // Default: 1000
    int maxOpenFiles = 1000;

    // If true, the database will be created if it is missing.
    // Default: false
    boolean createIfMissing;

    // If true, an error is raised if the database already exists.
    // Default: false
    boolean errorIfExists;

    public interface Logger {
        public static void log(Logger logger, String msg, String ... args) {
            if (logger != null) {
                logger.log(msg, args);
            }

        }

        void log(String msg, String ... args);
    }

    public Options() {}

    public Options(Options options) {
        this.blockRestartInterval = options.blockRestartInterval;
        this.comparator = options.comparator;
        this.filterPolicy = options.filterPolicy;
        this.blockSize = options.blockSize;
        this.compression = options.compression;
        this.env = options.env;
        this.maxFileSize = options.maxFileSize;
        this.infoLog = options.infoLog;
        this.writeBufferSize = options.writeBufferSize;
        this.maxOpenFiles = options.maxOpenFiles;
        this.reuseLogs = options.reuseLogs;
        this.paranoidChecks = options.paranoidChecks;
        this.blockCache = options.blockCache;
        this.createIfMissing = options.createIfMissing;
        this.errorIfExists = options.errorIfExists;
    }
}
