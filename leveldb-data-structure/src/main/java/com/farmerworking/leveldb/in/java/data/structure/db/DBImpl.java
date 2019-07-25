package com.farmerworking.leveldb.in.java.data.structure.db;

import com.farmerworking.leveldb.in.java.api.Options;
import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.data.structure.cache.ShardedLRUCache;
import com.farmerworking.leveldb.in.java.data.structure.cache.TableCache;
import com.farmerworking.leveldb.in.java.data.structure.log.ILogWriter;
import com.farmerworking.leveldb.in.java.data.structure.log.LogWriter;
import com.farmerworking.leveldb.in.java.data.structure.memory.IMemtable;
import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKeyComparator;
import com.farmerworking.leveldb.in.java.data.structure.version.VersionEdit;
import com.farmerworking.leveldb.in.java.data.structure.version.VersionSet;
import com.farmerworking.leveldb.in.java.data.structure.writebatch.WriteBatch;
import com.farmerworking.leveldb.in.java.file.Env;
import com.farmerworking.leveldb.in.java.file.FileName;
import com.farmerworking.leveldb.in.java.file.WritableFile;
import javafx.util.Pair;
import lombok.Data;

import java.lang.reflect.Field;
import java.nio.channels.FileLock;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Data
public class DBImpl {
    static int NEW_DB_NEXT_FILE_NUMBER = 2;
    static int NEW_DB_LAST_SEQUENCE = 0;
    static int NEW_DB_LOG_NUMBER = 0;
    static int NEW_DB_MANIFEST_NUMBER = 1;

    private static int kNumNonTableCacheFiles = 10;

    private final Env env;
    private final InternalKeyComparator internalKeyComparator;
    private final InternalFilterPolicy internalFilterPolicy;
    private final Options options;
    private final String dbname;

    // table_cache_ provides its own synchronization
    private TableCache tableCache;

    // Lock over the persistent DB state.  Non-NULL iff successfully acquired.
    private FileLock dbLock;

    // State below is protected by mutex
    private Lock mutex;
    private AtomicBoolean shuttingDown;
    private Condition bgCondition; // Signalled when background work finishes
    private IMemtable memtable;
    private IMemtable immutableMemtable; // Memtable being compacted
    private AtomicBoolean hasImmutableMemtable; // So bg thread can detect non-NULL immutableMemtable
    private WritableFile logFile;
    private long logFileNumber;
    private ILogWriter log;
    private long seed; // For sampling
    private WriteBatch tmpBatch;

    // Has a background compaction been scheduled or is running?
    private boolean bgCompactionScheduled;
    private ManualCompaction manualCompaction;

    private VersionSet versions;

    public DBImpl(Options rawOptions, String dbname) {
        this.env = rawOptions.getEnv();
        this.internalKeyComparator = new InternalKeyComparator(rawOptions.getComparator());
        this.internalFilterPolicy = new InternalFilterPolicy(rawOptions.getFilterPolicy());
        this.options = sanitizeOptions(dbname, this.internalKeyComparator, this.internalFilterPolicy, rawOptions);
        this.dbname = dbname;
        this.dbLock = null;
        this.mutex = new ReentrantLock();
        this.shuttingDown = null;
        this.bgCondition = mutex.newCondition();
        this.memtable = null;
        this.immutableMemtable = null;
        this.logFile = null;
        this.logFileNumber = 0;
        this.log = null;
        this.seed = 0;
        this.tmpBatch = new WriteBatch();
        this.bgCompactionScheduled = false;
        this.manualCompaction = null;

        this.hasImmutableMemtable = new AtomicBoolean(false);
        int tableCacheSize = this.options.getMaxFileSize() - kNumNonTableCacheFiles;
        this.tableCache = new TableCache(dbname, this.options, tableCacheSize);

        this.versions = new VersionSet(this.dbname, this.options, this.tableCache, this.internalKeyComparator);
    }

    public Status newDB() {
        VersionEdit newDB = new VersionEdit();
        newDB.setComparatorName(this.internalKeyComparator.getUserComparator().name());
        newDB.setLogNumber(NEW_DB_LOG_NUMBER);
        newDB.setNextFileNumber(NEW_DB_NEXT_FILE_NUMBER);
        newDB.setLastSequence(NEW_DB_LAST_SEQUENCE);

        String manifest = FileName.descriptorFileName(this.dbname, NEW_DB_MANIFEST_NUMBER);
        Pair<Status, WritableFile> pair = this.env.newWritableFile(manifest);
        Status status = pair.getKey();

        if (status.isNotOk()) {
            return status;
        }

        {
            ILogWriter logWriter = newDBGetLogWriter(pair.getValue());
            StringBuilder builder = new StringBuilder();
            newDB.encodeTo(builder);
            status = logWriter.addRecord(builder.toString());
            if (status.isOk()) {
                status = pair.getValue().close();
            }
        }

        if (status.isOk()) {
            status = FileName.setCurrentFile(this.env, this.dbname, NEW_DB_MANIFEST_NUMBER);
        } else {
            this.env.delete(manifest);
        }

        return status;
    }

    LogWriter newDBGetLogWriter(WritableFile file) {
        return new LogWriter(file);
    }

    static Options sanitizeOptions(String dbname, InternalKeyComparator icmp, InternalFilterPolicy ipolicy, Options src) {
        Options result = new Options(src);
        result.setComparator(icmp);
        result.setFilterPolicy(src.getFilterPolicy() != null ? ipolicy : null);

        clipToRange(result, "maxOpenFiles",    64 + kNumNonTableCacheFiles, 50000);
        clipToRange(result, "writeBufferSize", 64<<10,                      1<<30);
        clipToRange(result, "maxFileSize",     1<<20,                       1<<30);
        clipToRange(result, "blockSize",        1<<10,                       4<<20);

        if (result.getInfoLog() == null) {
//             Open a log file in the same directory as the db
            src.getEnv().createDir(dbname);  // In case it does not exist
            src.getEnv().renameFile(FileName.infoLogFileName(dbname), FileName.oldInfoLogFileName(dbname));
            Pair<Status, Options.Logger> pair = src.getEnv().newLogger(FileName.infoLogFileName(dbname));
            Status status = pair.getKey();
            if (status.isOk()) {
                result.setInfoLog(pair.getValue());
            }
        }

        if (result.getBlockCache() == null) {
            result.setBlockCache(new ShardedLRUCache(8388608)); // 8 << 20
        }

        return result;
    }

    static void clipToRange(Options options, String fieldName, int min, int max) {
        assert min <= max;
        try {
            Field field = Options.class.getDeclaredField(fieldName);
            field.setAccessible(true);

            int fieldValue = (int) field.get(options);
            if (fieldValue > max) {
                field.set(options, max);
            }

            if (fieldValue < min) {
                field.set(options, min);
            }
        } catch (Exception e) {
            throw new RuntimeException("sanitize option field error", e);
        }
    }
}
