package com.farmerworking.leveldb.in.java.data.structure.db;

import com.farmerworking.leveldb.in.java.api.Iterator;
import com.farmerworking.leveldb.in.java.api.Options;
import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.data.structure.cache.ShardedLRUCache;
import com.farmerworking.leveldb.in.java.data.structure.cache.TableCache;
import com.farmerworking.leveldb.in.java.data.structure.log.*;
import com.farmerworking.leveldb.in.java.data.structure.memory.IMemtable;
import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKey;
import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKeyComparator;
import com.farmerworking.leveldb.in.java.data.structure.version.*;
import com.farmerworking.leveldb.in.java.data.structure.memory.Memtable;
import com.farmerworking.leveldb.in.java.data.structure.writebatch.MemTableInserter;
import com.farmerworking.leveldb.in.java.data.structure.writebatch.WriteBatch;
import com.farmerworking.leveldb.in.java.file.Env;
import com.farmerworking.leveldb.in.java.file.FileName;
import com.farmerworking.leveldb.in.java.file.SequentialFile;
import com.farmerworking.leveldb.in.java.file.WritableFile;
import javafx.util.Pair;
import lombok.Data;

import java.lang.reflect.Field;
import java.nio.channels.FileLock;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
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
    private ReentrantLock mutex;
    private AtomicBoolean shuttingDown;
    private Condition bgCondition; // Signalled when background work finishes
    private IMemtable memtable;
    private IMemtable immutableMemtable; // Memtable being compacted
    private AtomicBoolean hasImmutableMemtable; // So bg thread can detect non-NULL immutableMemtable
    private WritableFile logFile;
    private long logFileNumber;
    private ILogWriter log;
    private long seed; // For sampling

    // Set of table files to protect from deletion because they are
    // part of ongoing compactions.
    Set<Long> pendingOutputs = new HashSet<>();

    // Has a background compaction been scheduled or is running?
    private boolean bgCompactionScheduled;
    private ManualCompaction manualCompaction;

    private VersionSet versions;

    private CompactionStats[] stats = new CompactionStats[Config.kNumLevels];
    private Builder builder = new Builder();

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
        this.bgCompactionScheduled = false;
        this.manualCompaction = null;

        this.hasImmutableMemtable = new AtomicBoolean(false);
        int tableCacheSize = this.options.getMaxFileSize() - kNumNonTableCacheFiles;
        this.tableCache = new TableCache(dbname, this.options, tableCacheSize);

        this.versions = new VersionSet(this.dbname, this.options, this.tableCache, this.internalKeyComparator);

        for (int i = 0; i < Config.kNumLevels; i++) {
            this.stats[i] = new CompactionStats();
        }
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

    @Data
    class RecoverLogFileResult {
        private Status status;
        private boolean saveManifest;
        private Long maxSequence;

        public RecoverLogFileResult(Status status) {
            this.status = status;
        }

        public RecoverLogFileResult(Status status, boolean saveManifest, Long maxSequence) {
            this.status = status;
            this.saveManifest = saveManifest;
            this.maxSequence = maxSequence;
        }
    }

    RecoverLogFileResult recoverLogFile(long logFileNumber, boolean lastLog, VersionEdit edit) {
        assert this.mutex.isHeldByCurrentThread();

        String filename = FileName.logFileName(this.dbname, logFileNumber);
        Pair<Status, SequentialFile> pair = this.env.newSequentialFile(filename);
        Status status = pair.getKey();
        if (status.isNotOk()) {
            status = maybeIgnoreError(status);
            return new RecoverLogFileResult(status);
        }

        LogReporter logReporter = new LogReporter(this.options.getInfoLog(), filename, this.options.isParanoidChecks() ? status : null);
        // We intentionally make log::Reader do checksumming even if
        // paranoid_checks==false so that corruptions cause entire commits
        // to be skipped instead of propagating bad information (like overly
        // large sequence numbers).
        ILogReader logReader = new LogReader(pair.getValue(), logReporter, true, 0);
        Options.Logger.log(this.options.getInfoLog(), String.format("Recovering log %d", logFileNumber));

        Long maxSequence = null;
        boolean saveManifest = false;
        int compactions = 0;
        Memtable memtable = null;
        WriteBatch batch = new WriteBatch();

        // Read all the records and add to a memtable
        for(Pair<Boolean, String> read = readLogRecord(logReader);
            read.getKey() && status.isOk();
            read = readLogRecord(logReader)) {
            if (read.getValue().length() < WriteBatch.kHeaderSize) {
                logReporter.corruption(read.getValue().length(), Status.Corruption("log record too small"));
                continue;
            }

            batch.decode(read.getValue().toCharArray());
            if (memtable == null) {
                memtable = new Memtable(this.internalKeyComparator);
            }

            MemTableInserter memTableInserter = new MemTableInserter(batch.getSequence(), memtable);
            status = iterateBatch(batch, memTableInserter);
            status = maybeIgnoreError(status);
            if (status.isNotOk()) {
                break;
            }

            long lastSeq = batch.getSequence() + batch.getCount() - 1;
            if (maxSequence == null || lastSeq > maxSequence) {
                maxSequence = lastSeq;
            }

            if (memtable.approximateMemoryUsage() > getWriteBufferSize()) {
                compactions ++;
                saveManifest = true;
                status = writeLevel0Table(memtable, edit, null);
                memtable = null;
                if (status.isNotOk()) {
                    // Reflect errors immediately so that conditions like full
                    // file-systems cause the DB::Open() to fail.
                    break;
                }
            }
        }

        if (status.isOk() && this.options.isReuseLogs() && lastLog && compactions == 0) {
            assert this.logFile == null;
            assert this.log == null;
            assert this.memtable == null;

            Pair<Status, Long> fileSize = getFileSize(filename);
            Pair<Status, WritableFile> append = getAppendableFile(filename);
            if (fileSize.getKey().isOk() && append.getKey().isOk()) {
                this.logFile = append.getValue();
                Long logFileSize = fileSize.getValue();

                Options.Logger.log(this.options.getInfoLog(), String.format("Reusing old log %s", filename));
                this.log = new LogWriter(this.logFile, logFileSize);
                this.logFileNumber = logFileNumber;

                if (memtable != null) {
                    this.memtable = memtable;
                    memtable = null;
                } else {
                    // mem can be NULL if lognum exists but was empty.
                    this.memtable = new Memtable(this.internalKeyComparator);
                }
            }
        }

        if (memtable != null) {
            // mem did not get reused; compact it.
            if (status.isOk()) {
                saveManifest = true;
                status = writeLevel0Table(memtable, edit, null);
            }
        }

        return new RecoverLogFileResult(status, saveManifest, maxSequence);
    }

    Pair<Status, WritableFile> getAppendableFile(String filename) {
        return this.env.newAppendableFile(filename);
    }

    Pair<Status, Long> getFileSize(String filename) {
        return this.env.getFileSize(filename);
    }

    int getWriteBufferSize() {
        return this.options.getWriteBufferSize();
    }

    Status iterateBatch(WriteBatch batch, MemTableInserter memTableInserter) {
        return batch.iterate(memTableInserter);
    }

    Status writeLevel0Table(IMemtable memtable, VersionEdit edit, Version base) {
        assert this.mutex.isHeldByCurrentThread();
        long start = System.nanoTime();
        FileMetaData metaData = new FileMetaData();
        metaData.setFileNumber(this.versions.newFileNumber());

        this.pendingOutputs.add(metaData.getFileNumber());
        Iterator<InternalKey, String> iter = memtable.iterator();
        Options.Logger.log(this.options.getInfoLog(), String.format("Level-0 table %d: started", metaData.getFileNumber()));
        Status status;
        {
            unlock();
            status = builder.buildTable(this.dbname, this.env, this.options, this.tableCache, iter, metaData);
            lock();
        }

        Options.Logger.log(this.options.getInfoLog(), String.format("Level-0 table %d: %d bytes %s", metaData.getFileNumber(), metaData.getFileSize(), status.toString()));
        this.pendingOutputs.remove(metaData.getFileNumber());

        // Note that if file_size is zero, the file has been deleted and
        // should not be added to the manifest.
        int level = 0;
        if (status.isOk() && metaData.getFileSize() > 0) {
            String minUserKey = metaData.getSmallest().userKey;
            String maxUserKey = metaData.getLargest().userKey;

            if (base != null) {
                level = base.pickLevelForMemTableOutput(minUserKey, maxUserKey);
            }

            edit.addFile(level, metaData.getFileNumber(), metaData.getFileSize(), metaData.getSmallest(), metaData.getLargest());
        }

        CompactionStats stats = new CompactionStats();
        stats.setMicros(System.nanoTime() - start);
        stats.setBytesWritten(metaData.getFileSize());
        this.stats[level].add(stats);
        return status;
    }

    Status maybeIgnoreError(Status status) {
        if (status.isOk() || this.options.isParanoidChecks()) {
            return status; // no need to change
        } else {
            Options.Logger.log(this.options.getInfoLog(), String.format("Ignoring error %s", status.toString()));
            return Status.OK();
        }
    }

    void lock() {
        this.mutex.lock();
    }

    void unlock() {
        this.mutex.unlock();
    }

    Pair<Boolean, String> readLogRecord(ILogReader logReader) {
        return logReader.readRecord();
    }
}
