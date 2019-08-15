package com.farmerworking.leveldb.in.java.data.structure.db;

import com.farmerworking.leveldb.in.java.api.Iterator;
import com.farmerworking.leveldb.in.java.api.Options;
import com.farmerworking.leveldb.in.java.api.ReadOptions;
import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.data.structure.cache.ShardedLRUCache;
import com.farmerworking.leveldb.in.java.data.structure.cache.TableCache;
import com.farmerworking.leveldb.in.java.data.structure.log.*;
import com.farmerworking.leveldb.in.java.data.structure.memory.*;
import com.farmerworking.leveldb.in.java.data.structure.table.TableBuilder;
import com.farmerworking.leveldb.in.java.data.structure.version.*;
import com.farmerworking.leveldb.in.java.file.*;
import javafx.util.Pair;
import lombok.Data;

import java.lang.reflect.Field;
import java.nio.channels.FileLock;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

@Data
public class DBImpl implements DB {
    static int NEW_DB_NEXT_FILE_NUMBER = 2;
    static int NEW_DB_LAST_SEQUENCE = 0;
    static int NEW_DB_LOG_NUMBER = 0;
    static int NEW_DB_MANIFEST_NUMBER = 1;

    private static int kNumNonTableCacheFiles = 10;

    private Env env;
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

    // Have we encountered a background error in paranoid mode?
    private Status bgError = Status.OK();

    public DBImpl(Options rawOptions, String dbname) {
        this.env = rawOptions.getEnv();
        this.internalKeyComparator = new InternalKeyComparator(rawOptions.getComparator());
        this.internalFilterPolicy = new InternalFilterPolicy(rawOptions.getFilterPolicy());
        this.options = sanitizeOptions(dbname, this.internalKeyComparator, this.internalFilterPolicy, rawOptions);
        this.dbname = dbname;
        this.dbLock = null;
        this.mutex = new ReentrantLock();
        this.shuttingDown = new AtomicBoolean(false);
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

    public Pair<Status, Boolean> recover(VersionEdit edit) {
        assert this.mutex.isHeldByCurrentThread();

        // Ignore error from CreateDir since the creation of the DB is
        // committed only when the descriptor is created, and this directory
        // may already exist from a previous failed creation attempt.
        env.createDir(this.dbname);
        assert this.dbLock == null;
        Pair<Status, FileLock> pair = lockFile();
        Status status = pair.getKey();
        if (status.isNotOk()) {
            return new Pair<>(status, null);
        }
        this.dbLock = pair.getValue();

        if (!this.env.isFileExists(FileName.currentFileName(this.dbname))) {
            if (this.options.isCreateIfMissing()) {
                status = newDB();
                if (status.isNotOk()) {
                    return new Pair<>(status, null);
                }
            } else {
                return new Pair<>(Status.InvalidArgument(this.dbname, "does not exist (createIfMissing is false)"), null);
            }
        } else {
            if (this.options.isErrorIfExists()) {
                return new Pair<>(Status.InvalidArgument(this.dbname, "exists (errorIfExists is true)"), null);
            }
        }

        Pair<Status, Boolean> recover = versionsRecover();
        status = recover.getKey();
        if (status.isNotOk()) {
            return new Pair<>(status, null);
        }

        Boolean saveManifest = recover.getValue();

        Pair<Status, List<String>> children = getChildren(this.dbname);
        status = children.getKey();
        if (status.isNotOk()) {
            return new Pair<>(status, null);
        }
        List<String> filenames = children.getValue();

        // Recover from all newer log files than the ones named in the
        // descriptor (new log files may have been added by the previous
        // incarnation without registering them in the descriptor).
        //
        // Note that PrevLogNumber() is no longer used, but we pay
        // attention to it in case we are recovering a database
        // produced by an older version of leveldb.
        long minLog = versions.getLogNumber();
        long prevLog = versions.getPrevLogNumber();

        Set<Long> liveTableFileNumbers = getLiveFiles();
        List<Long> logs = filterRecoverLog(minLog, prevLog, filenames, liveTableFileNumbers);

        if (!liveTableFileNumbers.isEmpty()) {
            return new Pair<>(Status.Corruption(String.format("%d missing file; e.g. %s", liveTableFileNumbers.size(), FileName.tableFileName(this.dbname, liveTableFileNumbers.iterator().next()))), null);
        }

        Pair<Status, Pair<Long, Boolean>> tmp = recoverLogFiles(logs, edit);
        status = tmp.getKey();
        if (status.isNotOk()) {
            return new Pair<>(status, null);
        }

        long maxSequence = tmp.getValue().getKey();
        saveManifest = saveManifest || tmp.getValue().getValue();

        if (this.versions.getLastSequence() < maxSequence) {
            this.versions.setLastSequence(maxSequence);
        }

        return new Pair<>(Status.OK(), saveManifest);
    }

    Pair<Status, Pair<Long, Boolean>> recoverLogFiles(List<Long> logs, VersionEdit edit) {
        assert this.mutex.isHeldByCurrentThread();

        long maxSequence = 0L;
        boolean saveManifest = false;

        for (int i = 0; i < logs.size(); i++) {
            RecoverLogFileResult recoverLogFileResult = recoverLogFile(logs.get(i), i == logs.size() - 1, edit);
            Status status = recoverLogFileResult.getStatus();
            if (status.isNotOk()) {
                return new Pair<>(status, null);
            }
            maxSequence = Math.max(maxSequence, recoverLogFileResult.getMaxSequence());
            saveManifest = saveManifest || recoverLogFileResult.isSaveManifest();

            // The previous incarnation may not have written any MANIFEST
            // records after allocating this log number.  So we manually
            // update the file number allocation counter in VersionSet.
            this.versions.markFileNumberUsed(logs.get(i));
        }

        return new Pair<>(Status.OK(), new Pair<>(maxSequence, saveManifest));
    }

    List<Long> filterRecoverLog(long minLog, long prevLog, List<String> filenames, Set<Long> liveTableFileNumbers) {
        assert this.mutex.isHeldByCurrentThread();

        List<Long> logs = new ArrayList<>();

        for (int i = 0; i < filenames.size(); i++) {
            Pair<Long, FileType> parse = FileName.parseFileName(filenames.get(i));

            if (parse != null) {
                liveTableFileNumbers.remove(parse.getKey());
                if (parse.getValue().equals(FileType.kLogFile) && isLogValid(parse.getKey(), minLog, prevLog)) {
                    logs.add(parse.getKey());
                }
            }
        }

        Collections.sort(logs);
        return logs;
    }

    boolean isLogValid(long logNumber, long minLog, long prevLog) {
        return logNumber >= minLog || logNumber == prevLog;
    }

    Set<Long> getLiveFiles() {
        return this.versions.getLiveFiles();
    }

    Pair<Status, List<String>> getChildren(String directory) {
        return this.env.getChildren(directory);
    }

    Pair<Status, Boolean> versionsRecover() {
        return this.versions.recover();
    }

    Pair<Status, FileLock> lockFile() {
        return this.env.lockFile(FileName.lockFileName(this.dbname));
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

    RecoverLogFileResult recoverLogFile(long logFileNumber, boolean lastLog, VersionEdit edit) {
        assert this.mutex.isHeldByCurrentThread();

        String filename = FileName.logFileName(this.dbname, logFileNumber);
        Pair<Status, SequentialFile> pair = this.env.newSequentialFile(filename);
        Status status = pair.getKey();
        if (status.isNotOk()) {
            status = maybeIgnoreError(status);
            return new RecoverLogFileResult(status);
        }

        Options.Logger.log(this.options.getInfoLog(), String.format("Recovering log %d", logFileNumber));
        Log2MemtableReader log2MemtableReader = getLog2MemtableReader(edit, filename, pair.getValue()).invoke();

        status = log2MemtableReader.getStatus();
        Long maxSequence = log2MemtableReader.getMaxSequence();
        boolean saveManifest = log2MemtableReader.isSaveManifest();
        int compactions = log2MemtableReader.getCompactions();
        Memtable memtable = log2MemtableReader.getMemtable();

        if (shouldReuseLog(status, lastLog, compactions)) {
            memtable = reuseLog(logFileNumber, filename, memtable);
        }

        // mem did not get reused; compact it.
        if (status.isOk() && memtable != null) {
            saveManifest = true;
            status = writeLevel0Table(memtable, edit, null);
        }

        return new RecoverLogFileResult(status, saveManifest, maxSequence);
    }

    Memtable reuseLog(long logFileNumber, String filename, Memtable memtable) {
        assert this.mutex.isHeldByCurrentThread();

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
        return memtable;
    }

    boolean shouldReuseLog(Status status, boolean lastLog, int compactions) {
        assert this.mutex.isHeldByCurrentThread();
        return status.isOk() && this.options.isReuseLogs() && lastLog && compactions == 0;
    }

    Pair<Status, WritableFile> getAppendableFile(String filename) {
        return this.env.newAppendableFile(filename);
    }

    Pair<Status, Long> getFileSize(String filename) {
        return this.env.getFileSize(filename);
    }


    Status writeLevel0Table(IMemtable memtable, VersionEdit edit, Version base) {
        assert this.mutex.isHeldByCurrentThread();
        long start = System.nanoTime();
        FileMetaData metaData = new FileMetaData();
        metaData.setFileNumber(this.versions.newFileNumber());

        this.pendingOutputs.add(metaData.getFileNumber());
        Iterator<String, String> iter = memtable.iterator();
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
            String minUserKey = metaData.getSmallest().userKey();
            String maxUserKey = metaData.getLargest().userKey();

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

    Log2MemtableReader getLog2MemtableReader(VersionEdit edit, String filename, SequentialFile file) {
        return new Log2MemtableReader(this, edit, filename, file);
    }

    public void deleteObsoleteFiles() {
        if (bgError.isNotOk()) {
            // After a background error, we don't know whether a new version may
            // or may not have been committed, so we cannot safely garbage collect.
            return;
        }

        // Make a set of all of the live files
        Set<Long> liveFiles = getLiveFiles();
        liveFiles.addAll(pendingOutputs);

        Pair<Status, List<String>> pair = getChildren(this.dbname);
        if (pair.getKey().isNotOk()) {
            return; // Ignoring errors on purpose
        }
        for (String filename : pair.getValue()) {
            Pair<Long, FileType> parse = FileName.parseFileName(filename);

            if (parse != null) {
                boolean keep = true;
                Long number = parse.getKey();
                FileType fileType = parse.getValue();

                switch (fileType) {
                    case kLogFile:
                        keep = isLogValid(number, this.versions.getLogNumber(), this.versions.getPrevLogNumber());
                        break;
                    case kDescriptorFile:
                        // Keep my manifest file, and any newer incarnations'
                        // (in case there is a race that allows other incarnations)
                        keep = (number >= this.versions.getManifestFileNumber());
                        break;
                    case kTableFile:
                        keep = liveFiles.contains(number);
                        break;
                    case kTempFile:
                        // Any temp files that are currently being written to must
                        // be recorded in pending_outputs_, which is inserted into "live"
                        keep = liveFiles.contains(number);
                        break;
                    case kCurrentFile:
                    case kDBLockFile:
                    case kInfoLogFile:
                        keep = true;
                        break;
                }

                if (!keep) {
                    if (fileType == FileType.kTableFile) {
                        tableCache.evict(number);
                    }
                    Options.Logger.log(this.options.getInfoLog(), String.format("Delete type=%d #%d", fileType.ordinal(), number));
                    this.env.delete(this.dbname + "/" + filename);
                }
            }
        }
    }

    public boolean maybeScheduleCompaction() {
        assert this.mutex.isHeldByCurrentThread();

        if (bgCompactionScheduled) {
            // already scheduled
            return false;
        } else if (shuttingDown.get()) {
            // DB is being deleted; no more background compactions
            return false;
        } else if (bgError.isNotOk()) {
            // Already got an error; no more changes
            return false;
        } else if (this.immutableMemtable == null && this.manualCompaction == null && !this.versions.needCompaction()) {
            // No work to be done
            return false;
        } else {
            bgCompactionScheduled = true;
            schedule();
            return true;
        }
    }

    boolean backgroundCall() {
        try {
            mutex.lock();
            assert this.bgCompactionScheduled;

            if (this.shuttingDown.get()) {
                // No more background work when shutting down.
                return false;
            } else if (this.bgError.isNotOk()) {
                // No more background work after a background error.
                return false;
            } else {
                backgroundCompaction();
            }

            this.bgCompactionScheduled = false;
            // Previous compaction may have produced too many files in a level,
            // so reschedule another compaction if needed.
            maybeScheduleCompaction();
            this.bgCondition.signalAll();
            return true;
        } finally {
            mutex.unlock();
        }
    }

    void compactMemtable() {
        assert this.mutex.isHeldByCurrentThread();
        assert this.immutableMemtable != null;

        VersionEdit edit = new VersionEdit();
        Version base = this.versions.getCurrent();
        base.ref();
        Status status = writeLevel0Table(this.immutableMemtable, edit, base);
        base.unref();

        if (status.isOk() && this.shuttingDown.get()) {
            status = Status.IOError("Deleting DB during memtable compaction");
        }

        // Replace immutable memtable with the generated Table
        if (status.isOk()) {
            edit.setPrevLogNumber(0);
            edit.setLogNumber(this.logFileNumber); // Earlier logs no longer needed
            status = logAndApply(edit);
        }

        if (status.isOk()) {
            this.immutableMemtable = null;
            this.hasImmutableMemtable.set(false);
            this.deleteObsoleteFiles();
        } else {
            recordBackgroundError(status);
        }
    }

    Status logAndApply(VersionEdit edit) {
        return this.versions.logAndApply(edit, this.mutex);
    }


    boolean stopDuringIterateCompactionInput(CompactionState compact, String key) {
        return compact.getCompaction().shouldStopBefore(key) && compact.getBuilder() != null;
    }

    // used during disk file compaction
    long compactMemtableFirst() {
        if (this.hasImmutableMemtable.get()) {
            long immutableMemtableStart = System.currentTimeMillis();
            try {
                this.mutex.lock();
                if (this.immutableMemtable != null) {
                    this.compactMemtable();
                    bgCondition.signalAll();
                }
            } finally {
                this.mutex.unlock();
            }
            return System.currentTimeMillis() - immutableMemtableStart;
        } else {
            return 0;
        }
    }

    boolean isEntryDroppable(CompactionState compact, IterateInputState state, Pair<Boolean, ParsedInternalKey> pair) {
        boolean drop = false;

        if (!pair.getKey()) {
            // Do not hide error keys
            state.currentUserKey = null;
            state.hasCurrentUserKey = false;
            state.lastSequenceForKey = InternalKey.kMaxSequenceNumber;
        } else {
            if (!state.hasCurrentUserKey || this.internalKeyComparator.getUserComparator().compare(pair.getValue().getUserKeyChar(), state.currentUserKey.toCharArray()) != 0) {
                // First occurrence of this user key
                state.currentUserKey = pair.getValue().getUserKey();
                state.hasCurrentUserKey = true;
                state.lastSequenceForKey = InternalKey.kMaxSequenceNumber;
            }

            if (state.lastSequenceForKey <= compact.getSmallestSnapshot()) {
                // Hidden by an newer entry for same user key
                drop = true;    // (A)
            } else if (pair.getValue().getValueType().equals(ValueType.kTypeDeletion) &&
                    pair.getValue().getSequence() <= compact.getSmallestSnapshot() &&
                    compact.getCompaction().isBaseLevelForKey(pair.getValue().getUserKey())) {
                // For this user key:
                // (1) there is no data in higher levels
                // (2) data in lower levels will have larger sequence numbers
                // (3) data in layers that are being compacted here and have
                //     smaller sequence numbers will be dropped in the next
                //     few iterations of this loop (by rule (A) above).
                // Therefore this deletion marker is obsolete and can be dropped.
                drop = true;
            }

            state.lastSequenceForKey = pair.getValue().getSequence();
        }
        return drop;
    }

    Status installCompactionResults(CompactionState compact) {
        assert this.mutex.isHeldByCurrentThread();
        Options.Logger.log(options.getInfoLog(), String.format("Compacted %d@%d + %d@%d files => %d bytes",
                compact.getCompaction().numInputFiles(0),
                compact.getCompaction().getLevel(),
                compact.getCompaction().numInputFiles(1),
                compact.getCompaction().getLevel() + 1,
                compact.getTotalBytes()));

        finalizeCompactionState(compact);
        return logAndApply(compact.getCompaction().getEdit());
    }

    void finalizeCompactionState(CompactionState compact) {
        compact.getCompaction().addInputDeletions(compact.getCompaction().getEdit());
        int level = compact.getCompaction().getLevel();
        for (int i = 0; i < compact.getOutputs().size(); i++) {
            Output out = compact.getOutputs().get(i);
            compact.getCompaction().getEdit().addFile(level + 1, out.getNumber(), out.getFileSize(), out.getSmallest(), out.getLargest());
        }
    }

    Status openCompactionOutputFile(CompactionState compact) {
        assert compact != null;
        assert compact.getBuilder() == null;

        long fileNumber;
        try {
            this.mutex.lock();
            fileNumber = this.versions.newFileNumber();
            this.pendingOutputs.add(fileNumber);
            Output out = new Output(fileNumber);
            compact.add(out);
        } finally {
            this.mutex.unlock();
        }

        String filename = FileName.tableFileName(this.dbname, fileNumber);
        Pair<Status, WritableFile> pair = newWritableFile(filename);
        if (pair.getKey().isOk()) {
            compact.setOutfile(pair.getValue());
            compact.setBuilder(new TableBuilder(this.options, compact.getOutfile()));
        }
        return pair.getKey();
    }

    Pair<Status, WritableFile> newWritableFile(String filename) {
        return env.newWritableFile(filename);
    }

    Status finishCompactionOutputFile(CompactionState compact, Iterator<String, String> input) {
        assert compact != null;
        assert compact.getOutfile() != null;
        assert compact.getBuilder() != null;

        long outputNumber = compact.currentOutput().getNumber();
        assert outputNumber != 0;

        // Check for iterator errors
        Status status = input.status();
        long currentEntries = compact.getBuilder().numEntries();
        if (status.isOk()) {
            status = compact.getBuilder().finish();
        } else {
            compact.getBuilder().abandon();
        }

        long currentBytes = compact.getBuilder().fileSize();
        compact.currentOutput().setFileSize(currentBytes);
        compact.addBytes(currentBytes);
        compact.setBuilder(null);

        // Finish and check for file errors
        if (status.isOk()) {
            status = compact.getOutfile().sync();
        }

        if (status.isOk()) {
            status = compact.getOutfile().close();
        }
        compact.setOutfile(null);

        if (status.isOk() && currentEntries > 0) {
            // Verify that the table is usable
            Iterator<String, String> iter = this.tableCache.iterator(new ReadOptions(), outputNumber, currentBytes).getKey();
            status = iter.status();
            if (status.isOk()) {
                Options.Logger.log(options.getInfoLog(), String.format("Generated table #%d@%d: %d keys, %d bytes",
                        outputNumber, compact.getCompaction().getLevel(), currentEntries, currentBytes));
            }
        }

        return status;
    }

    void cleanupCompaction(CompactionState compactionState) {
        assert this.mutex.isHeldByCurrentThread();

        if (compactionState.getBuilder() != null) {
            // May happen if we get a shutdown call in the middle of compaction
            compactionState.getBuilder().abandon();
        } else {
            assert compactionState.getOutfile() == null;
        }

        for (int i = 0; i < compactionState.getOutputs().size(); i++) {
            Output output = compactionState.getOutputs().get(i);
            pendingOutputs.remove(output.getNumber());
        }
    }

    boolean isManualCompaction() {
        return this.manualCompaction != null;
    }

    void recordBackgroundError(Status status) {
        assert this.mutex.isHeldByCurrentThread();
        if (this.bgError.isOk()) {
            this.bgError = status;
            this.bgCondition.signalAll();
        }
    }

    void schedule() {
        env.schedule(new Runnable() {
            @Override
            public void run() {
                backgroundCall();
            }
        });
    }
}
