package com.farmerworking.leveldb.in.java.data.structure.db;

import com.farmerworking.leveldb.in.java.api.*;
import com.farmerworking.leveldb.in.java.api.Iterator;
import com.farmerworking.leveldb.in.java.data.structure.cache.ShardedLRUCache;
import com.farmerworking.leveldb.in.java.data.structure.cache.TableCache;
import com.farmerworking.leveldb.in.java.data.structure.log.*;
import com.farmerworking.leveldb.in.java.data.structure.memory.*;
import com.farmerworking.leveldb.in.java.data.structure.table.TableBuilder;
import com.farmerworking.leveldb.in.java.data.structure.version.*;
import com.farmerworking.leveldb.in.java.data.structure.writebatch.MemTableInserter;
import com.farmerworking.leveldb.in.java.data.structure.writebatch.WriteBatch;
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

    private Deque<Writer> writerList;
    private WriteBatch tmpBatch;

    private LinkedList<Long> snapshots = new LinkedList<>();
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
        this.writerList = new ArrayDeque<>();
        this.tmpBatch = new WriteBatch();

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

    public Pair<Status, String> get(ReadOptions readOptions, String key) {
        Status status = Status.OK();
        String result = null;
        try {
            this.mutex.lock();
            Long snapshot;
            if (readOptions.getSnapshot() != null) {
                snapshot = readOptions.getSnapshot();
            } else {
                snapshot = this.versions.getLastSequence();
            }

            IMemtable localMemtable = this.memtable;
            IMemtable localImmutable = this.immutableMemtable;
            Version localCurrentVersion = this.versions.getCurrent();
            localCurrentVersion.ref();

            boolean haveStatUpdate = false;
            GetStats getStats = new GetStats();

            // Unlock while reading from files and memtables
            try {
                this.mutex.unlock();
                Pair<Boolean, Pair<Status, String>> memtableGet = localMemtable.get(key, snapshot);
                if (memtableGet.getKey()) {
                    status = memtableGet.getValue().getKey();
                    result = memtableGet.getValue().getValue();
                } else if (localImmutable != null) {
                    Pair<Boolean, Pair<Status, String>> immutableMemtableGet = localImmutable.get(key, snapshot);

                    if (immutableMemtableGet.getKey()) {
                        status = immutableMemtableGet.getValue().getKey();
                        result = immutableMemtableGet.getValue().getValue();
                    } else {
                        Pair<Status, String> versionGet = localCurrentVersion.get(readOptions, new InternalKey(key, snapshot), getStats);
                        status = versionGet.getKey();
                        result = versionGet.getValue();
                        haveStatUpdate = true;
                    }
                } else {
                    Pair<Status, String> versionGet = localCurrentVersion.get(readOptions, new InternalKey(key, snapshot), getStats);
                    status = versionGet.getKey();
                    result = versionGet.getValue();
                    haveStatUpdate = true;
                }
            } finally {
                this.mutex.lock();
            }

            if (haveStatUpdate && localCurrentVersion.updateStats(getStats)) {
                maybeScheduleCompaction();
            }
            localCurrentVersion.unref();
            return new Pair<>(status, result);
        } finally {
            this.mutex.unlock();
        }
    }

    @Override
    public void close() {
        try {
            this.mutex.lock();
            this.shuttingDown.set(true);
            while(this.bgCompactionScheduled) {
                try {
                    this.bgCondition.await();
                } catch (Exception e){
                }
            }
        } finally {
            this.mutex.unlock();
        }

        if (this.dbLock != null) {
            this.env.unlockFile(FileName.lockFileName(this.dbname), this.dbLock);
        }
    }

    public Status put(WriteOptions writeOptions, String key, String value) {
        WriteBatch batch = new WriteBatch();
        batch.put(key, value);
        return write(writeOptions, batch);
    }

    public Status delete(WriteOptions writeOptions, String key) {
        WriteBatch batch = new WriteBatch();
        batch.delete(key);
        return write(writeOptions, batch);
    }

    @Override
    public Pair<Boolean, String> getProperty(String property) {
        try {
            this.mutex.lock();
            String prefix = "leveldb.";
            if (!property.startsWith(prefix)) {
                return new Pair<>(false, null);
            }
            String suffix = property.substring(prefix.length(), property.length());

            if (suffix.equals("approximate-memory-usage")) {
                int totalUsage = options.getBlockCache().totalCharge();
                if (this.memtable != null) {
                    totalUsage += this.memtable.approximateMemoryUsage();
                }

                if (this.immutableMemtable != null) {
                    totalUsage += this.immutableMemtable.approximateMemoryUsage();
                }

                return new Pair<>(true, String.valueOf(totalUsage));
            }

            if (suffix.startsWith("num-files-at-level")) {
                suffix = suffix.substring("num-files-at-level".length(), suffix.length());
                boolean ok = true;
                Integer level = null;
                try {
                    level = Integer.parseInt(suffix);
                } catch (NumberFormatException e) {
                    ok = false;
                }

                if (!ok || level > Config.kNumLevels) {
                    return new Pair<>(false, null);
                } else {
                    return new Pair<>(true, String.valueOf(this.versions.numLevelFiles(level)));
                }
            }

            return new Pair<>(false, null);
        } finally {
            this.mutex.unlock();
        }
    }

    public Status write(WriteOptions writeOptions, WriteBatch batch) {
        Writer writer = new Writer(this.mutex);
        writer.setBatch(batch);
        writer.setSync(writeOptions.isSync());
        writer.setDone(false);

        try {
            this.mutex.lock();
            this.writerList.add(writer);
            while(writer.isNotDone() && writer != this.writerList.peekFirst()) {
                try {
                    writer.getCondition().await();
                } catch (InterruptedException e) {
                    writer.setDone(true);
                    writer.setStatus(Status.IOError("interrupted"));
                    break;
                }
            }

            if (writer.isDone()) {
                return writer.getStatus();
            }

            // May temporarily unlock and wait.
            Status status = makeRoomForWrite(batch == null);
            long lastSequence = this.versions.getLastSequence();
            Writer lastWriter = writer;
            if (status.isOk() && batch != null) { // NULL batch is for compactions
                Pair<WriteBatch, Writer> pair = buildBatchGroup();
                WriteBatch updates = pair.getKey();
                lastWriter = pair.getValue();
                updates.setSequence(lastSequence + 1);
                lastSequence += updates.getCount();

                // Add to log and apply to memtable.  We can release the lock
                // during this phase since &w is currently responsible for logging
                // and protects against concurrent loggers and concurrent writes
                // into mem_.
                {
                    this.mutex.unlock();
                    status = this.log.addRecord(new String(updates.encode()));
                    boolean syncError = false;
                    if (status.isOk() && writeOptions.isSync()) {
                       status = this.logFile.sync();
                       if (status.isNotOk()) {
                           syncError = true;
                       }
                    }
                    if (status.isOk()) {
                        status = updates.iterate(new MemTableInserter(updates.getSequence(), this.memtable));
                    }
                    this.mutex.lock();
                    if (syncError) {
                        // The state of the log file is indeterminate: the log record we
                        // just added may or may not show up when the DB is re-opened.
                        // So we force the DB into a mode where all future writes fail.
                        recordBackgroundError(status);
                    }
                }

                if (updates == tmpBatch) {
                    tmpBatch.clear();
                }

                this.versions.setLastSequence(lastSequence);
            }

            while(true) {
                Writer ready = this.writerList.peekFirst();
                this.writerList.pop();
                if (ready != writer) {
                    ready.setStatus(status);
                    ready.setDone(true);
                    ready.getCondition().signal();
                }

                if (ready == lastWriter) {
                    break;
                }
            }

            if (!this.writerList.isEmpty()) {
                this.writerList.peekFirst().getCondition().signal();
            }

            return status;
        } finally {
            this.mutex.unlock();
        }
    }

    public long getSnapshot() {
        try {
            this.mutex.lock();
            long result = this.versions.getLastSequence();
            this.snapshots.add(result);
            return result;
        } finally {
            this.mutex.unlock();
        }
    }

    public void releaseSnapshot(long snapshot) {
        try {
            this.mutex.lock();
            this.snapshots.remove(snapshot);
        } finally {
            this.mutex.unlock();
        }
    }

    @Override
    public Iterator<String, String> iterator(ReadOptions readOptions) {
        Pair<Iterator<String, String>, Pair<Long, Long>> pair = internalIterator(readOptions);
        return new DBIterator(this, this.internalKeyComparator.getUserComparator(), pair.getKey() ,
            (readOptions.getSnapshot() != null ? readOptions.getSnapshot() : pair.getValue().getKey()),
            pair.getValue().getValue());
    }

    public void recordReadSample(String key) {
        try {
            this.mutex.lock();
            if (this.versions.getCurrent().recordReadSample(key)) {
                maybeScheduleCompaction();
            }
        } finally {
            this.mutex.unlock();
        }
    }

    @Data
    class IterState {
        private ReentrantLock mutex;
        private Version version;
        private IMemtable memtable;
        private IMemtable immutableMemtable;
    }

    private Pair<Iterator<String, String>, Pair<Long, Long>> internalIterator(ReadOptions readOptions) {
        IterState iterState = new IterState();
        this.mutex.lock();
        long latestSnapshot = this.versions.getLastSequence();

        // Collect together all needed child iterators
        Vector<Iterator<String, String>> list = new Vector<>();
        list.add(this.memtable.iterator());
        if (this.immutableMemtable != null) {
            list.add(this.immutableMemtable.iterator());
        }
        list.addAll(this.versions.getCurrent().iterators(readOptions));
        MergingIterator internalIterator = new MergingIterator(this.internalKeyComparator, list);
        this.versions.getCurrent().ref();

        iterState.setMutex(this.mutex);
        iterState.setMemtable(this.memtable);
        iterState.setImmutableMemtable(this.immutableMemtable);
        iterState.setVersion(this.versions.getCurrent());
        internalIterator.registerCleanup(new Runnable() {
            @Override
            public void run() {
                iterState.getMutex().lock();
                iterState.getVersion().unref();
                iterState.getMutex().unlock();
            }
        });

        long returnSeed = ++this.seed;
        this.mutex.unlock();
        return new Pair<>(internalIterator, new Pair<>(latestSnapshot, returnSeed));
    }

    Pair<WriteBatch, Writer> buildBatchGroup() {
        assert !this.writerList.isEmpty();
        Writer first = this.writerList.peekFirst();
        WriteBatch result = first.getBatch();
        assert result != null;

        int size = first.getBatch().approximateSize();
        // Allow the group to grow up to a maximum size, but if the
        // original write is small, limit the growth so we do not slow
        // down the small write too much.
        int maxSize = 1 << 20;
        if (size <= (128 << 10)) {
            maxSize = size + 128 << 10;
        }

        Writer lastWriter = first;
        java.util.Iterator<Writer> iter = this.writerList.iterator();
        iter.next();  // Advance past "first"
        while(iter.hasNext()) {
            Writer writer = iter.next();

            if (writer.isDone()) {
                continue;
            }

            if (writer.isSync() && first.isNotSync()) {
                // Do not include a sync write into a batch handled by a non-sync write.
                break;
            }

            if (writer.getBatch() != null) {
                size += writer.getBatch().approximateSize();
                if (size > maxSize) {
                    // Do not make batch too big
                    break;
                }

                // Append to *result
                if (result == first.getBatch()) {
                    // Switch to temporary batch instead of disturbing caller's batch
                    result = tmpBatch;
                    assert result.getCount() == 0;
                    result.append(first.getBatch());
                }

                result.append(writer.getBatch());
            }
            lastWriter = writer;
        }
        return new Pair<>(result, lastWriter);
    }

    public long TEST_maxNextLevelOverlappingBytes() {
        try {
            this.mutex.lock();
            return this.versions.maxNextLevelOverlappingBytes();
        } finally {
            this.mutex.unlock();
        }
    }

    public Status TEST_compactMemtable() {
       Status s = write(new WriteOptions(), null);
       if (s.isOk()) {
           try {
               this.mutex.lock();
               while(this.immutableMemtable != null && bgError.isOk()) {
                   try {
                       bgCondition.await();
                   } catch (InterruptedException e) {
                   }
               }

               if (this.immutableMemtable != null) {
                   s = bgError;
               }
           } finally {
               this.mutex.unlock();
           }
       }

       return s;
    }

    public Status makeRoomForWrite(boolean force) {
        assert this.mutex.isHeldByCurrentThread();
        assert !this.writerList.isEmpty();
        boolean allowDelay = !force;
        Status status = Status.OK();

        while(true) {
            if (this.bgError.isNotOk()) {
                status = this.bgError;
                break;
            } else if (allowDelay && this.versions.numLevelFiles(0) >= Config.kL0_SlowdownWritesTrigger) {
                // We are getting close to hitting a hard limit on the number of
                // L0 files.  Rather than delaying a single write by several
                // seconds when we hit the hard limit, start delaying each
                // individual write by 1ms to reduce latency variance.  Also,
                // this delay hands over some CPU to the compaction thread in
                // case it is sharing the same core as the writer.
                this.mutex.unlock();
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    status = Status.IOError("thread sleep interrupted");
                    break;
                }
                allowDelay = false; // Do not delay a single write more than once
                this.mutex.lock();
            } else if (!force && this.memtable.approximateMemoryUsage() <= this.options.getWriteBufferSize()) {
                // There is room in current memtable
                break;
            } else if (this.immutableMemtable != null) {
                // We have filled up the current memtable, but the previous
                // one is still being compacted, so we wait.
                Options.Logger.log(this.options.getInfoLog(), "Current memtable full; waiting...\n");
                try {
                    this.bgCondition.await();
                } catch (InterruptedException e) {
                    status = Status.IOError("bgCondition await interrupted");
                    break;
                }
            } else if (this.versions.numLevelFiles(0) >= Config.kL0_StopWritesTrigger) {
                // There are too many level-0 files.
                Options.Logger.log(this.options.getInfoLog(), "Too many L0 files; waiting...\n");
                try {
                    this.bgCondition.await();
                } catch (InterruptedException e) {
                    status = Status.IOError("bgCondition await interrupted");
                    break;
                }
            } else {
                // Attempt to switch to a new memtable and trigger compaction of old
                assert this.versions.getPrevLogNumber() == 0;
                long newLogNumber = this.versions.getNextFileNumber();
                Pair<Status, WritableFile> writable = this.env.newWritableFile(FileName.logFileName(this.dbname, newLogNumber));
                status = writable.getKey();
                if (status.isNotOk()) {
                    // Avoid chewing through file number space in a tight loop.
                    this.versions.reuseFileNumber(newLogNumber);
                    break;
                }
                this.logFile = writable.getValue();
                this.logFileNumber = newLogNumber;
                this.log = new LogWriter(this.logFile);
                this.immutableMemtable = this.memtable;
                this.hasImmutableMemtable.set(true);
                this.memtable = new Memtable(this.internalKeyComparator);
                force = false; // Do not force another compaction if have room
                this.maybeScheduleCompaction();
            }
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
        long start = System.currentTimeMillis();
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
        stats.setMicros(System.currentTimeMillis() - start);
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

    void backgroundCompaction() {
        assert this.mutex.isHeldByCurrentThread();

        if (this.immutableMemtable != null) {
            this.compactMemtable();
            return;
        }

        boolean isManual = isManualCompaction();
        Pair<Compaction, InternalKey> pair = pickCompaction(isManual);
        Compaction compaction = pair.getKey();
        InternalKey manualEnd = pair.getValue();

        Status status = doBackgroundCompaction(isManual, compaction);

        if (status.isOk()) {
            // Done
        } else if (this.shuttingDown.get()) {
            // Ignore compaction errors found during shutting down
        } else {
            Options.Logger.log(options.getInfoLog(), String.format("Compaction error: %s", status.toString()));
        }

        if (isManual) {
            if (status.isNotOk()) {
                // mark manual compaction done if exception happens
                this.manualCompaction.setDone(true);
            }

            if (!this.manualCompaction.isDone()) {
                // We only compacted part of the requested range.  Update *m
                // to the range that is left to be compacted.
                this.manualCompaction.setTmpStorage(manualEnd);
                this.manualCompaction.setBegin(manualEnd);
            }
            this.manualCompaction = null;
        }
    }

    Status doBackgroundCompaction(boolean isManual, Compaction compaction) {
        Status status = Status.OK();
        if (compaction == null) {
            // Nothing to do
        } else if (!isManual && compaction.isTrivialMove()) {
            // Move file to next level
            assert compaction.numInputFiles(0) == 1;
            FileMetaData metaData = compaction.input(0, 0);
            compaction.getEdit().deleteFile(compaction.getLevel(), metaData.getFileNumber());
            compaction.getEdit().addFile(compaction.getLevel() + 1, metaData.getFileNumber(), metaData.getFileSize(), metaData.getSmallest(), metaData.getLargest());
            status = logAndApply(compaction.getEdit());
            if (status.isNotOk()) {
                recordBackgroundError(status);
            }
            Options.Logger.log(this.options.getInfoLog(), String.format("Moved #%d to level-%d %d bytes %s: %s",
                    metaData.getFileNumber(),
                    compaction.getLevel() + 1,
                    metaData.getFileSize(),
                    status.toString(),
                    this.versions.levelSummary()));
        } else {
            CompactionState compact = new CompactionState(compaction);
            status = doCompactionWork(compact);
            if (status.isNotOk()) {
                recordBackgroundError(status);
            }
            compaction.releaseInputs();
            cleanupCompaction(compact);
            deleteObsoleteFiles();
        }
        return status;
    }

    Pair<Compaction, InternalKey> pickCompaction(boolean isManual) {
        Compaction compaction;
        InternalKey manualEnd = null;
        if (isManual) {
            compaction = this.versions.compactRange(
                    this.manualCompaction.getLevel(),
                    this.manualCompaction.getBegin(),
                    this.manualCompaction.getEnd());

            this.manualCompaction.setDone(compaction == null);
            if (compaction != null) {
                manualEnd = compaction.input(0, compaction.numInputFiles(0) - 1).getLargest();
            }
            Options.Logger.log(this.options.getInfoLog(), String.format("Manual compaction at level-%d from %s .. %s; will stop at %s",
                    this.manualCompaction.getLevel(),
                    (this.manualCompaction.getBegin() != null ? this.manualCompaction.getBegin().toString() : "(begin)"),
                    (this.manualCompaction.getEnd() != null ? this.manualCompaction.getEnd().toString() : "(end)"),
                    (this.manualCompaction.isDone() ? "(end)" : manualEnd.toString())));

        } else {
            compaction = this.versions.pickCompaction();
        }

        return new Pair<>(compaction, manualEnd);
    }

    public void compactRange(String begin, String end) {
        int maxLevelWithFiles = 1;
        try {
            this.mutex.lock();
            Version base = this.versions.getCurrent();
            for (int level = 0; level < Config.kNumLevels; level++) {
                if (base.overlapInLevel(level, begin, end)) {
                    maxLevelWithFiles = level;
                }
            }
        } finally {
            this.mutex.unlock();
        }
        this.TEST_compactMemtable();
        for (int level = 0; level < maxLevelWithFiles; level++) {
            this.TEST_compactRange(level, begin, end);
        }
    }

    public void TEST_compactRange(int level, String begin, String end) {
        assert level >= 0;
        assert level + 1 < Config.kNumLevels;

        ManualCompaction manual = new ManualCompaction();
        manual.setLevel(level);
        manual.setDone(false);
        if (begin == null) {
            manual.setBegin(null);
        } else {
            manual.setBegin(new InternalKey(begin, InternalKey.kMaxSequenceNumber, ValueType.kValueTypeForSeek));
        }

        if (end == null) {
            manual.setEnd(null);
        } else {
            manual.setEnd(new InternalKey(begin, 0, ValueType.kTypeDeletion));
        }

        try {
            this.mutex.lock();
            while (!manual.isDone() && !this.shuttingDown.get() && this.bgError.isOk()) {
                if (this.manualCompaction == null) { // idle
                    this.manualCompaction = manual;
                    maybeScheduleCompaction();
                } else {
                    // Running either my compaction or another compaction.
                    try {
                        bgCondition.await();
                    } catch (InterruptedException e) {
                    }
                }
            }

            if (this.manualCompaction == manual) {
                // Cancel my manual compaction since we aborted early for some reason.
                this.manualCompaction = null;
            }
        } finally {
            this.mutex.unlock();
        }
    }

    Status doCompactionWork(CompactionState compact) {
        long startMicros = System.currentTimeMillis();

        Options.Logger.log(this.options.getInfoLog(), String.format("Compacting %d@%d + %d@%d files",
                compact.getCompaction().numInputFiles(0),
                compact.getCompaction().getLevel(),
                compact.getCompaction().numInputFiles(1),
                compact.getCompaction().getLevel() + 1));

        assert numLevelFiles(compact.getCompaction().getLevel()) > 0;
        assert compact.getBuilder() == null;
        assert compact.getOutfile() == null;

        if (snapshots.isEmpty()) {
            compact.setSmallestSnapshot(this.versions.getLastSequence());
        } else {
            compact.setSmallestSnapshot(snapshots.getFirst());
        }

        // Release mutex while we're actually doing the compaction work
        CompactionStats stats = new CompactionStats();

        this.mutex.unlock();
        Status status = actualCompact(compact, startMicros, stats);
        this.mutex.lock();

        this.stats[compact.getCompaction().getLevel() + 1].add(stats);

        if (status.isOk()) {
            status = installCompactionResults(compact);
        }

        if (status.isNotOk()) {
            recordBackgroundError(status);
        }

        Options.Logger.log(options.getInfoLog(), String.format("compacted to: %s", versions.levelSummary()));
        return status;
    }

    public int numLevelFiles(int level) {
        return this.versions.numLevelFiles(level);
    }

    Status actualCompact(CompactionState compact, long startMicros, CompactionStats stats) {
        assert !this.mutex.isHeldByCurrentThread();
        Iterator<String, String> input = makeInputIterator(compact);

        Pair<Status, Long> pair = iterateInput(input, compact);
        Status status = pair.getKey();
        Long immutableMemtableMicros = pair.getValue();

        if (status.isOk() && this.shuttingDown.get()) {
            status = Status.IOError("Deleting DB during compaction");
        }
        if (status.isOk() && compact.getBuilder() != null) {
            status = finishCompactionOutputFile(compact, input);
        }
        if (status.isOk()) {
            status = input.status();
        }

        stats.setMicros(System.currentTimeMillis() - startMicros - immutableMemtableMicros);
        for (int which = 0; which < 2; which++) {
            for (int i = 0; i < compact.getCompaction().numInputFiles(which); i++) {
                stats.setBytesRead(stats.getBytesRead() + compact.getCompaction().input(which, i).getFileSize());
            }
        }

        for (int i = 0; i < compact.getOutputs().size(); i++) {
            stats.setBytesWritten(stats.getBytesWritten() + compact.getOutputs().get(i).getFileSize());
        }
        return status;
    }

    Iterator<String, String> makeInputIterator(CompactionState compact) {
        return this.versions.makeInputIterator(compact.getCompaction());
    }

    static class IterateInputState {
        String currentUserKey = null;
        boolean hasCurrentUserKey = false;
        long lastSequenceForKey = InternalKey.kMaxSequenceNumber;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            IterateInputState that = (IterateInputState) o;
            return hasCurrentUserKey == that.hasCurrentUserKey &&
                    lastSequenceForKey == that.lastSequenceForKey &&
                    Objects.equals(currentUserKey, that.currentUserKey);
        }

        @Override
        public int hashCode() {
            return Objects.hash(currentUserKey, hasCurrentUserKey, lastSequenceForKey);
        }
    }

    boolean stopDuringIterateCompactionInput(CompactionState compact, String key) {
        return compact.getCompaction().shouldStopBefore(key) && compact.getBuilder() != null;
    }

    Pair<Status, Long> iterateInput(Iterator<String, String> input, CompactionState compact) {
        long immutableMemtableMicros = 0;
        Status status = Status.OK();

        IterateInputState state = new IterateInputState();
        for (input.seekToFirst(); input.valid() && !this.shuttingDown.get(); input.next()) {
            // Prioritize immutable compaction work
            immutableMemtableMicros += compactMemtableFirst();

            String key = input.key();
            // avoid too much overlap
            if (stopDuringIterateCompactionInput(compact, key)) {
                status = finishCompactionOutputFile(compact, input);
                if (status.isNotOk()) {
                    break;
                }
            }

            // Handle key/value, add to state, etc.
            Pair<Boolean, ParsedInternalKey> pair = InternalKey.parseInternalKey(key);
            boolean drop = isEntryDroppable(compact, state, pair);

            if (!drop) {
                // Open output file if necessary
                if (compact.getBuilder() == null) {
                    status = openCompactionOutputFile(compact);
                    if (status.isNotOk()) {
                        break;
                    }
                }

                if (compact.getBuilder().numEntries() == 0) {
                    compact.currentOutput().getSmallest().decodeFrom(key);
                }
                compact.currentOutput().getLargest().decodeFrom(key);
                compact.getBuilder().add(key, input.value());

                // Close output file if it is big enough
                if (isBigEnough(compact)) {
                    status = finishCompactionOutputFile(compact, input);
                    if (status.isNotOk()) {
                        break;
                    }
                }
            }
        }

        return new Pair<>(status, immutableMemtableMicros);
    }

    boolean isBigEnough(CompactionState compact) {
        return compact.getBuilder().fileSize() >= compact.getCompaction().maxOutputFileSize();
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
