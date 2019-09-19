package com.farmerworking.leveldb.in.java.data.structure.version;

import com.farmerworking.leveldb.in.java.api.*;
import com.farmerworking.leveldb.in.java.api.Iterator;
import com.farmerworking.leveldb.in.java.data.structure.cache.TableCache;
import com.farmerworking.leveldb.in.java.data.structure.log.ILogReporter;
import com.farmerworking.leveldb.in.java.data.structure.log.ILogWriter;
import com.farmerworking.leveldb.in.java.data.structure.log.LogReader;
import com.farmerworking.leveldb.in.java.data.structure.log.LogWriter;
import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKey;
import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKeyComparator;
import com.farmerworking.leveldb.in.java.data.structure.table.ITableReader;
import com.farmerworking.leveldb.in.java.data.structure.two.level.iterator.TwoLevelIterator;
import com.farmerworking.leveldb.in.java.file.*;
import javafx.util.Pair;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

@Data
public class VersionSet {
    private Env env;
    private String dbname;
    private long nextFileNumber;
    private long manifestFileNumber;
    private long lastSequence;
    private long logNumber;

    // 0 or backing store for memtable being compacted
    private long prevLogNumber;

    // Opened lazily
    private WritableFile descriptorFile;
    private ILogWriter descriptorLog;

    // Head of circular doubly-linked list of versions.
    private List<Version> dummyVersions;
    // == dummy_versions_.prev_
    private Version current;

    private InternalKeyComparator internalKeyComparator;
    private Options options;
    private TableCache tableCache;

    // Per-level key at which the next compaction at that level should start.
    // Either an empty string, or a valid InternalKey.
    String[] compactPointer = new String[Config.kNumLevels];

    public VersionSet() {}

    public VersionSet(String dbname, Options options, TableCache tableCache, InternalKeyComparator comparator) {
        this.env = options.getEnv();
        this.dbname = dbname;
        this.options = options;
        this.tableCache = tableCache;
        this.internalKeyComparator = comparator;
        this.nextFileNumber = 2;
        this.manifestFileNumber = 0;
        this.lastSequence = 0;
        this.logNumber = 0;
        this.prevLogNumber = 0;
        this.descriptorFile = null;
        this.descriptorLog = null;
        this.dummyVersions = new ArrayList<>();
        this.current = null;

        appendVersion(new Version(this));
    }

    // Apply *edit to the current version to form a new descriptor that
    // is both saved to persistent state and installed as the new
    // current version.  Will release *mu while actually writing to the file.
    // REQUIRES: *mu is held on entry.
    // REQUIRES: no other thread concurrently calls LogAndApply()
    public Status logAndApply(VersionEdit edit, ReentrantLock lock) {
        assert lock.isHeldByCurrentThread();

        if (edit.isHasLogNumber()) {
            assert edit.getLogNumber() >= this.logNumber;
            assert edit.getLogNumber() < this.nextFileNumber;
        } else {
            edit.setLogNumber(this.logNumber);
        }

        if (!edit.isHasPrevLogNumber()) {
            edit.setPrevLogNumber(this.prevLogNumber);
        }

        edit.setNextFileNumber(this.nextFileNumber);
        edit.setLastSequence(this.lastSequence);

        Version version = new Version(this);

        VersionBuilder builder = new VersionBuilder(this.internalKeyComparator, current);
        builder.apply(edit);
        builder.saveTo(this, version);

        finalizeVersion(version);

        // Initialize new descriptor log file if necessary by creating
        // a temporary file that contains a snapshot of the current version.
        String newManifestFile = null;
        Status status = Status.OK();
        if (this.descriptorLog == null) {
            // No reason to unlock *mu here since we only hit this path in the
            // first call to LogAndApply (when opening the database).
            assert descriptorFile == null;
            newManifestFile = FileName.descriptorFileName(dbname, manifestFileNumber);
            edit.setNextFileNumber(nextFileNumber);

            Pair<Status, WritableFile> pair = env.newWritableFile(newManifestFile);
            status = pair.getKey();
            if (status.isOk()) {
                descriptorFile = pair.getValue();
                descriptorLog = new LogWriter(descriptorFile);
                status = writeSnapshot(descriptorLog);
            }
        }

        // Unlock during expensive MANIFEST log write
        {
            lock.unlock();

            if (status.isOk()) {
                StringBuilder stringBuilder = new StringBuilder();
                edit.encodeTo(stringBuilder);
                status = addRecord(stringBuilder.toString());

                if (status.isOk()) {
                    status = sync();
                }

                if (status.isNotOk()) {
                    Options.Logger.log(this.options.getInfoLog(), String.format("MANIFEST write: %s", status.toString()));
                }
            }

            // If we just created a new descriptor file, install it by writing a
            // new CURRENT file that points to it.
            if (status.isOk() && StringUtils.isNotEmpty(newManifestFile)) {
                status = setCurrentFile();
            }

            lock.lock();
        }

        if (status.isOk()) {
            appendVersion(version);
            this.logNumber = edit.getLogNumber();
            this.prevLogNumber = edit.getPrevLogNumber();
        } else {
            if (StringUtils.isNotEmpty(newManifestFile)) {
                descriptorLog = null;
                descriptorFile = null;
                env.delete(newManifestFile);
            }
        }

        return status;
    }

    Status setCurrentFile() {
        return FileName.setCurrentFile(this.env, this.dbname, this.manifestFileNumber);
    }

    Status sync() {
        return descriptorFile.sync();
    }

    Status addRecord(String content) {
        return descriptorLog.addRecord(content);
    }

    class LogReporter implements ILogReporter {
        Status status = Status.OK();

        @Override
        public void corruption(long bytes, Status status) {
            if (this.status.isOk()) {
                this.status.setCode(status.getCode());
                this.status.setMessage(status.getMessage());
            }
        }
    }

    public Pair<Status, Boolean> recover() {
        Boolean saveManifest = false;
        Pair<Status, String> pair = readFileToString();
        Status status = pair.getKey();
        if (status.isNotOk()) {
            return new Pair<>(status, saveManifest);
        }

        String current = pair.getValue();
        if (StringUtils.isEmpty(current) || current.charAt(current.length() - 1) != '\n') {
            return new Pair<>(Status.Corruption("CURRENT file does not end with newline"), saveManifest);
        }
        current = current.substring(0, current.length() - 1);

        String dscName = dbname + "/" + current;
        Pair<Status, SequentialFile> tmp = env.newSequentialFile(dscName);
        status = tmp.getKey();
        if (status.isNotOk()) {
            return new Pair<>(status, saveManifest);
        }

        boolean haveLogNumber = false;
        boolean havePrevLogNumber = false;
        boolean haveNextFile = false;
        boolean haveLastSequence = false;
        long nextFile = 0;
        long lastSequence = 0;
        long logNumber = 0;
        long prevLogNumber = 0;
        VersionBuilder builder = new VersionBuilder(this.internalKeyComparator, this.current);

        {
            LogReporter reporter = new LogReporter();
            reporter.status = status;

            LogReader logReader = getLogReader(tmp.getValue(), reporter);
            for (Pair<Boolean, String> logPair = logReader.readRecord(); logPair.getKey() && status.isOk(); logPair = logReader.readRecord()) {
                VersionEdit edit = getVersionEdit();
                status = edit.decodeFrom(logPair.getValue().toCharArray());

                if (status.isOk()) {
                    if (edit.isHasComparator() && !edit.getComparatorName().equals(internalKeyComparator.getUserComparator().name())) {
                        status = Status.InvalidArgument(edit.getComparatorName() + " does not match existing comparator",
                                internalKeyComparator.getUserComparator().name());
                    }
                }

                if (status.isOk()) {
                    builder.apply(edit);
                }

                if (edit.isHasLogNumber()) {
                    logNumber = edit.getLogNumber();
                    haveLogNumber = true;
                }

                if (edit.isHasPrevLogNumber()) {
                    prevLogNumber = edit.getPrevLogNumber();
                    havePrevLogNumber = true;
                }

                if (edit.isHasNextFileNumber()) {
                    nextFile = edit.getNextFileNumber();
                    haveNextFile = true;
                }

                if (edit.isHasLastSequence()) {
                    lastSequence = edit.getLastSequence();
                    haveLastSequence = true;
                }
            }
        }

        if (status.isOk()) {
            if (!haveNextFile) {
                status = Status.Corruption("no meta-nextfile entry in descriptor");
            } else if (!haveLogNumber) {
                status = Status.Corruption("no meta-lognumber entry in descriptor");
            } else if (!haveLastSequence) {
                status = Status.Corruption("no last-sequence-number entry in descriptor");
            }

            if (!havePrevLogNumber) {
                this.prevLogNumber = 0;
            }

            markFileNumberUsed(prevLogNumber);
            markFileNumberUsed(logNumber);
        }

        if (status.isOk()) {
            Version version = new Version(this);
            builder.saveTo(this, version);
            finalizeVersion(version);
            appendVersion(version);

            this.manifestFileNumber = nextFile;
            this.nextFileNumber = nextFile + 1;
            this.lastSequence = lastSequence;
            this.logNumber = logNumber;
            this.prevLogNumber = prevLogNumber;

            if (reuseManifest(dscName, current)) {
                // no need to save new manifest
            } else {
                saveManifest = true;
            }
        }

        return new Pair<>(status, saveManifest);
    }

    VersionEdit getVersionEdit() {
        return new VersionEdit();
    }

    LogReader getLogReader(SequentialFile sequentialFile, LogReporter reporter) {
        return new LogReader(sequentialFile, reporter, true, 0);
    }

    Pair<Status, String> readFileToString() {
        return Env.readFileToString(this.env, FileName.currentFileName(this.dbname));
    }

    boolean reuseManifest(String dscName, String dscBase) {
        if (!options.isReuseLogs()) {
            return false;
        }

        Pair<Long, FileType> pair = FileName.parseFileName(dscBase);
        Long manifestSize;
        if (pair == null || pair.getValue() != FileType.kDescriptorFile) {
            return false;
        } else {
            Pair<Status, Long> tmp = env.getFileSize(dscName);

            if (tmp.getKey().isNotOk()) {
                return false;
            } else {
                manifestSize = tmp.getValue();
                if (manifestSize >= VersionUtils.targetFileSize(this.options)) {
                    // Make new compacted MANIFEST if old one is too big
                    return false;
                }
            }
        }

        assert this.descriptorFile == null;
        assert this.descriptorLog == null;
        Pair<Status, WritableFile> tmp = env.newAppendableFile(dscName);
        if (tmp.getKey().isNotOk()) {
            Options.Logger.log(options.getInfoLog(), String.format("Reuse MANIFEST: %s", tmp.getKey().toString()));
            return false;
        }

        this.descriptorFile = tmp.getValue();
        Options.Logger.log(options.getInfoLog(), String.format("Reusing MANIFEST %s", dscName));
        this.descriptorLog = new LogWriter(descriptorFile, manifestSize);
        this.manifestFileNumber = pair.getKey();
        return true;
    }

    public void markFileNumberUsed(long fileNumber) {
        if (nextFileNumber <= fileNumber) {
            this.nextFileNumber = fileNumber + 1;
        }
    }

    public long newFileNumber() {
        return nextFileNumber ++;
    }

    // Arrange to reuse "file_number" unless a newer file number has
    // already been allocated.
    public void reuseFileNumber(long fileNumber) {
        if (nextFileNumber == fileNumber + 1) {
            nextFileNumber = fileNumber;
        }
    }

    public int numLevelFiles(int level) {
        assert level >= 0;
        assert level < Config.kNumLevels;
        return current.files.get(level).size();
    }

    // Return the combined file size of all files at the specified level.
    public long numLevelBytes(int level) {
        assert level >= 0;
        assert level < Config.kNumLevels;
        return VersionUtils.totalFileSize(current.files.get(level));
    }

    public void setLastSequence(long lastSequence) {
        assert lastSequence >= this.lastSequence;
        this.lastSequence = lastSequence;
    }

    // Pick level and inputs for a new compaction.
    // Returns NULL if there is no compaction to be done.
    // Otherwise returns a pointer to a heap-allocated object that
    // describes the compaction.  Caller should delete the result.
    public Compaction pickCompaction() {
        // We prefer compactions triggered by too much data in a level over
        // the compactions triggered by seeks.
        boolean sizeCompaction = current.compactionScore >= 1;
        boolean seekCompaction = current.fileToCompact != null;

        int level;
        Compaction compaction;
        if (sizeCompaction) {
            level = current.compactionLevel;
            assert level >= 0;
            assert level + 1 < Config.kNumLevels;
            compaction = new Compaction(this.options, level);

            // Pick the first file that comes after compact_pointer_[level]
            for (int i = 0; i < current.files.get(level).size(); i++) {
                FileMetaData metaData = current.files.get(level).get(i);
                if (StringUtils.isEmpty(this.compactPointer[level]) ||
                        this.internalKeyComparator.compare(metaData.getLargest().getRep(), compactPointer[level].toCharArray()) > 0) {
                    compaction.inputs[0].add(metaData);
                    break;
                }
            }

            if (compaction.inputs[0].isEmpty()) {
                // Wrap-around to the beginning of the key space
                compaction.inputs[0].add(current.files.get(level).get(0));
            }
        } else if (seekCompaction) {
            level = current.fileToCompactLevel;
            compaction = new Compaction(this.options, level);
            compaction.inputs[0].add(current.fileToCompact);
        } else {
            return null;
        }

        compaction.inputVersion = current;
        compaction.inputVersion.ref();

        // Files in level 0 may overlap each other, so pick up all overlapping ones
        if (level == 0) {
            Pair<InternalKey, InternalKey> pair = getRange(compaction.inputs[0]);
            // Note that the next call will discard the file we placed in
            // c->inputs_[0] earlier and replace it with an overlapping set
            // which will include the picked file.
            compaction.inputs[0] = current.getOverlappingInputs(0, pair.getKey(), pair.getValue());
            assert !compaction.inputs[0].isEmpty();
        }

        setupOtherInputs(compaction);
        return compaction;
    }

    // Return a compaction object for compacting the range [begin,end] in
    // the specified level.  Returns NULL if there is nothing in that
    // level that overlaps the specified range.  Caller should delete
    // the result.
    public Compaction compactRange(int level, InternalKey begin, InternalKey end) {
        Vector<FileMetaData> inputs = current.getOverlappingInputs(level, begin, end);
        if (inputs == null || inputs.isEmpty()) {
            return null;
        }

        // Avoid compacting too much in one shot in case the range is large.
        // But we cannot do this for level-0 since level-0 files can overlap
        // and we must not pick one file and drop another older file if the
        // two files overlap.
        if (level > 0) {
            long limit = maxFileSizeForLevel(level);
            long total = 0;
            for (int i = 0; i < inputs.size(); i++) {
                total += inputs.get(i).getFileSize();
                if (total >= limit) {
                    inputs = new Vector<>(inputs.subList(0, i + 1));
                    break;
                }
            }
        }

        Compaction compaction = new Compaction(this.options, level);
        compaction.inputVersion = current;
        compaction.inputVersion.ref();
        compaction.inputs[0] = inputs;
        setupOtherInputs(compaction);
        return compaction;
    }

    // Return the maximum overlapping data (in bytes) at next level for any
    // file at a level >= 1.
    public long maxNextLevelOverlappingBytes() {
        long result = 0L;
        for (int level = 1; level < Config.kNumLevels; level++) {
            for (int i = 0; i < current.files.get(level).size(); i++) {
                FileMetaData metaData = current.files.get(level).get(i);
                Vector<FileMetaData> overlaps = current.getOverlappingInputs(
                        level + 1,
                        metaData.getSmallest(),
                        metaData.getLargest());
                long sum = VersionUtils.totalFileSize(overlaps);
                if (sum > result) {
                    result = sum;
                }
            }
        }

        return result;
    }

    // Create an iterator that reads over the compaction inputs for "*c".
    // The caller should delete the iterator when no longer needed.
    public Iterator<String, String> makeInputIterator(Compaction compaction) {
        ReadOptions readOptions = new ReadOptions();
        readOptions.setVerifyChecksums(options.isParanoidChecks());

        // Level-0 files have to be merged together.  For other levels,
        // we will make a concatenating iterator per level.
        // TODO(opt): use concatenating iterator for level-0 if there is no overlap
        int space = (compaction.getLevel() == 0 ? compaction.inputs[0].size() + 1 : 2);
        List<Iterator<String, String>> list = new ArrayList<>(space);
        int num = 0;
        for (int which = 0; which < 2; which++) {
            if (!compaction.inputs[which].isEmpty()) {
                if (compaction.getLevel() + which == 0) {
                    Vector<FileMetaData> files = compaction.inputs[which];
                    for (int i = 0; i < files.size(); i++) {
                        list.add(num ++, tableCache.iterator(
                                readOptions,
                                files.get(i).getFileNumber(),
                                files.get(i).getFileSize()).getKey());
                    }
                } else {
                    // Create concatenating iterator for the files from this level
                    list.add(num ++, new TwoLevelIterator<>(
                            new LevelFileNumIterator(internalKeyComparator, compaction.inputs[which]),
                            readOptions,
                            new TableCacheIndexTransfer(tableCache)));
                }
            }
        }
        assert num <= space;
        return MergingIterator.newMergingIterator(internalKeyComparator, list);
    }

    // Returns true iff some level needs a compaction.
    public boolean needCompaction() {
        Version version = current;
        return version.compactionScore >= 1 || version.fileToCompact != null;
    }

    // Return a human-readable short (single-line) summary of the number
    // of files per level.  Uses *scratch as backing store.
    public String levelSummary() {
        // Update code if kNumLevels changes
        assert(Config.kNumLevels == 7);

        return String.format(
                "files[ %d %d %d %d %d %d %d ]",
                current.files.get(0).size(),
                current.files.get(1).size(),
                current.files.get(2).size(),
                current.files.get(3).size(),
                current.files.get(4).size(),
                current.files.get(5).size(),
                current.files.get(6).size()
                );
    }

    // Return the approximate offset in the database of the data for
    // "key" as of version "v".
    public long approximateOffsetOf(Version version, InternalKey key) {
        long result = 0L;
        for (int level = 0; level < Config.kNumLevels; level++) {
            Vector<FileMetaData> files = version.files.get(level);
            for (int i = 0; i < files.size(); i++) {
                if (internalKeyComparator.compare(files.get(i).getLargest(), key) <= 0) {
                    // Entire file is before "ikey", so just add the file size
                    result += files.get(i).getFileSize();
                } else if (internalKeyComparator.compare(files.get(i).getSmallest(), key) > 0) {
                    // Entire file is after "ikey", so ignore
                    if (level > 0) {
                        // Files other than level 0 are sorted by meta->smallest, so
                        // no further files in this level will contain data for
                        // "ikey".
                        break;
                    }
                } else {
                    // "ikey" falls in the range for this table.  Add the
                    // approximate offset of "ikey" within the table.
                    Pair<Iterator<String, String>, ITableReader> pair = tableCache.iterator(
                            new ReadOptions(),
                            files.get(i).getFileNumber(),
                            files.get(i).getFileSize());
                    if (pair.getValue() != null) {
                        result += pair.getValue().approximateOffsetOf(key.encode());
                    }
                }
            }
        }

        return result;
    }

    // Add all files listed in any live version to *live.
    // May also mutate some internal state.
    public Set<Long> getLiveFiles() {
        Set<Long> result = new HashSet<>();
        for(Version version : dummyVersions) {
            for (int level = 0; level < Config.kNumLevels; level++) {
                Vector<FileMetaData> files = version.files.get(level);
                for (int i = 0; i < files.size(); i++) {
                    result.add(files.get(i).getFileNumber());
                }
            }
        }
        return result;
    }

    Status writeSnapshot(ILogWriter logWriter) {
        // TODO: Break up into multiple records to reduce memory usage on recovery?

        // Save metadata
        VersionEdit edit = new VersionEdit();
        edit.setComparatorName(internalKeyComparator.getUserComparator().name());

        // Save compaction pointers
        for (int level = 0; level < Config.kNumLevels; level++) {
            if (StringUtils.isNotEmpty(compactPointer[level])) {
                InternalKey internalKey = new InternalKey();
                internalKey.decodeFrom(compactPointer[level]);
                edit.addCompactPoint(level, internalKey);
            }
        }

        // Save file
        for (int level = 0; level < Config.kNumLevels; level++) {
            Vector<FileMetaData> files = current.files.get(level);
            for (int i = 0; i < files.size(); i++) {
                FileMetaData metaData = files.get(i);
                edit.addFile(level, metaData.getFileNumber(), metaData.getFileSize(),
                        metaData.getSmallest(), metaData.getLargest());
            }
        }

        StringBuilder builder = new StringBuilder();
        edit.encodeTo(builder);
        return logWriter.addRecord(builder.toString());
    }

    void finalizeVersion(Version version) {
        // Precomputed best level for next compaction
        int bestLevel = -1;
        double bestScore = -1;

        for (int level = 0; level < Config.kNumLevels; level++) {
            double score;

            if (level == 0) {
                // We treat level-0 specially by bounding the number of files
                // instead of number of bytes for two reasons:
                //
                // (1) With larger write-buffer sizes, it is nice not to do too
                // many level-0 compactions.
                //
                // (2) The files in level-0 are merged on every read and
                // therefore we wish to avoid too many files when the individual
                // file size is small (perhaps because of a small write-buffer
                // setting, or very high compression ratios, or lots of
                // overwrites/deletions).
                score = version.files.get(level).size() / (double)Config.kL0_CompactionTrigger;
            } else {
                long levelBytes = VersionUtils.totalFileSize(version.files.get(level));
                score = (double)levelBytes / maxBytesForLevel(this.options, level);
            }

            if (score > bestScore) {
                bestLevel = level;
                bestScore = score;
            }
        }

        version.compactionLevel = bestLevel;
        version.compactionScore = bestScore;
    }

    long maxBytesForLevel(Options options, int level) {
        // Note: the result for level zero is not really used since we set
        // the level-0 compaction threshold based on number of files.

        // Result for both level-0 and level-1
        long result = 10 * 1024 * 1024;
        while (level > 1) {
            result *= 10;
            level--;
        }
        return result;
    }

    protected void appendVersion(Version version) {
        assert version != this.current;

        this.current = version;
        dummyVersions.add(version);
    }

    Pair<InternalKey, InternalKey> getRange(Collection<FileMetaData> inputs) {
        assert inputs != null && !inputs.isEmpty();

        InternalKey smallest = null, largest = null;
        for (FileMetaData metaData : inputs) {
            if (smallest == null && largest == null) {
                smallest = metaData.getSmallest();
                largest = metaData.getLargest();
            } else {
                if (this.internalKeyComparator.compare(metaData.getSmallest(), smallest) < 0) {
                    smallest = metaData.getSmallest();
                }
                if (this.internalKeyComparator.compare(metaData.getLargest(), largest) > 0) {
                    largest = metaData.getLargest();
                }
            }
        }
        return new Pair<>(smallest, largest);
    }

    Pair<InternalKey, InternalKey> getRange2(Collection<FileMetaData> input, Collection<FileMetaData> input1) {
        Vector<FileMetaData> allResult = new Vector<>();
        allResult.addAll(input);
        allResult.addAll(input1);
        return getRange(allResult);
    }

    protected void setupOtherInputs(Compaction compaction) {
        int level = compaction.getLevel();
        Pair<InternalKey, InternalKey> pair = getRange(compaction.inputs[0]);
        InternalKey smallest = pair.getKey();
        InternalKey largest = pair.getValue();
        compaction.inputs[1] = current.getOverlappingInputs(level + 1, smallest, largest);

        // Get entire range covered by compaction
        Pair<InternalKey, InternalKey> pair2 = getRange2(compaction.inputs[0], compaction.inputs[1]);
        InternalKey allStart = pair2.getKey();
        InternalKey allEnd = pair2.getValue();

        // See if we can grow the number of inputs in "level" without
        // changing the number of "level+1" files we pick up.
        if (!compaction.inputs[1].isEmpty()) {
            Vector<FileMetaData> expand = current.getOverlappingInputs(level, allStart, allEnd);

            long inputs0Size = VersionUtils.totalFileSize(compaction.inputs[0]);
            long inputs1Size = VersionUtils.totalFileSize(compaction.inputs[1]);
            long expanded0Size = VersionUtils.totalFileSize(expand);

            if (expand.size() > compaction.inputs[0].size() &&
                    inputs1Size + expanded0Size < expandedCompactionByteSizeLimit(this.options)) {
                Pair<InternalKey, InternalKey> pair3 = getRange(expand);

                Vector<FileMetaData> expand1 = current.getOverlappingInputs(
                        level + 1, pair3.getKey(), pair3.getValue());

                if (expand1.size() == compaction.inputs[1].size()) {
                    Options.Logger.log(this.options.getInfoLog(),
                            String.format("Expanding@%d %d+%d (%d+%d bytes) to %d+%d (%d+%d bytes)",
                            level, compaction.inputs[0].size(), compaction.inputs[1].size(),
                            inputs0Size, inputs1Size, expand.size(), expand1.size(), expanded0Size, inputs1Size));
                    smallest = pair3.getKey();
                    largest = pair3.getValue();
                    compaction.inputs[0] = expand;
                    compaction.inputs[1] = expand1;
                    Pair<InternalKey, InternalKey> pair4 = getRange2(compaction.inputs[0], compaction.inputs[1]);
                    allStart = pair4.getKey();
                    allEnd = pair4.getValue();
                }
            }
        }

        // Compute the set of grandparent files that overlap this compaction
        // (parent == level+1; grandparent == level+2)
        if (level + 2 < Config.kNumLevels) {
            compaction.grandparents = current.getOverlappingInputs(level + 2, allStart, allEnd);
        }

        if (false) {
            Options.Logger.log(this.options.getInfoLog(), String.format("Compacting %d '%s' .. '%s'",
                    level, smallest.toString(), largest.toString()));
        }

        // Update the place where we will do the next compaction for this level.
        // We update this immediately instead of waiting for the VersionEdit
        // to be applied so that if the compaction fails, we will try a different
        // key range next time.
        this.compactPointer[level] = largest.encode();
        compaction.getEdit().addCompactPoint(level, largest);
    }

    // Maximum number of bytes in all compacted files.  We avoid expanding
    // the lower level file set of a compaction if it would make the
    // total compaction cover more than this many bytes.
    long expandedCompactionByteSizeLimit(Options options) {
        return 25 * VersionUtils.targetFileSize(options);
    }

    long maxFileSizeForLevel(int level) {
        return VersionUtils.maxFileSizeForLevel(this.options, level);
    }
}
