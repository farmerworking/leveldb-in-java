package com.farmerworking.leveldb.in.java.data.structure.version;

import com.farmerworking.leveldb.in.java.api.*;
import com.farmerworking.leveldb.in.java.api.Comparator;
import com.farmerworking.leveldb.in.java.api.Iterator;
import com.farmerworking.leveldb.in.java.data.structure.cache.TableCache;
import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKey;
import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKeyComparator;
import com.farmerworking.leveldb.in.java.data.structure.memory.ValueType;
import com.farmerworking.leveldb.in.java.data.structure.table.GetSaver;
import com.farmerworking.leveldb.in.java.data.structure.table.GetState;
import com.farmerworking.leveldb.in.java.data.structure.two.level.iterator.TwoLevelIterator;
import com.google.common.collect.Lists;
import javafx.util.Pair;

import java.util.*;
import java.util.function.Predicate;

public class Version {
    final InternalKeyComparator internalKeyComparator;
    final Options options;
    private final TableCache tableCache;

    // Number of live refs to this version
    int refs;

    // List of files per level
    List<Vector<FileMetaData>> files;

    // Next file to compact based on seek stats
    FileMetaData fileToCompact;
    int fileToCompactLevel;

    /**
     * Level that should be compacted next and its compaction score.
     * Score < 1 means compaction is not strictly needed.  These fields
     * are initialized by Finalize().
     */
    double compactionScore;
    int compactionLevel;

    FileRangeHelper fileRangeHelper;

    public static int DEFAULT_COMPACTION_SCORE = -1;
    public static int DEFAULT_COMPACTION_LEVEL = -1;

    public Version(VersionSet versionSetBelongTo) {
        this.internalKeyComparator = versionSetBelongTo.getInternalKeyComparator();
        this.options = versionSetBelongTo.getOptions();
        this.tableCache = versionSetBelongTo.getTableCache();

        this.refs = 0;
        this.fileToCompact = null;
        this.fileToCompactLevel = -1;
        this.compactionScore = DEFAULT_COMPACTION_SCORE;
        this.compactionLevel = DEFAULT_COMPACTION_LEVEL;
        this.fileRangeHelper = new FileRangeHelper();

        // init
        this.files = new ArrayList<>();
        for (int i = 0; i < Config.kNumLevels; i++) {
            files.add(new Vector<>());
        }
    }

    // return a sequence of iterators that will yield the contents of this Version when merged together.
    // requires: This version has been saved
    Vector<Iterator> iterators(ReadOptions readOptions) {
        Vector<Iterator> iterators = new Vector<>();

        // Merge all level zero files together since they may overlap
        Vector<FileMetaData> level0 = files.get(0);
        for (int i = 0; i < level0.size(); i++) {
            iterators.add(tableCache.iterator(
                    readOptions,
                    level0.get(i).getFileNumber(),
                    level0.get(i).getFileSize()).getKey());
        }

        // For levels > 0, we can use a concatenating iterator that sequentially
        // walks through the non-overlapping files in the level, opening them lazily.
        for (int level = 1; level < Config.kNumLevels; level++) {
            if (!files.get(level).isEmpty()) {
                iterators.add(new TwoLevelIterator<>(
                        new LevelFileNumIterator(internalKeyComparator, files.get(level)),
                        readOptions,
                        new TableCacheIndexTransfer(tableCache)
                ));
            }
        }

        return iterators;
    }

    public Pair<Status, String> get(ReadOptions options, InternalKey internalKey, GetStats stats) {
        stats.setSeekFileLevel(-1);
        stats.setSeekFile(null);

        for (int level = 0; level < Config.kNumLevels; level++) {
            Collection<FileMetaData> filesToSearch = getFilesToSearchForLevel(level, internalKey);

            FileMetaData lastFileRead = null;
            Integer lastFileReadLevel = null;
            for (FileMetaData fileMetaData : filesToSearch) {
                if (lastFileRead != null && stats.getSeekFile() == null) {
                    // We have had more than one seek for this read.  Charge the 1st file.
                    stats.setSeekFile(lastFileRead);
                    stats.setSeekFileLevel(lastFileReadLevel);
                }

                lastFileRead = fileMetaData;
                lastFileReadLevel = level;

                GetSaver saver = newGetSaver(internalKey.userKey());
                Status status = tableCache.get(
                        options,
                        fileMetaData.getFileNumber(),
                        fileMetaData.getFileSize(),
                        internalKey.encode(),
                        saver);

                if (status.isNotOk()) {
                    return new Pair<>(status, null);
                } else {
                    if (saver.getState().equals(GetState.kNotFound)) {
                        continue; // keep searching
                    } else if (saver.getState().equals(GetState.kFound)) {
                        return new Pair<>(status, saver.getValue());
                    } else if (saver.getState().equals(GetState.kDeleted)) {
                        return new Pair<>(Status.NotFound(""), null);
                    } else if (saver.getState().equals(GetState.kCorrupt)) {
                        return new Pair<>(Status.Corruption("corrupted key for " + internalKey.userKey()), null);
                    }
                }
            }
        }

        return new Pair<>(Status.NotFound(""), null);
    }

    // Adds "stats" into the current state.  Returns true if a new compaction may need to be triggered, false otherwise.
    // REQUIRES: lock is held
    public boolean updateStats(GetStats stats) {
        FileMetaData metaData = stats.getSeekFile();
        if (metaData != null) {
            metaData.setAllowedSeeks(metaData.getAllowedSeeks() - 1);

            if (metaData.getAllowedSeeks() <= 0 && this.fileToCompact == null) {
                this.fileToCompact = metaData;
                this.fileToCompactLevel = stats.getSeekFileLevel();
                return true;
            }
        }
        return false;
    }

    // Record a sample of bytes read at the specified internal key.
    // Samples are taken approximately once every config::kReadBytesPeriod
    // bytes.  Returns true if a new compaction may need to be triggered.
    // REQUIRES: lock is held
    public boolean recordReadSample(InternalKey internalKey) {
        MatchObj matchObj = new MatchObj();
        forEachOverlapping(internalKey, matchObj);
        // Must have at least two matches since we want to merge across
        // files. But what if we have a single file that contains many
        // overwrites and deletions?  Should we have another mechanism for
        // finding such files?
        if (matchObj.matches >= 2) {
            // 1MB cost is about 1 seek (see comment in Builder::Apply).
            return updateStats(matchObj.stats);
        }
        return false;
    }

    static class MatchObj implements Predicate<Pair<Integer, FileMetaData>> {
        int matches = 0;
        GetStats stats = new GetStats();

        @Override
        public boolean test(Pair<Integer, FileMetaData> pair) {
            matches ++;
            if (this.matches == 1) {
                // Remember first match
                stats.setSeekFile(pair.getValue());
                stats.setSeekFileLevel(pair.getKey());
            }

            if (matches >= 2) {
                return false;
            }
            return true;
        }
    }

    protected void forEachOverlapping(InternalKey internalKey, Predicate<Pair<Integer, FileMetaData>> predicate) {
        for (int level = 0; level < Config.kNumLevels; level++) {
            Collection<FileMetaData> filesToSearch = getFilesToSearchForLevel(level, internalKey);

            for(FileMetaData fileMetaData : filesToSearch) {
                if (!predicate.test(new Pair<>(level, fileMetaData))) {
                    return;
                }
            }
        }
    }

    public void ref() {
        this.refs ++;
    }

    public void unref() {
        assert this.refs >= 1;
        this.refs --;
    }

    public int numFiles(int level) {
        return this.files.get(level).size();
    }

    public boolean overlapInLevel(int level, String smallestUserKey, String largestUserKey) {
        return this.fileRangeHelper.isSomeFileOverlapsRange(
                this.internalKeyComparator,
                level > 0,
                this.files.get(level),
                smallestUserKey,
                largestUserKey);
    }

    public int pickLevelForMemTableOutput(String smallestUserKey, String largestUserKey) {
        int level = 0;
        if (!overlapInLevel(0, smallestUserKey, largestUserKey)) {
            // Push to next level if there is no overlap in this level,
            // and the #bytes overlapping in the level after that are limited.

            while(level < Config.kMaxMemCompactLevel) {
                if (overlapInLevel(level + 1, smallestUserKey, largestUserKey)) {
                    break;
                }

                if (level + 2 < Config.kNumLevels) {
                    // Check that file does not overlap too many grandparent bytes.
                    Vector<FileMetaData> overlaps = getOverlappingInputs(
                            level + 2,
                            new InternalKey(smallestUserKey, InternalKey.kMaxSequenceNumber, ValueType.kValueTypeForSeek),
                            new InternalKey(largestUserKey, 0, ValueType.kTypeDeletion));
                    long sum = totalFileSize(overlaps);
                    if (sum > VersionUtils.maxGrandParentOverlapBytes(this.options)) {
                        break;
                    }

                }

                level ++;
            }
        }
        return level;
    }

    public Vector<FileMetaData> getOverlappingInputs(int level, InternalKey begin, InternalKey end) {
        assert level >= 0;
        assert level < Config.kNumLevels;

        char[] userBegin = null, userEnd = null;
        if (begin != null) {
            userBegin = begin.userKey().toCharArray();
        }

        if (end != null) {
            userEnd = end.userKey().toCharArray();
        }

        Vector<FileMetaData> result = new Vector<>();
        Comparator comparator = this.internalKeyComparator.getUserComparator();
        for(int i = 0; i < this.files.get(level).size(); ) {
            FileMetaData metaData = this.files.get(level).get(i++);
            if (begin != null && comparator.compare(metaData.getLargest().userKey().toCharArray(), userBegin) < 0) {
                // completely before specified range; skip it
            } else if (end != null && comparator.compare(metaData.getSmallest().userKey().toCharArray(), userEnd) > 0) {
                // completely after specified range; skip it
            } else {
                result.add(metaData);

                if (level == 0) {
                    // Level-0 files may overlap each other.  So check if the newly
                    // added file has expanded the range.  If so, restart search.
                    if (begin != null && comparator.compare(metaData.getSmallest().userKey().toCharArray(), userBegin) < 0) {
                        userBegin = metaData.getSmallest().userKey().toCharArray();
                        result.clear();
                        i = 0;
                    } else if (end != null && comparator.compare(metaData.getLargest().userKey().toCharArray(), userEnd) > 0) {
                        userEnd = metaData.getLargest().userKey().toCharArray();
                        result.clear();
                        i = 0;
                    }
                }
            }
        }

        return result;
    }

    protected GetSaver newGetSaver(String userKey) {
        return new GetSaver(userKey, this.internalKeyComparator.getUserComparator());
    }

    protected List<FileMetaData> getFilesToSearchForLevel(int level, InternalKey internalKey) {
        int fileNums = files.get(level).size();

        if (fileNums == 0) {
            return new ArrayList<>();
        }

        Vector<FileMetaData> filesInLevel = files.get(level);
        if (level == 0) {
            ArrayList<FileMetaData> result = new ArrayList<>();
            for (int i = 0; i < fileNums; i++) {
                FileMetaData metaData = filesInLevel.get(i);

                if (this.internalKeyComparator.compare(internalKey, metaData.getSmallest()) >= 0 &&
                        this.internalKeyComparator.compare(internalKey, metaData.getLargest()) <= 0) {
                    result.add(metaData);

                }
            }

            if (result.isEmpty()) {
                return new ArrayList<>();
            } else {
                Collections.sort(result, new NewestFileComparator());
                return result;
            }
        } else {
            int index = fileRangeHelper.findFile(this.internalKeyComparator, files.get(level), internalKey.encode());

            if (index >= fileNums) {
                return new ArrayList<>();
            } else {
                FileMetaData fileMetaData = filesInLevel.get(index);
                if (internalKeyComparator.compare(internalKey, fileMetaData.getSmallest()) < 0) {
                    return new ArrayList<>();
                } else {
                    return Lists.newArrayList(fileMetaData);
                }
            }
        }
    }

    // for ease unit test
    protected long totalFileSize(Vector<FileMetaData> files) {
        return VersionUtils.totalFileSize(files);
    }
}
