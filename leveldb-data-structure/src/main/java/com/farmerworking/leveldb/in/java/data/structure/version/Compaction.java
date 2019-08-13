package com.farmerworking.leveldb.in.java.data.structure.version;

import com.farmerworking.leveldb.in.java.api.Comparator;
import com.farmerworking.leveldb.in.java.api.Options;
import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKey;
import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKeyComparator;
import lombok.Data;

import java.util.Vector;

// A Compaction encapsulates information about a compaction.
@Data
public class Compaction {
    private final int level;

    private long maxOutputFileSize;
    Version inputVersion;

    // Index in grandparent_starts_
    int grandparentIndex;

    // Some output key has been seen
    boolean seenKey;

    // Bytes of overlap between current output and grandparent files
    long overlappedBytes;

    // level_ptrs_ holds indices into input_version_->levels_: our state
    // is that we are positioned at one of the file ranges for each
    // higher level than the ones involved in this compaction (i.e. for
    // all L >= level_ + 2).
    int[] levelPtrs = new int[Config.kNumLevels];

    private VersionEdit edit;

    // Each compaction reads inputs from "level_" and "level_+1"
    Vector<FileMetaData>[] inputs = new Vector[2];

    // State for implementing IsBaseLevelForKey

    // State used to check for number of of overlapping grandparent files
    // (parent == level_ + 1, grandparent == level_ + 2)
    // require: parameter is passed in increased order
    // require: grandparent files is immutable during usage
    Vector<FileMetaData> grandparents;

    public Compaction(Options options, int level) {
        this.level = level;
        this.maxOutputFileSize = VersionUtils.maxFileSizeForLevel(options, level);
        this.inputVersion = null;
        this.grandparentIndex = 0;
        this.seenKey = false;
        this.overlappedBytes = 0;
        this.grandparents = new Vector<>();
        this.edit = new VersionEdit();

        for (int i = 0; i < 2; i++) {
            inputs[i] = new Vector<>();
        }

        for (int i = 0; i < Config.kNumLevels; i++) {
            levelPtrs[i] = 0;
        }
    }

    // Return the level that is being compacted.  Inputs from "level"
    // and "level+1" will be merged to produce a set of "level+1" files.
    public int getLevel() {
        return level;
    }

    // Return the object that holds the edits to the descriptor done by this compaction.
    public VersionEdit getEdit() {
        return edit;
    }

    // "which" must be either 0 or 1
    public int numInputFiles(int which) {
        return inputs[which].size();
    }

    // Return the ith input file at "level()+which" ("which" must be 0 or 1).
    public FileMetaData input(int which, int i) {
        return inputs[which].get(i);
    }

    // Maximum size of files to build during this compaction.
    public long maxOutputFileSize() {
        return maxOutputFileSize;
    }

    // Is this a trivial compaction that can be implemented by just
    // moving a single input file to the next level (no merging or splitting)
    public boolean isTrivialMove() {
        // Avoid a move if there is lots of overlapping grandparent data.
        // Otherwise, the move could create a parent file that will require
        // a very expensive merge later on.
        return (this.numInputFiles(0) == 1 &&
                this.numInputFiles(1) == 0 &&
                VersionUtils.totalFileSize(this.grandparents) <= maxGrandParentOverlapBytes());
    }

    // Add all inputs to this compaction as delete operations to *edit.
    public void addInputDeletions(VersionEdit edit) {
        for (int which = 0; which < 2; which++) {
            for (int i = 0; i < inputs[which].size(); i++) {
                edit.deleteFile(this.level + which, inputs[which].get(i).getFileNumber());
            }
        }
    }

    // Returns true if the information we have available guarantees that
    // the compaction is producing data in "level+1" for which no data exists in levels greater than "level+1".
    public boolean isBaseLevelForKey(String userKey) {
        // Maybe use binary search to find right entry instead of linear search?
        char[] userKeyChar = userKey.toCharArray();
        Comparator userComparator = inputVersion.internalKeyComparator.getUserComparator();
        for (int lvl = this.level + 2; lvl < Config.kNumLevels; lvl++) {
            Vector<FileMetaData> files = inputVersion.files.get(lvl);
            for (; this.levelPtrs[lvl] < files.size();) {
                FileMetaData metaData = files.get(this.levelPtrs[lvl]);
                if (userComparator.compare(userKeyChar, metaData.getLargest().userKey().toCharArray()) <= 0) {
                    // We've advanced far enough
                    if (userComparator.compare(userKeyChar, metaData.getSmallest().userKey().toCharArray()) >= 0) {
                        // Key falls in this file's range, so definitely not base level
                        return false;
                    }
                    break;
                }
                this.levelPtrs[lvl]++;
            }
        }
        return true;
    }

    // Returns true iff we should stop building the current output before processing "internal_key".
    public boolean shouldStopBefore(String internalKey) {
        InternalKeyComparator comparator = inputVersion.internalKeyComparator;
        while(grandparentIndex < grandparents.size() &&
                comparator.compare(internalKey.toCharArray(), grandparents.get(grandparentIndex).getLargest().getRep()) > 0) {
            if (this.seenKey) {
                this.overlappedBytes += grandparents.get(grandparentIndex).getFileSize();
            }
            grandparentIndex ++;
        }

        this.seenKey = true;
        if (this.overlappedBytes > maxGrandParentOverlapBytes()) {
            // Too much overlap for current output; start new output
            this.overlappedBytes = 0;
            return true;
        } else {
            return false;
        }
    }

    public void releaseInputs() {
        if (this.inputVersion != null) {
            this.inputVersion.unref();
            this.inputVersion = null;
        }
    }

    // for ease unit test
    public long maxGrandParentOverlapBytes() {
        return VersionUtils.maxGrandParentOverlapBytes(inputVersion.options);
    }
}
