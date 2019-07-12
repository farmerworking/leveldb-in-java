package com.farmerworking.leveldb.in.java.data.structure.version;

import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKey;
import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKeyComparator;
import javafx.util.Pair;

import java.util.*;

// A helper class so we can efficiently apply a whole sequence
// of edits to a particular state without creating intermediate
// Versions that contain full copies of the intermediate state.
public class VersionBuilder {
    class LevelState {
        Set<Long> deletedFiles = new HashSet<>();
        SortedSet<FileMetaData> addedFiles;
    }

    private InternalKeyComparator internalKeyComparator;
    private Version base;
    private LevelState[] levels = new LevelState[Config.kNumLevels];
    private String[] compactPointer = new String[Config.kNumLevels];

    public VersionBuilder(InternalKeyComparator internalKeyComparator, Version base) {
        this.internalKeyComparator = internalKeyComparator;
        this.base = base;

        SmallestKeyComparator comparator = new SmallestKeyComparator(this.internalKeyComparator);
        for (int level = 0; level < Config.kNumLevels; level++) {
            levels[level] = new LevelState();
            levels[level].addedFiles = new TreeSet<>(comparator);
        }
    }

    // Apply all of the edits in *edit to the current state.
    public void apply(VersionEdit edit) {
        // Update compaction pointers
        for (int i = 0; i < edit.getCompactPointers().size(); i++) {
            int level = edit.getCompactPointers().get(i).getKey();
            this.compactPointer[level] = edit.getCompactPointers().get(i).getValue().encode();
        }

        // Delete files
        Iterator<Pair<Integer, Long>> iter = edit.getDeletedFiles().iterator();
        while(iter.hasNext()) {
            Pair<Integer, Long> pair = iter.next();
            Integer level = pair.getKey();
            Long number = pair.getValue();

            levels[level].deletedFiles.add(number);
        }

        // Add new files
        for (int i = 0; i < edit.getNewFiles().size(); i++) {
            Integer level = edit.getNewFiles().get(i).getKey();
            FileMetaData metaData = new FileMetaData(edit.getNewFiles().get(i).getValue());
            metaData.setRefs(1);

            // We arrange to automatically compact this file after
            // a certain number of seeks.  Let's assume:
            //   (1) One seek costs 10ms
            //   (2) Writing or reading 1MB costs 10ms (100MB/s)
            //   (3) A compaction of 1MB does 25MB of IO:
            //         1MB read from this level
            //         10-12MB read from next level (boundaries may be misaligned)
            //         10-12MB written to next level
            // This implies that 25 seeks cost the same as the compaction
            // of 1MB of data.  I.e., one seek costs approximately the
            // same as the compaction of 40KB of data.  We are a little
            // conservative and allow approximately one seek for every 16KB
            // of data before triggering a compaction.
            metaData.setAllowedSeeks((int) (metaData.getFileSize() / 16384));
            if (metaData.getAllowedSeeks() < 100) {
                metaData.setAllowedSeeks(100);
            }

            levels[level].deletedFiles.remove(metaData.getFileNumber());
            levels[level].addedFiles.add(metaData);
        }
    }

    // Save the current state in *v.
    public void saveTo(VersionSet versionSet, Version version) {
        SmallestKeyComparator comparator = new SmallestKeyComparator(this.internalKeyComparator);

        // save compact point state in versionSet
        for (int level = 0; level < Config.kNumLevels; level++) {
            versionSet.compactPointer[level] = this.compactPointer[level];
        }

        for (int level = 0; level < Config.kNumLevels; level++) {
            // Merge the set of added files with the set of pre-existing files.
            // Drop any deleted files.  Store the result in *v.
            Vector<FileMetaData> baseFiles = base.files.get(level);
            SortedSet<FileMetaData> added = levels[level].addedFiles;

            List<FileMetaData> list = new ArrayList<>();
            list.addAll(baseFiles);
            list.addAll(added);
            Collections.sort(list, comparator);

            for (FileMetaData metaData : list) {
                maybeAddFile(version, level, metaData);
            }
        }
    }

    private void maybeAddFile(Version version, int level, FileMetaData metaData) {
        if (levels[level].deletedFiles.stream().anyMatch(item -> item.equals(metaData.getFileNumber()))) {
            // File is deleted: do nothing
        } else {
            Vector<FileMetaData> files = version.files.get(level);

            if (level > 0 && !files.isEmpty()) {
                // Must not overlap
                assert this.internalKeyComparator.compare(files.get(files.size()-1).getLargest(), metaData.getSmallest()) < 0;
            }

            metaData.setRefs(metaData.getRefs() + 1);
            files.add(metaData);
        }
    }
}
