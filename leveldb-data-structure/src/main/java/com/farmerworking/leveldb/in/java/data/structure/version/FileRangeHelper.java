package com.farmerworking.leveldb.in.java.data.structure.version;

import com.farmerworking.leveldb.in.java.api.Comparator;
import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKey;
import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKeyComparator;
import com.farmerworking.leveldb.in.java.data.structure.memory.ValueType;

import java.util.Vector;

public class FileRangeHelper {
    /**
     * Return the smallest index i such that files[i]->largest >= key.
     * Return files.size() if there is no such file.
     * REQUIRES: "files" contains a sorted list of non-overlapping files.
     * @param comparator
     * @param files
     * @param key
     * @return
     */
    public int findFile(InternalKeyComparator comparator, Vector<FileMetaData> files, String key) {
        int left = 0;
        int right = files.size();
        InternalKey internalKey = InternalKey.decode(key);
        while(left < right) {
            int middle = (left + right) / 2;
            FileMetaData metaData = files.get(middle);

            if (comparator.compare(metaData.getLargest(), internalKey) < 0) {
                left = middle + 1;
            } else {
                right = middle;
            }
        }

        return right;
    }

    /**
     * Returns true iff some file in "files" overlaps the user key range [smallest,largest].
     * smallest==nullptr represents a key smaller than all keys in the DB.
     * largest==nullptr represents a key largest than all keys in the DB.
     * REQUIRES: If disjoint_sorted_files, files contains disjoint ranges in sorted order.
     * @return
     */
    public boolean isSomeFileOverlapsRange(InternalKeyComparator comparator,
                                           boolean disjointSortedFiles,
                                           Vector<FileMetaData> files,
                                           String smallestUserKey,
                                           String largestUserKey) {
        Comparator userComparator = comparator.getUserComparator();
        if (!disjointSortedFiles) {
            for(FileMetaData metaData : files) {
                if (afterFile(userComparator, smallestUserKey, metaData) ||
                        beforeFile(userComparator, largestUserKey, metaData)) {
                    // no overlap
                } else {
                    return true; // overlap
                }
            }

            return false;
        }

        int index = 0;
        if (smallestUserKey != null) {
            index = findFile(comparator, files, new InternalKey(
                    smallestUserKey,
                    InternalKey.kMaxSequenceNumber,
                    ValueType.kValueTypeForSeek).encode());
        }

        if (index >= files.size()) {
            return false;
        }

        return !beforeFile(userComparator, largestUserKey, files.get(index));
    }

    private boolean beforeFile(Comparator userComparator, String userKey, FileMetaData metaData) {
        return (userKey != null && userComparator.compare(
                userKey.toCharArray(),
                metaData.getSmallest().userKey.toCharArray()) < 0);
    }

    private boolean afterFile(Comparator userComparator, String userKey, FileMetaData metaData) {
        return (userKey != null && userComparator.compare(
                userKey.toCharArray(),
                metaData.getLargest().userKey.toCharArray()) > 0);
    }
}
