package com.farmerworking.leveldb.in.java.data.structure.version;

import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.common.ICoding;
import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKey;
import javafx.util.Pair;
import lombok.Data;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;

@Data
public class VersionEdit {
    // Tag numbers for serialized VersionEdit.  These numbers are written to disk and should not be changed.
    int kComparator           = 1;
    int kLogNumber            = 2;
    int kNextFileNumber       = 3;
    int kLastSequence         = 4;
    int kCompactPointer       = 5;
    int kDeletedFile          = 6;
    int kNewFile              = 7;
    // 8 was used for large value refs
    int kPrevLogNumber        = 9;


    private String comparatorName;
    private long logNumber;
    private long prevLogNumber;
    private long nextFileNumber;
    private long lastSequence;
    private boolean hasComparator;
    private boolean hasLogNumber;
    private boolean hasPrevLogNumber;
    private boolean hasNextFileNumber;
    private boolean hasLastSequence;

    private Vector<Pair<Integer, InternalKey>> compactPointers = new Vector<>();
    Set<Pair<Integer, Long>> deletedFiles = new HashSet<>();
    private Vector<Pair<Integer, FileMetaData> > newFiles = new Vector<>();

    private ICoding coding = ICoding.getInstance();

    public VersionEdit() {
        clear();
    }

    public void setComparatorName(String comparatorName) {
        this.hasComparator = true;
        this.comparatorName = comparatorName;
    }

    public void setLogNumber(long logNumber) {
        this.hasLogNumber = true;
        this.logNumber = logNumber;
    }

    public void setPrevLogNumber(long prevLogNumber) {
        this.hasPrevLogNumber = true;
        this.prevLogNumber = prevLogNumber;
    }

    public void setNextFileNumber(long nextFileNumber) {
        this.hasNextFileNumber = true;
        this.nextFileNumber = nextFileNumber;
    }

    public void setLastSequence(long lastSequence) {
        this.hasLastSequence = true;
        this.lastSequence = lastSequence;
    }

    public void addCompactPoint(int level, InternalKey key) {
        this.compactPointers.add(new Pair<>(level, key));
    }

    public void addFile(int level, long fileNumber, long fileSize, InternalKey smallest, InternalKey largest) {
        FileMetaData fileMetaData = new FileMetaData(fileNumber, fileSize, smallest, largest);
        newFiles.add(new Pair<>(level, fileMetaData));
    }

    public void deleteFile(int level, long fileNumber) {
        deletedFiles.add(new Pair<>(level, fileNumber));
    }

    public void encodeTo(StringBuilder builder) {
        if (hasComparator) {
            coding.putVarint32(builder, kComparator);
            coding.putLengthPrefixedString(builder, this.comparatorName);
        }
        if (hasLogNumber) {
            coding.putVarint32(builder, kLogNumber);
            coding.putVarint64(builder, this.logNumber);
        }
        if (hasPrevLogNumber) {
            coding.putVarint32(builder, kPrevLogNumber);
            coding.putVarint64(builder, this.prevLogNumber);
        }
        if (hasNextFileNumber) {
            coding.putVarint32(builder, kNextFileNumber);
            coding.putVarint64(builder, this.nextFileNumber);
        }
        if (hasLastSequence) {
            coding.putVarint32(builder, kLastSequence);
            coding.putVarint64(builder, this.lastSequence);
        }

        for (int i = 0; i < this.compactPointers.size(); i++) {
            coding.putVarint32(builder, kCompactPointer);
            coding.putVarint32(builder, this.compactPointers.get(i).getKey());  // level
            coding.putLengthPrefixedString(builder, this.compactPointers.get(i).getValue().encode());
        }

        for (Iterator<Pair<Integer, Long>> iter = this.deletedFiles.iterator(); iter.hasNext(); ) {
            Pair<Integer, Long> next = iter.next();
            coding.putVarint32(builder, kDeletedFile);
            coding.putVarint32(builder, next.getKey());   // level
            coding.putVarint64(builder, next.getValue());  // file number
        }

        for (int i = 0; i < this.newFiles.size(); i++) {
            FileMetaData fileMetaData = this.newFiles.get(i).getValue();
            coding.putVarint32(builder, kNewFile);
            coding.putVarint32(builder, this.newFiles.get(i).getKey());  // level
            coding.putVarint64(builder, fileMetaData.getFileNumber());
            coding.putVarint64(builder, fileMetaData.getFileSize());
            coding.putLengthPrefixedString(builder, fileMetaData.getSmallest().encode());
            coding.putLengthPrefixedString(builder, fileMetaData.getLargest().encode());
        }
    }

    public Status decodeFrom(char[] buffer) {
        clear();
        String msg = null;
        int tag;
        int offset = 0;

        // Temporary storage for parsing
        int level;
        long number;
        FileMetaData f;
        String str;
        InternalKey key;

        Pair<Integer, Integer> pair = coding.decodeVarint32(buffer, offset);
        while (msg == null && pair != null) {
            tag = pair.getKey();
            offset = pair.getValue();

            if (tag == kComparator) {
                Pair<String, Integer> tmp = coding.getLengthPrefixedString(buffer, offset);

                if (tmp != null) {
                    setComparatorName(tmp.getKey());
                    offset = tmp.getValue();
                } else {
                    msg = "comparator name";
                }
            } else if (tag == kLogNumber) {
                Pair<Long, Integer> tmp = coding.decodeVarint64(buffer, offset);
                if (tmp != null) {
                    setLogNumber(tmp.getKey());
                    offset = tmp.getValue();
                } else {
                    msg = "log number";
                }
            } else if (tag == kPrevLogNumber) {
                Pair<Long, Integer> tmp = coding.decodeVarint64(buffer, offset);
                if (tmp != null) {
                    setPrevLogNumber(tmp.getKey());
                    offset = tmp.getValue();
                } else {
                    msg = "previous log number";
                }
            } else if (tag == kNextFileNumber) {
                Pair<Long, Integer> tmp = coding.decodeVarint64(buffer, offset);
                if (tmp != null) {
                    setNextFileNumber(tmp.getKey());
                    offset = tmp.getValue();
                } else {
                    msg = "next file number";
                }
            } else if (tag == kLastSequence) {
                Pair<Long, Integer> tmp = coding.decodeVarint64(buffer, offset);
                if (tmp != null) {
                    setLastSequence(tmp.getKey());
                    offset = tmp.getValue();
                } else {
                    msg = "last sequence number";
                }
            } else if (tag == kCompactPointer) {
                try {
                    Pair<Integer, Integer> tmp = coding.decodeVarint32(buffer, offset);
                    assert tmp != null;
                    level = tmp.getKey();
                    offset = tmp.getValue();

                    Pair<String, Integer> abc = coding.getLengthPrefixedString(buffer, offset);
                    assert abc != null;
                    key = InternalKey.decode(abc.getKey());
                    offset = abc.getValue();
                    compactPointers.add(new Pair<>(level, key));
                } catch (AssertionError e) {
                    msg = "compaction pointer";
                }
            } else if (tag == kDeletedFile) {
                try {
                    Pair<Integer, Integer> tmp = coding.decodeVarint32(buffer, offset);
                    assert tmp != null;
                    level = tmp.getKey();
                    offset = tmp.getValue();

                    Pair<Long, Integer> abc = coding.decodeVarint64(buffer, offset);
                    assert abc != null;
                    offset = abc.getValue();
                    deletedFiles.add(new Pair<>(level, abc.getKey()));
                } catch (AssertionError e) {
                    msg = "deleted file";
                }
            } else if (tag == kNewFile) {
                try {
                    Pair<Integer, Integer> tmp = coding.decodeVarint32(buffer, offset);
                    assert tmp != null;
                    level = tmp.getKey();
                    offset = tmp.getValue();

                    Pair<Long, Integer> abc = coding.decodeVarint64(buffer, offset);
                    assert abc != null;
                    Long fileNumber = abc.getKey();
                    offset = abc.getValue();

                    abc = coding.decodeVarint64(buffer, offset);
                    assert abc != null;
                    Long fileSize = abc.getKey();
                    offset = abc.getValue();

                    Pair<String, Integer> def = coding.getLengthPrefixedString(buffer, offset);
                    assert def != null;
                    InternalKey smallest = InternalKey.decode(def.getKey());
                    offset = def.getValue();

                    def = coding.getLengthPrefixedString(buffer, offset);
                    assert def != null;
                    InternalKey largest = InternalKey.decode(def.getKey());
                    offset = def.getValue();

                    newFiles.add(new Pair<>(level, new FileMetaData(fileNumber, fileSize, smallest, largest)));
                } catch (AssertionError e) {
                    msg = "new-file entry";
                }
            } else {
                msg = "unknown tag";
            }

            pair = coding.decodeVarint32(buffer, offset);
        }

        if (msg == null && offset != buffer.length) {
            msg = "invalid tag";
        }

        Status result = Status.OK();
        if (msg != null) {
            result = Status.Corruption("VersionEdit", msg);
        }
        return result;
    }

    public void clear() {
        this.comparatorName = "";
        logNumber = 0;
        prevLogNumber = 0;
        lastSequence = 0;
        nextFileNumber = 0;
        hasComparator = false;
        hasLogNumber = false;
        hasPrevLogNumber = false;
        hasNextFileNumber = false;
        hasLastSequence = false;
        deletedFiles.clear();
        newFiles.clear();
    }
}
