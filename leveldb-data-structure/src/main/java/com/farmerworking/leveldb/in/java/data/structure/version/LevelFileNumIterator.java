package com.farmerworking.leveldb.in.java.data.structure.version;

import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.data.structure.iterator.AbstractIterator;
import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKey;
import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKeyComparator;
import javafx.util.Pair;

import java.util.Vector;

public class LevelFileNumIterator extends AbstractIterator<InternalKey, Pair<Long, Long>> {
    private int index;
    private Vector<FileMetaData> files;
    private InternalKeyComparator comparator;
    private FileRangeHelper fileRangeHelper;

    public LevelFileNumIterator(InternalKeyComparator comparator, Vector<FileMetaData> files) {
        this.comparator = comparator;
        this.files = files;
        this.index = files.size();
        this.fileRangeHelper = new FileRangeHelper();
    }

    @Override
    public boolean valid() {
        return index < files.size();
    }

    @Override
    public void seekToFirst() {
        index = 0;
    }

    @Override
    public void seekToLast() {
        index = files.isEmpty() ? 0 : files.size() - 1;
    }

    @Override
    public void seek(String target) {
        index = fileRangeHelper.findFile(comparator, files, target);
    }

    @Override
    public void next() {
        assert valid();
        index ++;
    }

    @Override
    public void prev() {
        assert valid();
        if (index == 0) {
            index = files.size();
        } else {
            index --;
        }
    }

    @Override
    public InternalKey key() {
        assert valid();
        return files.get(index).getLargest();
    }

    @Override
    public Pair<Long, Long> value() {
        assert valid();
        return new Pair<>(files.get(index).getFileNumber(), files.get(index).getFileSize());
    }

    @Override
    public Status status() {
        return Status.OK();
    }
}
