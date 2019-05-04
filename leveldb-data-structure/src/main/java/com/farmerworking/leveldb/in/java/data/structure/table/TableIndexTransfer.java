package com.farmerworking.leveldb.in.java.data.structure.table;

import com.farmerworking.leveldb.in.java.api.Iterator;
import com.farmerworking.leveldb.in.java.api.Options;
import com.farmerworking.leveldb.in.java.api.ReadOptions;
import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.data.structure.block.EmptyIterator;
import com.farmerworking.leveldb.in.java.data.structure.block.IBlockReader;
import com.farmerworking.leveldb.in.java.data.structure.two.level.iterator.IndexTransfer;
import com.farmerworking.leveldb.in.java.file.RandomAccessFile;
import javafx.util.Pair;

public class TableIndexTransfer implements IndexTransfer {
    private final Options options;
    private final RandomAccessFile file;

    public TableIndexTransfer(RandomAccessFile file, Options options) {
        this.file = file;
        this.options = options;
    }

    @Override
    public Iterator<String, String> transfer(ReadOptions options, String value) {
        BlockHandle handle = new BlockHandle();
        Pair<Status, Integer> decodeStatus = handle.decodeFrom(value.toCharArray(), 0);
        Status status = decodeStatus.getKey();

        IBlockReader blockReader = null;
        if (status.isOk()) {
            // todo: block cache
            Pair<Status, String> pair = TableReader.readBlock(file, options, handle);
            status = pair.getKey();
            if (status.isOk()) {
                blockReader = IBlockReader.getDefaultImpl(pair.getValue());
            }
        }

        Iterator<String, String> iter = null;
        if (blockReader != null) {
            iter = blockReader.iterator(this.options.getComparator());
        } else {
            iter = new EmptyIterator(status);
        }

        return iter;
    }
}
