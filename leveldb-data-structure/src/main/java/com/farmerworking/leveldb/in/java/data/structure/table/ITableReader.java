package com.farmerworking.leveldb.in.java.data.structure.table;

import com.farmerworking.leveldb.in.java.api.Iterator;
import com.farmerworking.leveldb.in.java.api.Options;
import com.farmerworking.leveldb.in.java.api.ReadOptions;
import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.file.RandomAccessFile;

public interface ITableReader {
    // Returns a new iterator over the table contents.
    // The result of iterator() is initially invalid (caller must
    // call one of the Seek methods on the iterator before using it).
    Iterator<String, String> iterator(ReadOptions readOptions);

    // Given a key, return an approximate byte offset in the file where
    // the data for that key begins (or would begin if the key were
    // present in the file).  The returned value is in terms of file
    // bytes, and so includes effects like compression of the underlying data.
    // E.g., the approximate offset of the last key in the table will
    // be close to the file length.
    long approximateOffsetOf(String key);

    Status status();

    Status open(Options options, RandomAccessFile file, long size);

    public static ITableReader getDefaultImpl() {
        return new TableReader();
    }
}
