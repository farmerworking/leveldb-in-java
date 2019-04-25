package com.farmerworking.leveldb.in.java.data.structure.memory;

import com.farmerworking.leveldb.in.java.common.Status;
import com.farmerworking.leveldb.in.java.data.structure.Iterator;
import javafx.util.Pair;

public interface IMemtable {
    // Return an iterator that yields the contents of the memtable.
    //
    // The caller must ensure that the underlying MemTable remains live
    // while the returned iterator is live.  The keys returned by this
    // iterator are internal keys encoded by AppendInternalKey
    Iterator<InternalKey, String> iterator();

    // Add an entry into memtable that maps key to value at the
    // specified sequence number and with the specified type.
    // Typically value will be empty if type==kTypeDeletion.
    void add(long sequence, ValueType type, String key, String value);

    // If memtable contains a value for key, store it in value and return true.
    // If memtable contains a deletion for key, store a NotFound() error
    // in status and return true.
    // Else, return false.
    Pair<Boolean, Pair<Status, String>> get(String userKey, long sequence);

    // Returns an estimate of the number of bytes of data in use by this
    // data structure. It is safe to call when MemTable is being modified.
    int approximateMemoryUsage();
}
