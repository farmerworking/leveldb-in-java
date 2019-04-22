package com.farmerworking.leveldb.in.java.data.structure;

import com.farmerworking.leveldb.in.java.common.Status;

public interface Iterator {
    // An iterator is either positioned at a key/value pair, or
    // not valid.  This method returns true iff the iterator is valid.
    boolean valid();

    // Position at the first key in the source.  The iterator is Valid()
    // after this call iff the source is not empty.
    void seekToFirst();

    // Position at the last key in the source.  The iterator is
    // Valid() after this call iff the source is not empty.
    void seekToLast();

    // Position at the first key in the source that is at or past target.
    // The iterator is Valid() after this call iff the source contains
    // an entry that comes at or past target.
    void seek(String target);

    // Moves to the next entry in the source.  After this call, Valid() is
    // true iff the iterator was not positioned at the last entry in the source.
    // REQUIRES: Valid()
    void next();

    // Moves to the previous entry in the source.  After this call, Valid() is
    // true iff the iterator was not positioned at the first entry in source.
    // REQUIRES: Valid()
    void prev();

    // Return the key for the current entry
    // REQUIRES: Valid()
    String key();

    // Return the value for the current entry
    // REQUIRES: Valid()
    String value();

    // If an error has occurred, return it. Else return an ok status.
    Status status();
}
