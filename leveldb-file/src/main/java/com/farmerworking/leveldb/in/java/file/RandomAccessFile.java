package com.farmerworking.leveldb.in.java.file;

import com.farmerworking.leveldb.in.java.api.Status;
import javafx.util.Pair;

// A file abstraction for randomly reading the contents of a file.
public interface RandomAccessFile {
    // Read up to "n" bytes from the file starting at "offset".
    // If an error was encountered, returns a non-OK status.
    //
    // Safe for concurrent use by multiple threads.
    Pair<Status, String> read(long offset, int n);
}
