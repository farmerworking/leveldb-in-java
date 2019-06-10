package com.farmerworking.leveldb.in.java.file;

import com.farmerworking.leveldb.in.java.api.Status;
import javafx.util.Pair;

public interface SequentialFile {
    // Read up to "n" bytes from the file.
    // If an error was encountered, returns a non-OK status.
    //
    // REQUIRES: External synchronization
    Pair<Status, String> read(int n);

    // Skip "n" bytes from the file. This is guaranteed to be no
    // slower that reading the same data, but may be faster.
    //
    // If end of file is reached, skipping will stop at the end of the
    // file, and Skip will return OK.
    //
    // REQUIRES: External synchronization
    Status skip(long n);
}
