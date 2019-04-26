package com.farmerworking.leveldb.in.java.file;

import com.farmerworking.leveldb.in.java.api.Status;

// A file abstraction for sequential writing.  The implementation
// must provide buffering since callers may append small fragments
// at a time to the file.
public interface WritableFile {
    Status append(String data);

    // flush memory to disk if implementation use memory buffer to speed up write
    // data will not lost once flush method return if program is killed but may lost if machine is power off
    Status flush();

    Status close();

    Status sync();
}
