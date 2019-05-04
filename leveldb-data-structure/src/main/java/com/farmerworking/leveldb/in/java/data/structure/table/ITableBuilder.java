package com.farmerworking.leveldb.in.java.data.structure.table;

import com.farmerworking.leveldb.in.java.api.Options;
import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.file.WritableFile;

public interface ITableBuilder {
    // Change the options used by this builder.  Note: only some of the
    // option fields can be changed after construction.  If a field is
    // not allowed to change dynamically and its value in the structure
    // passed to the constructor is different from its value in the
    // structure passed to this method, this method will return an error
    // without changing any fields.
    Status changeOptions(Options options);

    // Add key,value to the table being constructed.
    // REQUIRES: key is after any previously added key according to comparator.
    // REQUIRES: finish(), abandon() have not been called
    void add(String key, String value);

    // Advanced operation: flush any buffered key/value pairs to file.
    // Can be used to ensure that two adjacent entries never live in
    // the same data block.  Most clients should not need to use this method.
    // REQUIRES: finish(), abandon() have not been called
    void flush();

    // Return non-ok iff some error has been detected.
    Status status();

    // finish building the table.  Stops using the file passed to the
    // constructor after this function returns.
    // REQUIRES: finish(), abandon() have not been called
    Status finish();

    // Indicate that the contents of this builder should be abandoned.  Stops
    // using the file passed to the constructor after this function returns.
    // If the caller is not going to call finish(), it must call abandon()
    // before destroying this builder.
    // REQUIRES: finish(), abandon() have not been called
    void abandon();

    // Number of calls to add() so far.
    long numEntries();

    // Size of the file generated so far.  If invoked after a successful
    // finish() call, returns the size of the final generated file.
    long fileSize();

    public static ITableBuilder getDefaultImpl(Options options, WritableFile file) {
        return new TableBuilder(options, file);
    }
}
