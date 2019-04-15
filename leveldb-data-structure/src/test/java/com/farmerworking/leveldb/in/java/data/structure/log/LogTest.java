package com.farmerworking.leveldb.in.java.data.structure.log;

import com.farmerworking.leveldb.in.java.data.structure.ICRC32C;
import com.farmerworking.leveldb.in.java.data.structure.ICoding;
import com.farmerworking.leveldb.in.java.file.SequentialFile;
import com.farmerworking.leveldb.in.java.file.WritableFile;

public class LogTest extends ILogTest {
    @Override
    protected ICRC32C getCrc32CImpl() {
        return ICRC32C.getDefaultImpl();
    }

    @Override
    protected ICoding getCodingImpl() {
        return ICoding.getDefaultImpl();
    }

    @Override
    protected ILogReader getLogReaderImpl(SequentialFile sequentialFile, ILogReporter logReporter, boolean checksum, long initialOffset) {
        return new LogReader(sequentialFile, logReporter, checksum, initialOffset);
    }

    @Override
    protected ILogWriter getLogWriterImpl(WritableFile writableFile) {
        return new LogWriter(writableFile);
    }

    @Override
    protected ILogWriter getLogWriterImpl(WritableFile writableFile, long offset) {
        return new LogWriter(writableFile, offset);
    }
}
