package com.farmerworking.leveldb.in.java.data.structure.db;

import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.data.structure.log.ILogReader;
import com.farmerworking.leveldb.in.java.data.structure.log.LogReader;
import com.farmerworking.leveldb.in.java.data.structure.memory.Memtable;
import com.farmerworking.leveldb.in.java.data.structure.version.VersionEdit;
import com.farmerworking.leveldb.in.java.data.structure.writebatch.MemTableInserter;
import com.farmerworking.leveldb.in.java.data.structure.writebatch.WriteBatch;
import com.farmerworking.leveldb.in.java.file.SequentialFile;
import javafx.util.Pair;
import lombok.Data;

@Data
public class Log2MemtableReader {
    private DBImpl db;
    private VersionEdit edit;
    private Status status;
    private LogReporter logReporter;
    private ILogReader logReader;
    private Long maxSequence;
    private boolean saveManifest;
    private int compactions;
    private Memtable memtable;
    private WriteBatch batch;


    public Log2MemtableReader() {}

    public Log2MemtableReader(DBImpl db, VersionEdit edit, String filename, SequentialFile file) {
        this.db = db;
        this.edit = edit;

        this.batch = new WriteBatch();

        // state needed by caller
        this.status = Status.OK();
        this.maxSequence = null;
        this.saveManifest = false;
        this.compactions = 0;
        this.memtable = null;

        this.logReporter = new LogReporter(db.getOptions().getInfoLog(), filename, db.getOptions().isParanoidChecks() ? status : null);
        // We intentionally make log::Reader do checksumming even if
        // paranoid_checks==false so that corruptions cause entire commits
        // to be skipped instead of propagating bad information (like overly
        // large sequence numbers).
        this.logReader = new LogReader(file, logReporter, true, 0);
    }

    public Log2MemtableReader invoke() {
        assert db.getMutex().isHeldByCurrentThread();

        // Read all the records and add to a memtable
        for(Pair<Boolean, String> read = readLogRecord(logReader);
            status.isOk() && read.getKey();
            read = readLogRecord(logReader)) {
            if (read.getValue().length() < WriteBatch.kHeaderSize) {
                logReporter.corruption(read.getValue().length(), Status.Corruption("log record too small"));
                continue;
            }

            batch.decode(read.getValue().toCharArray());
            if (memtable == null) {
                memtable = new Memtable(db.getInternalKeyComparator());
            }

            MemTableInserter memTableInserter = new MemTableInserter(batch.getSequence(), memtable);
            status = iterateBatch(batch, memTableInserter);
            status = db.maybeIgnoreError(status);
            if (status.isNotOk()) {
                break;
            }

            long lastSeq = batch.getSequence() + batch.getCount() - 1;
            if (maxSequence == null || lastSeq > maxSequence) {
                maxSequence = lastSeq;
            }

            if (memtable.approximateMemoryUsage() > getWriteBufferSize()) {
                compactions ++;
                saveManifest = true;
                status = writeLevel0Table();
                memtable = null;
                if (status.isNotOk()) {
                    // Reflect errors immediately so that conditions like full
                    // file-systems cause the DB::Open() to fail.
                    break;
                }
            }
        }
        return this;
    }

    Pair<Boolean, String> readLogRecord(ILogReader logReader) {
        return logReader.readRecord();
    }

    Status iterateBatch(WriteBatch batch, MemTableInserter memTableInserter) {
        return batch.iterate(memTableInserter);
    }

    int getWriteBufferSize() {
        return db.getOptions().getWriteBufferSize();
    }

    Status writeLevel0Table() {
        return db.writeLevel0Table(memtable, edit, null);
    }
}
