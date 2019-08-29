package com.farmerworking.leveldb.in.java.data.structure.db;

import com.farmerworking.leveldb.in.java.api.Options;
import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.data.structure.log.LogWriter;
import com.farmerworking.leveldb.in.java.data.structure.memory.Memtable;
import com.farmerworking.leveldb.in.java.data.structure.version.VersionEdit;
import com.farmerworking.leveldb.in.java.file.FileName;
import com.farmerworking.leveldb.in.java.file.WritableFile;
import javafx.util.Pair;

public interface DB {
    static Pair<Status, DB> open(Options options, String dbname) {
        DBImpl db = new DBImpl(options, dbname);
        db.getMutex().lock();
        VersionEdit edit = new VersionEdit();
        // Recover handles createIfMissing, errorIfExists
        Pair<Status, Boolean> pair = db.recover(edit);
        Status status = pair.getKey();
        Boolean saveManifest = pair.getValue();

        if (status.isOk() && db.getMemtable() == null) {
            long newLogNumber = db.getVersions().newFileNumber();
            Pair<Status, WritableFile> filePair = options.getEnv().newWritableFile(FileName.logFileName(dbname, newLogNumber));
            status = filePair.getKey();
            WritableFile logFile = filePair.getValue();

            if (status.isOk()) {
                edit.setLogNumber(newLogNumber);
                db.setLogFile(logFile);
                db.setLogFileNumber(newLogNumber);
                db.setLog(new LogWriter(logFile));
                db.setMemtable(new Memtable(db.getInternalKeyComparator()));
            }
        }

        if (status.isOk() && saveManifest) {
            edit.setPrevLogNumber(0); // No older logs needed after recovery
            edit.setLogNumber(db.getLogFileNumber());
            status = db.getVersions().logAndApply(edit, db.getMutex());
        }

        if (status.isOk()) {
            db.deleteObsoleteFiles();
            db.maybeScheduleCompaction();
        }
        db.getMutex().unlock();

        if (status.isOk()) {
            assert db.getMemtable() != null;
            return new Pair<>(status, db);
        } else {
            return new Pair<>(status, null);
        }
    }
}
