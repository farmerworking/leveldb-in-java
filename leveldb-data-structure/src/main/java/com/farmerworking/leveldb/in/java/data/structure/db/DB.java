package com.farmerworking.leveldb.in.java.data.structure.db;

import com.farmerworking.leveldb.in.java.api.*;
import com.farmerworking.leveldb.in.java.data.structure.log.LogWriter;
import com.farmerworking.leveldb.in.java.data.structure.memory.Memtable;
import com.farmerworking.leveldb.in.java.data.structure.version.VersionEdit;
import com.farmerworking.leveldb.in.java.data.structure.writebatch.WriteBatch;
import com.farmerworking.leveldb.in.java.file.Env;
import com.farmerworking.leveldb.in.java.file.FileName;
import com.farmerworking.leveldb.in.java.file.FileType;
import com.farmerworking.leveldb.in.java.file.WritableFile;
import javafx.util.Pair;

import java.nio.channels.FileLock;
import java.util.List;

public interface DB {
    Status write(WriteOptions writeOptions, WriteBatch batch) ;

    Iterator<String, String> iterator(ReadOptions readOptions);

    Pair<Status, String> get(ReadOptions readOptions, String key);

    Status put(WriteOptions writeOptions, String key, String value);

    Status delete(WriteOptions writeOptions, String key);

    Pair<Boolean, String> getProperty(String property);

    int numLevelFiles(int level);

    long getSnapshot();

    void releaseSnapshot(long snapshot);

    void compactRange(String begin, String end);

    void close();

    Status TEST_compactMemtable();

    void TEST_compactRange(int level, String begin, String end);

    Iterator<String, String> TEST_newInternalIterator();

    long TEST_maxNextLevelOverlappingBytes();

    List<Long> getApproximateSizes(List<Pair<String, String>> range, int n);

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
            db.close();
            return new Pair<>(status, null);
        }
    }

    static Status destroyDB(String dbname, Options options) {
        Env env = options.getEnv();
        Pair<Status, List<String>> children = env.getChildren(dbname);
        if (children.getValue() == null || children.getValue().isEmpty()) {
            return Status.OK();
        }

        String lockname = FileName.lockFileName(dbname);
        Pair<Status, FileLock> lock = env.lockFile(lockname);
        Status result = lock.getKey();
        if (result.isOk()) {
            for (int i = 0; i < children.getValue().size(); i++) {
                Pair<Long, FileType> parse = FileName.parseFileName(children.getValue().get(i));

                // Lock file will be deleted at end
                if (parse != null && parse.getValue() != FileType.kDBLockFile) {
                    Status del = env.delete(dbname + "/" + children.getValue().get(i)).getKey();
                    if (result.isOk() && del.isNotOk()) {
                        result = del;
                    }
                }
            }
            env.unlockFile(lockname, lock.getValue());
            env.delete(lockname);
            env.delete(dbname);
        }
        return result;
    }
}
