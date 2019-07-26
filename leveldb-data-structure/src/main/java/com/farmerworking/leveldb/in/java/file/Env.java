package com.farmerworking.leveldb.in.java.file;

import com.farmerworking.leveldb.in.java.api.Options;
import com.farmerworking.leveldb.in.java.api.Status;
import javafx.util.Pair;
import org.apache.commons.lang3.StringUtils;

import java.nio.channels.FileLock;
import java.util.Collection;

public interface Env {
    Pair<Status, WritableFile> newWritableFile(String filename) ;

    Pair<Status, WritableFile> newAppendableFile(String filename) ;

    Pair<Status, RandomAccessFile> newRandomAccessFile(String filename);

    Pair<Status, SequentialFile> newSequentialFile(String filename);

    Pair<Status, String> getTestDirectory();

    Pair<Status, Boolean> delete(String filename);

    Pair<Status, Boolean> isFileExists(String filename);

    Pair<Status, Long> getFileSize(String filename);

    Status renameFile(String from, String to);

    Status createDir(String name);

    Pair<Status, Options.Logger> newLogger(String logFileName);

    Pair<Status, Collection<String>> getChildren(String dbname);

    Pair<Status, FileLock> lockFile(String lockFileName);

    static Pair<Status, String> readFileToString(Env env, String fname) {
        Pair<Status, SequentialFile> pair = env.newSequentialFile(fname);
        Status status = pair.getKey();

        if (status.isNotOk()) {
            return new Pair<>(status, null);
        }
        SequentialFile file = pair.getValue();

        String result = "";
        while(true) {
            Pair<Status, String> readPair = file.read(8192);
            status = readPair.getKey();

            if (status.isNotOk()) {
                break;
            }
            result += readPair.getValue();
            if (StringUtils.isEmpty(readPair.getValue())) {
                break;
            }
        }

        return new Pair<>(status, result);
    }

    static Status writeStringToFileSync(Env env, String s, String fname) {
        return doWriteStringToFile(env, s, fname, true);
    }

    static Status doWriteStringToFile(Env env, String data, String fname, boolean shouldSync) {
        Pair<Status, WritableFile> pair = env.newWritableFile(fname);
        Status status = pair.getKey();
        if (status.isNotOk()) {
            return pair.getKey();
        }

        WritableFile file = pair.getValue();
        status = file.append(data);

        if (status.isOk() && shouldSync) {
            status = file.sync();
        }

        if (status.isOk()) {
            status = file.close();
        }

        if (status.isNotOk()) {
            env.delete(fname);
        }

        return status;
    }
}
