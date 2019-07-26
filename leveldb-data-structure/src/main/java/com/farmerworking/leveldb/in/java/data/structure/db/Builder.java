package com.farmerworking.leveldb.in.java.data.structure.db;

import com.farmerworking.leveldb.in.java.api.Iterator;
import com.farmerworking.leveldb.in.java.api.Options;
import com.farmerworking.leveldb.in.java.api.ReadOptions;
import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.data.structure.cache.TableCache;
import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKey;
import com.farmerworking.leveldb.in.java.data.structure.table.TableBuilder;
import com.farmerworking.leveldb.in.java.data.structure.version.FileMetaData;
import com.farmerworking.leveldb.in.java.file.Env;
import com.farmerworking.leveldb.in.java.file.FileName;
import com.farmerworking.leveldb.in.java.file.WritableFile;
import javafx.util.Pair;

public class Builder {
    // Build a Table file from the contents of *iter.  The generated file
    // will be named according to meta->number.  On success, the rest of
    // *meta will be filled with metadata about the generated table.
    // If no data is present in *iter, meta->file_size will be set to
    // zero, and no Table file will be produced.
    public Status buildTable(String dbname,
                             Env env,
                             Options options,
                             TableCache tableCache,
                             Iterator<InternalKey, String> iter,
                             FileMetaData metaData) {
        metaData.setFileSize(0);
        iter.seekToFirst();

        Status status = Status.OK();
        String filename = FileName.tableFileName(dbname, metaData.getFileNumber());
        if (iter.valid()) {
            Pair<Status, WritableFile> pair = env.newWritableFile(filename);
            status = pair.getKey();
            if (status.isNotOk()) {
                return status;
            }

            TableBuilder builder = new TableBuilder(options, pair.getValue());
            metaData.setSmallest(iter.key());
            for (; iter.valid(); iter.next()) {
                metaData.setLargest(iter.key());
                builder.add(iter.key().encode(), iter.value());
            }

            if (status.isOk()) {
                status = finish(builder);
                if (status.isOk()) {
                    metaData.setFileSize(builder.fileSize());
                    assert metaData.getFileSize() > 0;
                }
            } else {
                builder.abandon();
            }

            if (status.isOk()) {
                status = sync(pair.getValue());
            }

            if (status.isOk()) {
                status = close(pair.getValue());
            }

            if (status.isOk()) {
                Iterator<String, String> iterator = tableCache.iterator(new ReadOptions(), metaData.getFileNumber(), metaData.getFileSize()).getKey();
                status = status(iterator);
                iterator.close();
            }
        }

        if (iter.status().isNotOk()) {
            status = iter.status();
        }

        if (status.isOk() && metaData.getFileSize() > 0) {
            // keep it
        } else {
            env.delete(filename);
        }

        return status;
    }

    Status status(Iterator<String, String> iterator) {
        return iterator.status();
    }

    Status sync(WritableFile value) {
        return value.sync();
    }

    Status close(WritableFile value) {
        return value.close();
    }

    Status finish(TableBuilder builder) {
        return builder.finish();
    }
}
