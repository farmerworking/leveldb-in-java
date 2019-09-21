package com.farmerworking.leveldb.in.java.data.structure.db;

import java.nio.channels.FileLock;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import com.farmerworking.leveldb.in.java.api.CompressionType;
import com.farmerworking.leveldb.in.java.api.FilterPolicy;
import com.farmerworking.leveldb.in.java.api.Iterator;
import com.farmerworking.leveldb.in.java.api.Options;
import com.farmerworking.leveldb.in.java.api.Options.Logger;
import com.farmerworking.leveldb.in.java.api.ReadOptions;
import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.api.WriteOptions;
import com.farmerworking.leveldb.in.java.data.structure.filter.BloomFilterPolicy;
import com.farmerworking.leveldb.in.java.data.structure.writebatch.WriteBatch;
import com.farmerworking.leveldb.in.java.file.Env;
import com.farmerworking.leveldb.in.java.file.RandomAccessFile;
import com.farmerworking.leveldb.in.java.file.SequentialFile;
import com.farmerworking.leveldb.in.java.file.WritableFile;
import com.farmerworking.leveldb.in.java.file.impl.DefaultEnv;
import com.sun.corba.se.spi.ior.Writeable;
import javafx.util.Pair;
import static org.junit.Assert.*;

public class DBTest {
    enum OptionConfig {
        kDefault(0),
        kReuse(1),
        kFilter(2),
        kUncompressed(3),
        kEnd(4);

        private int value;

        OptionConfig(int value) {
            this.value = value;
        }

        OptionConfig next() {
            return OptionConfig.valueOf(this.value + 1);
        }

        static OptionConfig valueOf(int value) {
            if (value == 0) {
                return OptionConfig.kDefault;
            } else if (value == 1) {
                return OptionConfig.kReuse;
            } else if (value == 2) {
                return OptionConfig.kFilter;
            } else if (value == 3) {
                return OptionConfig.kUncompressed;
            } else if (value == 4) {
                return OptionConfig.kEnd;
            } else {
                return null;
            }
        }
    }

    class SpecialEnv implements Env {
        private final DefaultEnv env;
        AtomicBoolean delayDataSync = new AtomicBoolean(false);

        public SpecialEnv(DefaultEnv env) {
            this.env = env;
        }

        class DataFile implements WritableFile {
            SpecialEnv env;
            WritableFile base;

            public DataFile(SpecialEnv env, WritableFile base) {
                this.env = env;
                this.base = base;
            }

            @Override
            public Status append(String data) {
                return this.base.append(data);
            }

            @Override
            public Status flush() {
                return this.base.flush();
            }

            @Override
            public Status close() {
                return this.base.close();
            }

            @Override
            public Status sync() {
                while (this.env.delayDataSync.get()) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                    }
                }
                return this.base.sync();
            }
        }

        class ManifestFile implements WritableFile {
            SpecialEnv env;
            WritableFile base;

            public ManifestFile(SpecialEnv env, WritableFile base) {
                this.env = env;
                this.base = base;
            }

            @Override
            public Status append(String data) {
                return this.base.append(data);
            }

            @Override
            public Status flush() {
                return this.base.flush();
            }

            @Override
            public Status close() {
                return this.base.close();
            }

            @Override
            public Status sync() {
                return this.base.sync();
            }
        }

        @Override
        public Pair<Status, WritableFile> newWritableFile(String filename) {
            Pair<Status, WritableFile> pair = this.env.newWritableFile(filename);
            if (pair.getKey().isOk()) {
                if (filename.contains(".ldb") || filename.contains(".log")) {
                    return new Pair<>(pair.getKey(), new DataFile(this, pair.getValue()));
                } else if (filename.contains("MANIFEST")) {
                    return new Pair<>(pair.getKey(), new ManifestFile(this, pair.getValue()));
                }
            }

            return pair;
        }

        @Override
        public Pair<Status, WritableFile> newAppendableFile(String filename) {
            return this.env.newAppendableFile(filename);
        }

        @Override
        public Pair<Status, RandomAccessFile> newRandomAccessFile(String filename) {
            return this.env.newRandomAccessFile(filename);
        }

        @Override
        public Pair<Status, SequentialFile> newSequentialFile(String filename) {
            return this.env.newSequentialFile(filename);
        }

        @Override
        public Pair<Status, String> getTestDirectory() {
            return this.env.getTestDirectory();
        }

        @Override
        public Pair<Status, Boolean> delete(String filename) {
            return this.env.delete(filename);
        }

        @Override
        public boolean isFileExists(String filename) {
            return this.env.isFileExists(filename);
        }

        @Override
        public Pair<Status, Long> getFileSize(String filename) {
            return this.env.getFileSize(filename);
        }

        @Override
        public Status renameFile(String from, String to) {
            return this.env.renameFile(from, to);
        }

        @Override
        public Status createDir(String name) {
            return this.env.createDir(name);
        }

        @Override
        public Pair<Status, Logger> newLogger(String logFileName) {
            return this.env.newLogger(logFileName);
        }

        @Override
        public Pair<Status, List<String>> getChildren(String dbname) {
            return this.env.getChildren(dbname);
        }

        @Override
        public Pair<Status, FileLock> lockFile(String lockFileName) {
            return this.env.lockFile(lockFileName);
        }

        @Override
        public Future schedule(Runnable runnable) {
            return this.env.schedule(runnable);
        }

        @Override
        public Status unlockFile(String lockFileName, FileLock fileLock) {
            return this.env.unlockFile(lockFileName, fileLock);
        }
    }

    DB db;
    SpecialEnv env;
    private String dbname;
    private Options lastOptions;
    private FilterPolicy filterPolicy;
    private OptionConfig optionConfig;

    public DBTest() {
        this.optionConfig = OptionConfig.kDefault;
        this.env = new SpecialEnv(new DefaultEnv());
        this.filterPolicy = new BloomFilterPolicy(10);
        this.dbname = env.getTestDirectory().getValue() + "/db_test";
        DB.destroyDB(this.dbname, new Options());
        this.db = null;
        reopen();
    }

    void reopen() {
        assertTrue(tryReopen().isOk());
    }

    void reopen(Options options) {
        assertTrue(tryReopen(options).isOk());
    }

    // Switch to a fresh database with the next option configuration to
    // test.  Return false if there are no more configurations to test.
    boolean changeOptions() {
        optionConfig = optionConfig.next();
        if (optionConfig == null || optionConfig == OptionConfig.kEnd) {
            return false;
        } else {
            destroyAndReopon();
            return true;
        }
    }

    private void destroyAndReopon() {
        destroy();
        assertTrue(tryReopen().isOk());
    }

    void destroy() {
        if (this.db != null) {
            this.db.close();
        }
        this.db = null;
        DB.destroyDB(this.dbname, new Options());
    }

    private Status tryReopen() {
        if (this.db != null) {
            this.db.close();
        }
        this.db = null;
        Options options = currentOptions();
        options.setCreateIfMissing(true);
        this.lastOptions = options;
        Pair<Status, DB> pair = DB.open(options, this.dbname);
        if (pair.getKey().isOk()) {
            this.db = pair.getValue();
        }
        return pair.getKey();
    }

    private Status tryReopen(Options options) {
        if (this.db != null) {
            this.db.close();
        }
        this.db = null;
        this.lastOptions = options;
        Pair<Status, DB> pair = DB.open(options, this.dbname);
        if (pair.getKey().isOk()) {
            this.db = pair.getValue();
        }
        return pair.getKey();
    }

    Options currentOptions() {
        Options options = new Options();
        options.setReuseLogs(false);
        switch (this.optionConfig) {
            case kReuse:
                options.setReuseLogs(true);
                break;
            case kFilter:
                options.setFilterPolicy(this.filterPolicy);
                break;
            case kUncompressed:
                options.setCompression(CompressionType.kNoCompression);
                break;
            default:
                break;
        }
        return options;
    }

    String get(String key) {
        return get(key, null);
    }

    String get(String key, Long snapshot) {
        ReadOptions readOptions = new ReadOptions();
        readOptions.setSnapshot(snapshot);
        Pair<Status, String> pair = this.db.get(readOptions, key);
        String result = pair.getValue();
        if (pair.getKey().isNotFound()) {
            result = "NOT_FOUND";
        } else if (pair.getKey().isNotOk()) {
            result = pair.getKey().toString();
        }

        return result;
    }

    Status put(String key, String value) {
        return this.db.put(new WriteOptions(), key, value);
    }

    String iterStatus(Iterator iterator) {
        String result;

        if (iterator.valid()) {
            result = iterator.key().toString() + "->" + iterator.value().toString();
        } else {
            result = "(invalid)";
        }

        return result;
    }

    int numTableFilesAtLevel(int level) {
        Pair<Boolean, String> pair = db.getProperty("leveldb.num-files-at-level" + level);
        assertTrue(pair.getKey());
        return Integer.valueOf(pair.getValue());
    }
}