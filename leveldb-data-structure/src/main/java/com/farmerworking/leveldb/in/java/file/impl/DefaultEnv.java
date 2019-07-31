package com.farmerworking.leveldb.in.java.file.impl;

import com.farmerworking.leveldb.in.java.api.Options;
import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.file.Env;
import com.farmerworking.leveldb.in.java.file.RandomAccessFile;
import com.farmerworking.leveldb.in.java.file.SequentialFile;
import com.farmerworking.leveldb.in.java.file.WritableFile;
import com.google.common.collect.Lists;
import javafx.util.Pair;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DefaultEnv implements Env {
    private Set<String> locks = new HashSet<>();

    @Override
    public Pair<Status, WritableFile> newWritableFile(String filename) {
        try {
            FileOutputStream fileOutputStream = new FileOutputStream(filename, false);
            return new Pair<>(Status.OK(), new DefaultWritableFile(fileOutputStream));
        } catch (IOException e) {
            return new Pair<>(Status.IOError(e.getMessage()), null);
        }
    }

    @Override
    public Pair<Status, WritableFile> newAppendableFile(String filename) {
        try {
            FileOutputStream fileOutputStream = new FileOutputStream(filename, true);
            return new Pair<>(Status.OK(), new DefaultWritableFile(fileOutputStream));
        } catch (IOException e) {
            return new Pair<>(Status.IOError(e.getMessage()), null);
        }
    }

    @Override
    public Pair<Status, RandomAccessFile> newRandomAccessFile(String filename) {
        try {
            java.io.RandomAccessFile randomAccessFile = new java.io.RandomAccessFile(filename, "r");
            return new Pair<>(Status.OK(), new DefaultRandomAccessFile(randomAccessFile));
        } catch (FileNotFoundException e) {
            return new Pair<>(Status.NotFound(e.getMessage()), null);
        } catch (IOException e) {
            return new Pair<>(Status.IOError(e.getMessage()), null);
        }
    }

    @Override
    public Pair<Status, SequentialFile> newSequentialFile(String filename) {
        try {
            FileInputStream fileInputStream = new FileInputStream(filename);
            return new Pair<>(Status.OK(), new DefaultSequentialFile(fileInputStream));
        } catch (FileNotFoundException e) {
            return new Pair<>(Status.NotFound(e.getMessage()), null);
        } catch (IOException e) {
            return new Pair<>(Status.IOError(e.getMessage()), null);
        }
    }

    @Override
    public Pair<Status, String> getTestDirectory() {
        String directory = String.format("/tmp/leveldbtest-%s", ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
        try {
            Path path = Paths.get(directory);
            if (!Files.exists(path)) {
                Files.createDirectory(path);
            }
            return new Pair<>(Status.OK(), path.toString());
        } catch (IOException e) {
            return new Pair<>(Status.IOError(e.getMessage()), null);
        }
    }

    @Override
    public Pair<Status, Boolean> delete(String filename) {
        try {
            return new Pair<>(Status.OK(), Files.deleteIfExists(Paths.get(filename)));
        } catch (IOException e) {
            return new Pair<>(Status.IOError(e.getMessage()), null);
        }
    }

    @Override
    public boolean isFileExists(String filename) {
        return Files.exists(Paths.get(filename));
    }

    @Override
    public Pair<Status, Long> getFileSize(String filename) {
        Status status = Status.OK();
        Long result = null;

        File file = new File(filename);
        if (file.exists()) {
            result = file.length();
        } else {
            status = Status.IOError(filename);
        }
        return new Pair<>(status, result);
    }

    @Override
    public Status renameFile(String from, String to) {
        Status result = Status.OK();
        if (!new File(from).renameTo(new File(to))) {
            result = Status.IOError(from);
        }
        return result;
    }

    @Override
    public Status createDir(String name) {
        File file = new File(name);
        if (file.mkdirs()) {
            return Status.OK();
        } else {
            return Status.IOError("create directory: name error");
        }
    }

    @Override
    public Pair<Status, Options.Logger> newLogger(String logFileName) {
        return new Pair<>(Status.OK(), new LogImpl(logFileName));
    }

    @Override
    public Pair<Status, List<String>> getChildren(String dbname) {
        File directory = new File(dbname);

        if (!directory.exists()) {
            return new Pair<>(Status.IOError(dbname), null);
        }

        File[] files = directory.listFiles();
        List<String> result = Lists.newArrayList();
        if (files == null) {
            return new Pair<>(Status.OK(), result);
        } else {
            for(File file : files) {
                result.add(file.getName());
            }
            return new Pair<>(Status.OK(), result);
        }
    }

    @Override
    public Pair<Status, FileLock> lockFile(String lockFileName) {
        Path path = Paths.get(lockFileName);
        try {
            FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);

            if (!addLock(lockFileName)) {
                channel.close();
                return new Pair<>(Status.IOError(String.format("lock %s already held by process", lockFileName)), null);
            }

            FileLock lock = channel.tryLock();
            if (lock == null) {
                channel.close();
                removeLock(lockFileName);
                return new Pair<>(Status.IOError("lock " + lockFileName), null);
            }

            return new Pair<>(Status.OK(), lock);
        } catch (IOException e) {
            return new Pair<>(Status.IOError(lockFileName), null);
        }
    }

    private synchronized boolean addLock(String lockFileName) {
        return locks.add(lockFileName);
    }

    private synchronized boolean removeLock(String lockFileName) {
        return locks.remove(lockFileName);
    }
}
