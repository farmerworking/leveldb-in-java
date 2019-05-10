package com.farmerworking.leveldb.in.java.file.impl;

import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.file.Env;
import com.farmerworking.leveldb.in.java.file.RandomAccessFile;
import com.farmerworking.leveldb.in.java.file.SequentialFile;
import com.farmerworking.leveldb.in.java.file.WritableFile;
import javafx.util.Pair;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class DefaultEnv implements Env {
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
    public Pair<Status, Boolean> isFileExists(String filename) {
        try {
            return new Pair<>(Status.OK(), Files.exists(Paths.get(filename)));
        } catch (Exception e) {
            return new Pair<>(Status.IOError(e.getMessage()), null);
        }
    }
}
