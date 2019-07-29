package com.farmerworking.leveldb.in.java.data.structure.db;

import com.farmerworking.leveldb.in.java.api.Options;
import com.farmerworking.leveldb.in.java.api.Status;
import com.google.gson.Gson;
import org.junit.Test;

import static org.junit.Assert.*;

public class LogReporterTest {
    @Test
    public void test1() {
        LogReporter reporter = new LogReporter(new Options.Logger() {
            @Override
            public void log(String msg, String... args) {
                System.out.println(msg + ", " + new Gson().toJson(args));
            }
        }, "test", null);

        reporter.corruption(100L, Status.Corruption("corruption"));
    }

    @Test
    public void test2() {
        Status status = Status.OK();
        LogReporter reporter = new LogReporter(new Options.Logger() {
            @Override
            public void log(String msg, String... args) {
                System.out.println(msg + ", " + new Gson().toJson(args));
            }
        }, "test", status);

        reporter.corruption(100L, Status.Corruption("corruption"));
        assertTrue(status.IsCorruption());
        assertEquals("corruption", status.getMessage());
    }
}