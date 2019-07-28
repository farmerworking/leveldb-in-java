package com.farmerworking.leveldb.in.java.data.structure.db;

import org.junit.Test;

import static org.junit.Assert.*;

public class CompactionStatsTest {
    @Test
    public void test() {
        CompactionStats stats = new CompactionStats();

        assertEquals(0, stats.getBytesWritten());
        assertEquals(0, stats.getBytesRead());
        assertEquals(0, stats.getMicros());

        CompactionStats stats2 = new CompactionStats(stats);
        assertNotSame(stats, stats2);
        assertEquals(stats, stats2);

        stats2.setBytesWritten(1);
        stats2.setBytesRead(1);
        stats2.setMicros(1);
        stats.add(stats2);

        assertEquals(1, stats.getBytesWritten());
        assertEquals(1, stats.getBytesRead());
        assertEquals(1, stats.getMicros());
    }
}