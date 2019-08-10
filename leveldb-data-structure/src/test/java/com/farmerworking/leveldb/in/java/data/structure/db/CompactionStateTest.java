package com.farmerworking.leveldb.in.java.data.structure.db;

import com.farmerworking.leveldb.in.java.data.structure.version.Compaction;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class CompactionStateTest {
    @Test
    public void testInit() {
        Compaction compaction = mock(Compaction.class);
        CompactionState compactionState = new CompactionState(compaction);

        assertEquals(0, compactionState.getTotalBytes());
        assertNull(compactionState.getBuilder());
        assertNull(compactionState.getOutfile());
        assertTrue(compactionState.getOutputs().isEmpty());
        assertNull(compactionState.getSmallestSnapshot());
        assertSame(compaction, compactionState.getCompaction());
    }

    @Test
    public void testLastOutput() {
        CompactionState compactionState = new CompactionState(null);

        Output out1 = mock(Output.class);
        Output out2 = mock(Output.class);

        compactionState.add(out1);
        compactionState.add(out2);

        assertSame(out2, compactionState.currentOutput());
    }
}