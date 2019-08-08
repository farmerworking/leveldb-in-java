package com.farmerworking.leveldb.in.java.data.structure.db;

import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKey;
import com.farmerworking.leveldb.in.java.data.structure.table.TableBuilder;
import com.farmerworking.leveldb.in.java.data.structure.version.Compaction;
import com.farmerworking.leveldb.in.java.file.WritableFile;

import java.util.Vector;

public class CompactionState {
    private Compaction compaction;

    // Sequence numbers < smallest_snapshot are not significant since we
    // will never have to service a snapshot below smallest_snapshot.
    // Therefore if we have seen a sequence number S <= smallest_snapshot,
    // we can drop all entries for the same key with sequence numbers < S.
    private long smallestSnapshot;

    public static class Output {
        long number;
        long fileSize;
        InternalKey smallest;
        InternalKey largest;

        public Output(long number) {
            this.number = number;
        }
    }

    Vector<Output> outputs;

    // State kept for output being generated
    WritableFile outfile;
    TableBuilder builder;

    long totalBytes;

    Output currentOutput() {
        return outputs.get(outputs.size() - 1);
    }

    public CompactionState(Compaction compaction) {
        this.compaction = compaction;
        this.outfile = null;
        this.builder = null;
        this.totalBytes = 0;
        this.outputs = new Vector<>();
    }
}
