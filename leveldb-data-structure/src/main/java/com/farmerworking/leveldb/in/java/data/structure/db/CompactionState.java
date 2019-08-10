package com.farmerworking.leveldb.in.java.data.structure.db;

import com.farmerworking.leveldb.in.java.data.structure.table.TableBuilder;
import com.farmerworking.leveldb.in.java.data.structure.version.Compaction;
import com.farmerworking.leveldb.in.java.file.WritableFile;
import lombok.Data;

import java.util.Vector;

@Data
public class CompactionState {
    private Compaction compaction;

    // Sequence numbers < smallest_snapshot are not significant since we
    // will never have to service a snapshot below smallest_snapshot.
    // Therefore if we have seen a sequence number S <= smallest_snapshot,
    // we can drop all entries for the same key with sequence numbers < S.
    private Long smallestSnapshot;

    private Vector<Output> outputs;

    // State kept for output being generated
    private WritableFile outfile;
    private TableBuilder builder;

    private long totalBytes;

    public CompactionState(Compaction compaction) {
        this.compaction = compaction;
        this.outfile = null;
        this.builder = null;
        this.totalBytes = 0;
        this.outputs = new Vector<>();
    }

    Output currentOutput() {
        if (outputs.isEmpty()) {
            return null;
        }
        return outputs.get(outputs.size() - 1);
    }

    void add(Output output) {
        outputs.add(output);
    }

    void addBytes(long bytes) {
        this.totalBytes += bytes;
    }
}
