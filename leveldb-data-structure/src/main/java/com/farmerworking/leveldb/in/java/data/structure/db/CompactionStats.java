package com.farmerworking.leveldb.in.java.data.structure.db;

import lombok.Data;

import java.util.Objects;

// Per level compaction stats.  stats_[level] stores the stats for
// compactions that produced data for the specified "level".
@Data
public class CompactionStats {
    private long micros = 0;
    private long bytesRead = 0;
    private long bytesWritten = 0;

    public CompactionStats() {
    }

    public CompactionStats(CompactionStats stat) {
        this.micros = stat.micros;
        this.bytesRead = stat.bytesRead;
        this.bytesWritten = stat.bytesWritten;
    }

    public void add(CompactionStats stats) {
        this.micros += stats.micros;
        this.bytesRead += stats.bytesRead;
        this.bytesWritten += stats.bytesWritten;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CompactionStats that = (CompactionStats) o;
        return micros == that.micros &&
                bytesRead == that.bytesRead &&
                bytesWritten == that.bytesWritten;
    }

    @Override
    public int hashCode() {
        return Objects.hash(micros, bytesRead, bytesWritten);
    }
}
