package com.farmerworking.leveldb.in.java.data.structure.log;

import com.farmerworking.leveldb.in.java.common.Status;
import com.farmerworking.leveldb.in.java.data.structure.ICRC32C;
import com.farmerworking.leveldb.in.java.data.structure.ICoding;
import com.farmerworking.leveldb.in.java.file.SequentialFile;
import javafx.util.Pair;

import java.nio.charset.StandardCharsets;

public class LogReader implements ILogReader {
    private static ICoding coding = ICoding.getDefaultImpl();
    private static ICRC32C crc32c = ICRC32C.getDefaultImpl();

    private SequentialFile file;
    private ILogReporter reporter;
    private boolean checksum;
    private LogBuffer buffer;
    // Last Read() indicated EOF by returning < kBlockSize
    private boolean eof;

    // Offset of the last record returned by ReadRecord.
    private long lastRecordOffset;
    // Offset of the first location past the end of buffer.
    private long endOfBufferOffset;
    // Offset at which to start looking for the first record to return
    private long initialOffset;

    public LogReader(SequentialFile file, ILogReporter reporter, boolean checksum, long initialOffset) {
        this.file = file;
        this.reporter = reporter;
        this.checksum = checksum;
        this.initialOffset = initialOffset;

        this.eof = false;
        this.lastRecordOffset = 0;
        this.endOfBufferOffset = 0;
        this.buffer = new LogBuffer();
    }

    // Returns the physical offset of the last record returned by ReadRecord.
    //
    // Undefined before the first call to ReadRecord.
    public long lastRecordOffset() {
        return lastRecordOffset;
    }

    // Read the next record.  Returns true if read
    // successfully, false if we hit end of the input.
    public Pair<Boolean, String> readRecord() {
        if (lastRecordOffset < initialOffset) {
            Pair<Boolean, Pair<RecordType, String>> pair = skipToValidRecordAfterInitialOffset();
            Boolean skipSuccess = pair.getKey();
            if (!skipSuccess) {
                return new Pair<>(false, "");
            }
            return LogicalRecord(pair.getValue());
        } else {
            return LogicalRecord(readPhysicalRecord());
        }
    }


    // LL(1) for log logical record
    //
    // LogicalRecord    = kFullRecord                  |
    //                    kEofRecord                   |
    //                    kFirstRecord FragmentRecord  |
    //                    kBadRecord LogicalRecord     |
    //                    kMiddleRecord LogicalRecord  |
    //                    kLastRecord LogicalRecord    |
    //                    kUnknown LogicalRecord
    //
    // FragmentRecord   = kLastRecord                  |
    //                    kEofRecord                   |
    //                    kMiddleRecord FragmentRecord |
    //                    kBadRecord LogicalRecord     |
    //                    kFirstRecord LogicalRecord   |
    //                    kFullRecord LogicalRecord    |
    //                    kUnknown LogicalRecord
    //
    private Pair<Boolean, String> LogicalRecord(Pair<RecordType, String> physicalRecord) {
        long recordOffset = endOfBufferOffset - buffer.remain() - RecordType.kHeaderSize - physicalRecord.getValue()
                .length();
        switch (physicalRecord.getKey()) {
            case kFullType:
                lastRecordOffset = recordOffset;
                return new Pair<>(true, physicalRecord.getValue());
            case kEof:
                return new Pair<>(false, "");
            case kFirstType:
                return FragmentRecord(recordOffset, physicalRecord.getValue(), readPhysicalRecord());
            case kBadRecord: // skip this one
                return LogicalRecord(readPhysicalRecord());
            case kMiddleType: case kLastType:
                reportCorruption(physicalRecord.getValue().length(), "missing start of fragmented record(1)");
                return LogicalRecord(readPhysicalRecord());
            default: // kUnknown, kZeroType
                reportCorruption(physicalRecord.getValue().length(),
                        String.format("unknown record type %s", physicalRecord.getKey()));
                return LogicalRecord(readPhysicalRecord());
        }
    }

    private Pair<Boolean, String> FragmentRecord(long recordOffset, String prefix,
                                                 Pair<RecordType, String> physicalRecord) {
        switch (physicalRecord.getKey()) {
            case kLastType:
                lastRecordOffset = recordOffset;
                return new Pair<>(true, prefix + physicalRecord.getValue());
            case kEof:
                return new Pair<>(false, "");
            case kMiddleType:
                return FragmentRecord(recordOffset, prefix + physicalRecord.getValue(), readPhysicalRecord());
            case kBadRecord:
                reportCorruption(prefix.length(), "error in middle of record");
                return LogicalRecord(readPhysicalRecord());
            case kFullType: case kFirstType:
                if (!prefix.isEmpty()) {
                    reportCorruption(prefix.length(), "partial record without end(1)");
                }
                return LogicalRecord(physicalRecord);
            default: // kUnknown, kZeroType
                reportCorruption(physicalRecord.getValue().length() + prefix.length(),
                        String.format("unknown record type %s", physicalRecord.getKey()));
                return LogicalRecord(readPhysicalRecord());
        }
    }

    private Pair<Boolean, Pair<RecordType, String>> skipToValidRecordAfterInitialOffset() {
        boolean blockSkip = skipToInitialBlock();
        if (blockSkip) {
            while(true) {
                Pair<RecordType, String> physicalRecord = readPhysicalRecord();
                switch (physicalRecord.getKey()) {
                    case kFirstType: case kFullType: case kEof:
                        return new Pair<>(true, physicalRecord);
                    default:
                        continue; // skip invalid record like bad record, middle record, last record
                }
            }
        } else {
            return new Pair<>(false, null);
        }
    }

    // Skips all blocks that are completely before "initial_offset_".
    //
    // Returns true on success. Handles reporting.
    private boolean skipToInitialBlock() {
        int offsetInBlock = (int)(initialOffset % RecordType.kBlockSize);
        long blockStartLocation = initialOffset - offsetInBlock;

        // Don't search a block if we'd be in the trailer
        if (offsetInBlock > RecordType.kBlockSize - RecordType.kHeaderSize) {
            blockStartLocation += RecordType.kBlockSize;
        }

        this.endOfBufferOffset = blockStartLocation;

        if (blockStartLocation > 0) {
            Status status = file.skip(blockStartLocation);
            if (!status.isOk()) {
                reportDrop(blockStartLocation, status);
                return false;
            } else {
                return true;
            }
        } else {
            return true;
        }
    }

    // Return type, or one of the preceding special values
    private Pair<RecordType, String> readPhysicalRecord() {
        while(true) {
            if (buffer.remain() < RecordType.kHeaderSize) {
                if (eof) {
                    // if buffer is non-empty, we have a truncated header at the
                    // end of the file, which can be caused by the writer crashing in the
                    // middle of writing the header. Instead of considering this an error,
                    // just report EOF.
                    // Since the write request is interrupted and not returned, user should not treat it as an
                    // success write
                    buffer.clear();
                    return new Pair<>(RecordType.kEof, "");
                } else {
                    buffer.clear(); // Last read was a full read, so this is a trailer to skip
                    Status status = readBlock();

                    if (status.isNotOk()) {
                        return new Pair<>(RecordType.kEof, "");
                    }

                    continue;
                }
            }

            // Skip physical record that started before initial_offset_
            boolean skipRecordsBeforeInitialOffset = endOfBufferOffset - buffer.remain() < initialOffset;

            // Parse the header
            char[] header = buffer.getChars(0, RecordType.kHeaderSize);
            Pair<Integer, RecordType> parseResult = parseHeader(header);
            int length = parseResult.getKey();
            RecordType type = parseResult.getValue();
            if (RecordType.kHeaderSize + length > buffer.remain()) {
                int dropSize = buffer.remain();
                buffer.clear();

                if (eof) {
                    // If the end of the file has been reached without reading |length| bytes
                    // of payload, assume the writer died in the middle of writing the record.
                    // Don't report a corruption.
                    return new Pair<>(RecordType.kEof, "");
                } else {
                    reportCorruption(dropSize, "bad record length");
                    return new Pair<>(RecordType.kBadRecord, "");
                }
            }

            if (type == RecordType.kZeroType && length == 0) {
                // Skip zero length record without reporting any drops since
                // such records are produced by the mmap based writing code in
                // env_posix.cc that preallocates file regions.
                buffer.clear();
                return new Pair<>(RecordType.kEof, "");
            }

            // check crc
            char[] payload = buffer.getChars(RecordType.kHeaderSize - 1, length + 1);
            if (checksum) {
                int expectedCrc = crc32c.unmask(coding.decodeFixed32(header, 0));
                byte[] bytes = new String(payload).getBytes(StandardCharsets.UTF_8);
                int actualCrc = crc32c.value(bytes, 0, bytes.length);
                if (actualCrc != expectedCrc) {
                    // Drop the rest of the buffer since "length" itself may have
                    // been corrupted and if we trust it, we could find some
                    // fragment of a real log record that just happens to look
                    // like a valid log record.
                    int dropSize = buffer.remain();
                    buffer.clear();
                    reportCorruption(dropSize, "checksum mismatch");
                    return new Pair<>(RecordType.kBadRecord, "");
                }
            }

            buffer.seek(RecordType.kHeaderSize + length);

            if (skipRecordsBeforeInitialOffset) {
                continue;
            }

            return new Pair<>(type, new String(payload, 1, length));
        }
    }

    private Pair<Integer, RecordType> parseHeader(char[] header) {
        int a = header[4] & 0xff;
        int b = header[5] & 0xff;
        int length = a | (b << 8);
        char type = header[6];
        return new Pair<>(length, RecordType.valueOf(type));
    }

    private Status readBlock() {
        Pair<Status, String> readResult = file.read(RecordType.kBlockSize);
        if (readResult.getKey().isOk()) {
            buffer.set(readResult.getValue());
            endOfBufferOffset = endOfBufferOffset + buffer.remain();

            if (buffer.remain() < RecordType.kBlockSize) {
                eof = true;
            }
        } else {
            eof = true;
            reportDrop(RecordType.kBlockSize, readResult.getKey());
        }
        return readResult.getKey();
    }

    // Reports dropped bytes to the reporter.
    // buffer must be updated to remove the dropped bytes prior to invocation.
    private void reportCorruption(long bytes, String reason) {
        reportDrop(bytes, Status.Corruption(reason));
    }

    private void reportDrop(long bytes, Status reason) {
        if (reporter != null) {
            reporter.corruption(bytes, reason);
        }
    }
}
