package com.farmerworking.leveldb.in.java.data.structure.log;

import com.farmerworking.leveldb.in.java.common.Status;
import com.farmerworking.leveldb.in.java.common.ICRC32C;
import com.farmerworking.leveldb.in.java.common.ICoding;
import com.farmerworking.leveldb.in.java.file.WritableFile;

import java.nio.charset.StandardCharsets;

public class LogWriter implements ILogWriter {
    // used for padding
    private static char[] paddingBuffer = new char[RecordType.kHeaderSize - 1];
    static {
        for (int i = 0; i < RecordType.kHeaderSize - 1; i++) {
            paddingBuffer[i] = '\0';
        }
    }

    // Current offset in block
    private int blockOffset;
    private WritableFile dest;
    private char[] header = new char[RecordType.kHeaderSize];

    // Create a writer that will append data to "dest".
    // "dest" must be initially empty.
    public LogWriter(WritableFile dest) {
        this.dest = dest;
        this.blockOffset = 0;
    }

    // Create a writer that will append data to "dest".
    // "dest" must have initial length "dest_length".
    public LogWriter(WritableFile dest, long destLength) {
        this.dest = dest;
        this.blockOffset = (int) (destLength % RecordType.kBlockSize);
    }

    public Status addRecord(String data) {
        // Fragment the record if necessary and emit it.
        // Note: if data is empty, we still want to iterate once to emit a single zero-length record
        Status status;
        boolean begin = true;
        do {
            int leftOver = RecordType.kBlockSize - blockOffset;
            assert leftOver >= 0;
            if (leftOver < RecordType.kHeaderSize) {
                // Switch to a new block
                paddingIfNeed(leftOver);
                blockOffset = 0;
            }

            // Invariant: we never leave < kHeaderSize bytes in a block.
            assert RecordType.kBlockSize - blockOffset - RecordType.kHeaderSize >= 0;
            int availabe = RecordType.kBlockSize - blockOffset - RecordType.kHeaderSize;
            int fragmentLength = data.length() < availabe ? data.length() : availabe;
            RecordType type = getRecordType(begin, data.length() == fragmentLength);

            status = emitPhysicalRecord(type, data.substring(0, fragmentLength));
            data = data.substring(fragmentLength);
            begin = false;
        } while(status.isOk() && data.length() > 0);

        return status;
    }

    private RecordType getRecordType(boolean begin, boolean end) {
        if (begin && end) {
            return RecordType.kFullType;
        } else if (begin) {
            return RecordType.kFirstType;
        } else if (end) {
            return RecordType.kLastType;
        } else {
            return RecordType.kMiddleType;
        }
    }

    private void paddingIfNeed(int leftOver) {
        if (leftOver > 0) {
            dest.append(new String(paddingBuffer, 0, leftOver));
        }
    }

    private Status emitPhysicalRecord(RecordType type, String data) {
        assert data.length() <= RecordType.kMaxPayloadLength;
        assert blockOffset + RecordType.kHeaderSize + data.length() <= RecordType.kBlockSize;

        // checksum
        byte[] bytes = String.valueOf(type.getValue()).concat(data).getBytes(StandardCharsets.UTF_8);
        int crc = ICRC32C.getInstance().mask(ICRC32C.getInstance().value(bytes, 0, bytes.length));
        ICoding.getInstance().encodeFixed32(header, 0, crc);

        // length
        header[4] = (char) (data.length() & 0xff);
        header[5] = (char) (data.length() >> 8);

        // type
        header[6] = type.getValue();

        Status status = dest.append(new String(header));
        if (status.isOk()) {
            status = dest.append(data);

            if (status.isOk()) {
                // flush memory to disk to survive program being killed
                status = dest.flush();
            }
        }

        blockOffset = blockOffset + RecordType.kHeaderSize + data.length();
        return status;
    }
}
