package com.farmerworking.leveldb.in.java.data.structure.log;

public enum RecordType {
    kUnknown(99),
    // Zero is reserved for preallocated files
    kZeroType(0),

    kFullType(1),

    // For fragments
    kFirstType(2),
    kMiddleType(3),
    kLastType(4),

    kEof(5),

    // Returned whenever we find an invalid physical record.
    // Currently there are two situations in which this happens:
    // * The record has an invalid CRC (ReadPhysicalRecord reports a drop)
    // * The record is a wrong-length record (No drop is reported)
    kBadRecord(6);

    // 32 * 1024, 32kb
    public static final int kBlockSize = 1024 * 32;

    // Header is checksum (4 bytes), length (2 bytes), type (1 byte).
    public static final int kHeaderSize = 4 + 2 + 1;

    // binary expression: 1111111111111111, length 16, since only 2 bytes to store payload length
    public static final int kMaxPayloadLength = 65535;

    private char value;

    RecordType(int value) {
        this.value = (char) value;
    }

    public char getValue() {
        return value;
    }

    public static RecordType valueOf(char type) {
        if (type == kZeroType.value) {
            return kZeroType;
        } else if (type == kFullType.value) {
            return kFullType;
        } else if (type == kFirstType.value) {
            return kFirstType;
        } else if (type == kMiddleType.value) {
            return kMiddleType;
        } else if (type == kLastType.value) {
            return kLastType;
        } else if (type == kEof.value) {
            return kEof;
        } else if (type == kBadRecord.value) {
            return kBadRecord;
        } else {
            return kUnknown;
        }
    }
}
