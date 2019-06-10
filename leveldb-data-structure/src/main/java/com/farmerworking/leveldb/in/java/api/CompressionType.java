package com.farmerworking.leveldb.in.java.api;


// DB contents are stored in a set of blocks, each of which holds a
// sequence of key,value pairs.  Each block may be compressed before
// being stored in a file.  The following enum describes which
// compression method (if any) is used to compress a block.
public enum CompressionType {
    // NOTE: do not change the values of existing entries, as these are
    // part of the persistent format on disk.
    kNoCompression(0x0),
    kSnappyCompression(0x1);

    private int value;

    CompressionType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static CompressionType valueOf(int value) {
        if (value == kNoCompression.getValue()) {
            return kNoCompression;
        } else if (value == kSnappyCompression.getValue()) {
            return kSnappyCompression;
        } else {
            return null;
        }
    }
}
