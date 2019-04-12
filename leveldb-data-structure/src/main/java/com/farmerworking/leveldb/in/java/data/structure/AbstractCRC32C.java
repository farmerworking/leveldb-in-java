package com.farmerworking.leveldb.in.java.data.structure;

public abstract class AbstractCRC32C implements ICRC32C {
    protected static int kMaskDelta = 0xa282ead8;

    @Override
    public int mask(Integer crc) {
        return ((crc >>> 15) | (crc << 17)) + kMaskDelta;
    }

    @Override
    public int unmask(Integer maskedCrc) {
        int rot = maskedCrc - kMaskDelta;
        return ((rot >>> 17) | (rot << 15));
    }
}
