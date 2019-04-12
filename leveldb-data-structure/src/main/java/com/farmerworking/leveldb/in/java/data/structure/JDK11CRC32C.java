package com.farmerworking.leveldb.in.java.data.structure;

public class JDK11CRC32C extends AbstractCRC32C {
    @Override
    public int value(byte[] b, int off, int len) {
        CRC32C crc32C = new CRC32C();
        crc32C.update(b, off, len);
        return (int)crc32C.getValue();
    }

    @Override
    public int extend(int originCrc, byte[] b, int off, int len) {
        CRC32C crc32C = new CRC32C(originCrc);
        crc32C.update(b, off, len);
        return (int)crc32C.getValue();
    }
}
