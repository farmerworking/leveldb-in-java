package com.farmerworking.leveldb.in.java.data.structure;

/**
 * note: treat result returned and parameter passed as unsigned integer
 */
public interface ICRC32C {
    int value(byte[] b, int off, int len);

    int mask(Integer crc);

    int unmask(Integer maskedCrc);

    int extend(int originCrc, byte[] b, int off, int len);

    public static ICRC32C getDefaultImpl() {
        return new JDK11CRC32C();
    }
}

