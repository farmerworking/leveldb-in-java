package com.farmerworking.leveldb.in.java.common;

/**
 * note: treat result returned and parameter passed as unsigned integer
 */
public interface ICRC32C {
    ICRC32C instance = getDefaultImpl();

    int value(byte[] b, int off, int len);

    int mask(Integer crc);

    int unmask(Integer maskedCrc);

    int extend(int originCrc, byte[] b, int off, int len);

    static ICRC32C getDefaultImpl() {
        return new JDK11CRC32C();
    }

    static ICRC32C getInstance() {
        return instance;
    }
}

