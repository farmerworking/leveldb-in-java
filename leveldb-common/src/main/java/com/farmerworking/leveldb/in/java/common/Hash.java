package com.farmerworking.leveldb.in.java.common;

public class Hash implements IHash{
    public int hash(char[] data, int seed) {
        // Similar to murmur hash
        int m = 0xc6a4a793;
        int r = 24;
        int h = seed ^ (data.length * m);


        // Pick up four bytes at a time
        int offset = 0;
        while (offset + ICoding.getInstance().getFixed32Length() <= data.length) {
            int w = ICoding.getInstance().decodeFixed32(data, offset);
            offset += ICoding.getInstance().getFixed32Length();
            h += w;
            h *= m;
            h ^= (h >>> 16);
        }

        // Pick up remaining bytes
        int left = data.length - offset;
        while(left > 0) {
            left--;

            h += data[offset + left] << (8 * left);
            if (left == 0) {
                h *= m;
                h ^= (h >>> r);
            }
        }
        return h;
    }
}
