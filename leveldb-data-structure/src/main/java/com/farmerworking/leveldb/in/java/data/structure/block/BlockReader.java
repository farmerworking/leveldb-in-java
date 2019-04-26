package com.farmerworking.leveldb.in.java.data.structure.block;

import com.farmerworking.leveldb.in.java.api.Comparator;
import com.farmerworking.leveldb.in.java.api.Iterator;
import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.common.ICoding;

public class BlockReader implements IBlockReader {
    private final char[] data;
    private int restartOffset;
    private boolean error = false;
    private Integer numRestarts;

    public BlockReader(String blockContent) {
        this.data = blockContent.toCharArray();

        int integerBytesSize = ICoding.getInstance().getFixed32Length();
        if (data.length < integerBytesSize) {
            error = true;
        } else {
            int maxRestartsAllowed = (data.length - integerBytesSize) / integerBytesSize;
            if (getNumRestarts() > maxRestartsAllowed) {
                error = true; //  The size is too small for numRestarts()
            } else {
                restartOffset = data.length - (1 + getNumRestarts()) * integerBytesSize;
            }
        }
    }

    private int getNumRestarts() {
        if (numRestarts == null) {
            assert this.data.length >= 4;
            numRestarts = ICoding.getInstance().decodeFixed32(data, data.length - 4);
        }

        return numRestarts;
    }

    @Override
    public Iterator<String, String> iterator(Comparator comparator) {
        if (error) {
            return new EmptyIterator(Status.Corruption("bad block contents"));
        }

        if (getNumRestarts() == 0) {
            return new EmptyIterator();
        } else {
            return new BlockIterator(comparator, data, restartOffset, getNumRestarts());
        }
    }
}
