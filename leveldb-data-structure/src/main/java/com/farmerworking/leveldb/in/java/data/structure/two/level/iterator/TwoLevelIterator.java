package com.farmerworking.leveldb.in.java.data.structure.two.level.iterator;

import com.farmerworking.leveldb.in.java.api.Iterator;
import com.farmerworking.leveldb.in.java.api.ReadOptions;
import com.farmerworking.leveldb.in.java.api.Status;

// A two-level iterator contains an index iterator whose values point
// to a sequence of blocks where each block is itself a sequence of
// key,value pairs.  The returned two-level iterator yields the
// concatenation of all key/value pairs in the sequence of blocks.
// Takes ownership of "index_iter" and will delete it when no longer needed.
//
// Uses a supplied function to convert an index_iter value into
// an iterator over the contents of the corresponding block.
public class TwoLevelIterator implements Iterator<String, String> {
    private Iterator<String, String> indexIterator;
    private Iterator<String, String> dataIterator;
    private String dataBlockHandleValue;
    private IndexTransfer indexTransfer;
    private ReadOptions readOptions;
    private Status status;

    public TwoLevelIterator(Iterator<String, String> indexIter, ReadOptions readOptions, IndexTransfer transfer) {
        this.indexIterator = indexIter;
        this.readOptions = readOptions;
        this.indexTransfer = transfer;
        this.dataIterator = null;
        this.dataBlockHandleValue = null;
        this.status = Status.OK();
    }

    @Override
    public boolean valid() {
        if (dataIterator != null) {
            return dataIterator.valid();
        } else {
            return false;
        }
    }

    @Override
    public void seekToFirst() {
        indexIterator.seekToFirst();
        initDataBlock();
        if (dataIterator != null) {
            dataIterator.seekToFirst();
        }
        skipEmptyDataBlocksForward();
    }

    @Override
    public void seekToLast() {
        indexIterator.seekToLast();
        initDataBlock();
        if (dataIterator != null) {
            dataIterator.seekToLast();
        }
        skipEmptyDataBlocksBackward();
    }

    @Override
    public void seek(String target) {
        indexIterator.seek(target);
        initDataBlock();
        if (dataIterator != null) {
            dataIterator.seek(target);
        }
        skipEmptyDataBlocksForward();
    }

    @Override
    public void next() {
        assert valid();
        dataIterator.next();
        skipEmptyDataBlocksForward();
    }

    @Override
    public void prev() {
        assert valid();
        dataIterator.prev();
        skipEmptyDataBlocksBackward();
    }

    @Override
    public String key() {
        assert valid();
        return dataIterator.key();
    }

    @Override
    public String value() {
        assert valid();
        return dataIterator.value();
    }

    @Override
    public Status status() {
        if (indexIterator.status().isNotOk()) {
            return indexIterator.status();
        } else if (dataIterator != null && dataIterator.status().isNotOk()) {
            return dataIterator.status();
        } else {
            return status;
        }
    }

    private void initDataBlock() {
        if (indexIterator.valid()) {
            String handleValue = indexIterator.value();
            if (dataIterator != null && dataBlockHandleValue.equals(handleValue)) {
                // dataIterator is already constructed with this iterator, so no need to change anything
            } else {
                Iterator<String, String> iter = indexTransfer.transfer(readOptions, handleValue);
                dataBlockHandleValue = handleValue;
                setDataIterator(iter);
            }

        } else {
            setDataIterator(null);
        }
    }

    private void skipEmptyDataBlocksForward() {
        while(dataIterator == null || !dataIterator.valid()) {
            // Move to next block
            if (!indexIterator.valid()) {
                setDataIterator(null);
                return;
            }
            indexIterator.next();
            initDataBlock();
            if (dataIterator != null) {
                dataIterator.seekToFirst();
            }
        }
    }

    private void skipEmptyDataBlocksBackward() {
        while(dataIterator == null || !dataIterator.valid()) {
            // Move to next block
            if (!indexIterator.valid()) {
                setDataIterator(null);
                return;
            }
            indexIterator.prev();
            initDataBlock();
            if (dataIterator != null) {
                dataIterator.seekToLast();
            }
        }
    }

    private void setDataIterator(Iterator<String, String> dataIterator) {
        if (this.dataIterator != null && this.status.isOk() && this.dataIterator.status().isNotOk()) {
            this.status = this.dataIterator.status();
        }
        this.dataIterator = dataIterator;
    }
}
