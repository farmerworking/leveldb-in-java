package com.farmerworking.leveldb.in.java.data.structure.version;

import com.farmerworking.leveldb.in.java.api.Comparator;
import com.farmerworking.leveldb.in.java.api.Iterator;
import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.data.structure.block.EmptyIterator;
import com.farmerworking.leveldb.in.java.data.structure.iterator.AbstractIterator;
import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKeyComparator;

import java.util.List;

public class MergingIterator extends AbstractIterator<String, String> {

    public static Iterator<String, String> newMergingIterator(
            Comparator comparator,
            List<Iterator<String, String>> iteratorList) {
        assert iteratorList != null;

        if (iteratorList.isEmpty()) {
            return new EmptyIterator();
        } else if (iteratorList.size() == 1) {
            return iteratorList.get(0);
        } else {
            return new MergingIterator(comparator, iteratorList);
        }
    }

    private Comparator comparator;
    private List<Iterator<String, String>> iteratorList;
    private Iterator<String, String> current;

    // true == kForward, false == kReverse
    private boolean forwardDirection;

    public MergingIterator(Comparator comparator, List<Iterator<String, String>> iteratorList) {
        this.comparator = comparator;
        this.iteratorList = iteratorList;
        this.current = null;
        this.forwardDirection = true;
    }

    @Override
    public boolean valid() {
        assert !this.closed;
        return current != null;
    }

    @Override
    public void seekToFirst() {
        assert !this.closed;
        for (Iterator<String, String> iter : iteratorList) {
            iter.seekToFirst();
        }

        findSmallest();
        forwardDirection = true;
    }

    @Override
    public void seekToLast() {
        assert !this.closed;
        for (Iterator<String, String> iter : iteratorList) {
            iter.seekToLast();
        }

        findLargest();
        forwardDirection = false;
    }

    @Override
    public void seek(String target) {
        assert !this.closed;
        for (Iterator<String, String> iter : iteratorList) {
            iter.seek(target);
        }
        findSmallest();
        forwardDirection = true;
    }

    @Override
    public void next() {
        assert valid();

        // Ensure that all children are positioned after key().
        // If we are moving in the forward direction, it is already
        // true for all of the non-current_ children since current_ is
        // the smallest child and key() == current_->key().  Otherwise,
        // we explicitly position the non-current_ children.
        if (!forwardDirection) {
            for(Iterator<String, String> iter : iteratorList) {
                if (iter != current) {
                    iter.seek(key());
                    if (iter.valid() && this.comparator.compare(key().toCharArray(), iter.key().toCharArray()) == 0) {
                        iter.next();
                    }
                }
            }
            forwardDirection = true;
        }

        current.next();
        findSmallest();
    }

    @Override
    public void prev() {
        assert valid();

        // Ensure that all children are positioned before key().
        // If we are moving in the reverse direction, it is already
        // true for all of the non-current_ children since current_ is
        // the largest child and key() == current_->key().  Otherwise,
        // we explicitly position the non-current_ children.
        if (forwardDirection) {
            for (Iterator<String, String> iter : iteratorList) {
                if (iter != current) {
                    iter.seek(key());
                    if (iter.valid()) {
                        // Child is at first entry >= key().  Step back one to be < key()
                        iter.prev();
                    } else {
                        // Child has no entries >= key().  Position at last entry.
                        iter.seekToLast();
                    }
                }
            }
            forwardDirection = false;
        }

        current.prev();
        findLargest();
    }

    @Override
    public String key() {
        assert valid();
        return current.key();
    }

    @Override
    public String value() {
        assert valid();
        return current.value();
    }

    @Override
    public Status status() {
        assert !this.closed;
        Status status = Status.OK();

        for(Iterator<String, String> iter : iteratorList) {
            status = iter.status();
            if (status.isNotOk()) {
                break;
            }
        }

        return status;
    }

    private void findSmallest() {
        Iterator<String, String> smallest = null;
        for (Iterator<String, String> iter : iteratorList) {
            if (iter.valid()) {
                if (smallest == null) {
                    smallest = iter;
                } else if (comparator.compare(iter.key().toCharArray(), smallest.key().toCharArray()) < 0) {
                    smallest = iter;
                }
            }
        }

        current = smallest;
    }

    private void findLargest() {
        Iterator<String, String> largest = null;
        for (int i = iteratorList.size() - 1; i >= 0; i--) {
            Iterator<String, String> iter = iteratorList.get(i);

            if (iter.valid()) {
                if (largest == null) {
                    largest = iter;
                } else if (comparator.compare(iter.key().toCharArray(), largest.key().toCharArray()) > 0) {
                    largest = iter;
                }
            }
        }

        current = largest;
    }
}
