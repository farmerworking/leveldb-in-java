package com.farmerworking.leveldb.in.java.data.structure.memory;

import com.farmerworking.leveldb.in.java.api.Comparator;

public class InternalKeyComparator {
    final Comparator userComparator;

    public Comparator getUserComparator() {
        return userComparator;
    }

    public InternalKeyComparator(Comparator userComparator) {
        this.userComparator = userComparator;
    }

    // Order by:
    //    increasing user key (according to user-supplied comparator)
    //    decreasing sequence number
    public int compare(InternalKey a, InternalKey b) {
        int result = userComparator.compare(a.userKeyChar, b.userKeyChar);
        if (result == 0) {
            long aSequence = a.sequence;
            long bSequence = b.sequence;

            if (aSequence > bSequence) {
                return -1;
            } else if (aSequence < bSequence) {
                return 1;
            } else {
                return 0;
            }
        } else {
            return result;
        }
    }

    public InternalKey findShortestSeparator(InternalKey a, InternalKey b) {
        // Attempt to shorten the user portion of the key
        char[] shortSeparator = userComparator.findShortestSeparator(a.userKeyChar, b.userKeyChar);
        if (shortSeparator.length < a.userKeyChar.length &&
                userComparator.compare(a.userKeyChar, shortSeparator) < 0) {
            // User key has become shorter physically, but larger logically.
            // Tack on the earliest possible number to the shortened user key.
            InternalKey result = new InternalKey(shortSeparator, InternalKey.kMaxSequenceNumber, ValueType.kTypeValue);
            assert this.compare(a, result) < 0;
            assert this.compare(result, b) < 0;

            return result;
        } else {
            return a;
        }
    }

    public InternalKey findShortSuccessor(InternalKey a) {
        char[] shortSuccessor = userComparator.findShortSuccessor(a.userKeyChar);
        if (shortSuccessor.length < a.userKeyChar.length && userComparator.compare(a.userKeyChar, shortSuccessor) < 0) {
            // User key has become shorter physically, but larger logically.
            // Tack on the earliest possible number to the shortened user key.
            InternalKey result = new InternalKey(shortSuccessor, InternalKey.kMaxSequenceNumber, ValueType.kTypeValue);
            assert this.compare(a, result) <= 0;

            return result;
        } else {
            return a;
        }
    }
}
