package com.farmerworking.leveldb.in.java.data.structure.memory;

import com.farmerworking.leveldb.in.java.api.Comparator;
import com.farmerworking.leveldb.in.java.common.ICoding;

public class InternalKeyComparator implements Comparator{
    private static ICoding coding = ICoding.getInstance();

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
    public int compare(char[] a, char[] b) {
        int result = userComparator.compare(InternalKey.extractUserKey(a), InternalKey.extractUserKey(b));
        if (result == 0) {
            long aSequence = coding.decodeFixed64(a, a.length - coding.getFixed64Length());
            long bSequence = coding.decodeFixed64(b, b.length - coding.getFixed64Length());

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

    public int compare(InternalKey a, InternalKey b) {
        return compare(a.getRep(), b.getRep());
    }

    @Override
    public String name() {
        return "leveldb.InternalKeyComparator";
    }

    @Override
    public char[] findShortestSeparator(char[] a, char[] b) {
        // Attempt to shorten the user portion of the key
        char[] aUserKey = InternalKey.extractUserKey(a);
        char[] bUserKey = InternalKey.extractUserKey(b);

        char[] shortSeparator = userComparator.findShortestSeparator(aUserKey, bUserKey);
        if (shortSeparator.length < a.length &&
                userComparator.compare(aUserKey, shortSeparator) < 0) {
            // User key has become shorter physically, but larger logically.
            // Tack on the earliest possible number to the shortened user key.
            char[] result = finalize(shortSeparator);
            assert this.compare(a, result) < 0;
            assert this.compare(result, b) < 0;

            return result;
        } else {
            return a;
        }
    }

    public InternalKey findShortestSeparator(InternalKey a, InternalKey b) {
        InternalKey result = new InternalKey();
        result.decodeFrom(new String(findShortestSeparator(a.getRep(), b.getRep())));
        return result;
    }

    @Override
    public char[] findShortSuccessor(char[] a) {
        char[] userKey = InternalKey.extractUserKey(a);

        char[] shortSuccessor = userComparator.findShortSuccessor(userKey);
        if (shortSuccessor.length < userKey.length && userComparator.compare(userKey, shortSuccessor) < 0) {
            // User key has become shorter physically, but larger logically.
            // Tack on the earliest possible number to the shortened user key.
            char[] result = finalize(shortSuccessor);
            assert this.compare(a, result) <= 0;

            return result;
        } else {
            return a;
        }
    }

    private char[] finalize(char[] chars) {
        char[] result = new char[chars.length + coding.getFixed64Length()];
        System.arraycopy(chars, 0, result, 0, chars.length);
        coding.encodeFixed64(result, chars.length, InternalKey.packSequenceAndType(InternalKey.kMaxSequenceNumber, ValueType.kValueTypeForSeek));
        return result;
    }

    public InternalKey findShortSuccessor(InternalKey a) {
        InternalKey result = new InternalKey();
        result.decodeFrom(new String(findShortSuccessor(a.getRep())));
        return result;
    }
}
