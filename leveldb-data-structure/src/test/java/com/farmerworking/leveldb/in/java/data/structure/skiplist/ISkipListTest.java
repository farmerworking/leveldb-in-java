package com.farmerworking.leveldb.in.java.data.structure.skiplist;

import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public abstract class ISkipListTest {
    protected abstract ISkipList getImpl(Comparator comparator);

    class TmpComparator implements Comparator<Long> {
        @Override
        public int compare(Long a, Long b) {
            if (a < b) {
                return -1;
            } else if (a > b) {
                return +1;
            } else {
                return 0;
            }
        }
    }

    @Test
    public void testEmpty() throws Exception {
        ISkipList<Long> list = getImpl(new TmpComparator());
        assertTrue(!list.contains(10L));

        ISkipListIterator<Long> skipListIterator = list.iterator();
        assertTrue(!skipListIterator.valid());
        skipListIterator.seekToFirst();
        assertTrue(!skipListIterator.valid());
        skipListIterator.seek(100L);
        assertTrue(!skipListIterator.valid());
        skipListIterator.seekToLast();
        assertTrue(!skipListIterator.valid());
    }

    @Test
    public void testInsertAndLookup() throws Exception {
        int N = 2000;
        int R = 5000;
        SortedSet<Long> keys = new TreeSet<>();

        Random random = new Random();
        ISkipList<Long> list = getImpl(new TmpComparator());
        for (int i = 0; i < N; i++) {
            Long key = Long.valueOf(Math.abs(random.nextInt()) % R);

            if (keys.add(key)) {
                list.insert(key);
            }
        }

        for (int i = 0; i < R; i++) {
            if (list.contains((long)i)) {
                assertTrue(keys.contains((long)i));
            } else {
                assertFalse(keys.contains((long)i));
            }
        }

        // Simple skipListIterator tests
        ISkipListIterator<Long> skipListIterator = list.iterator();
        assertTrue(!skipListIterator.valid());

        skipListIterator.seek(0L);
        assertTrue(skipListIterator.valid());
        assertEquals(keys.first(), skipListIterator.key());

        skipListIterator.seekToFirst();
        assertTrue(skipListIterator.valid());
        assertEquals(keys.first(), skipListIterator.key());

        skipListIterator.seekToLast();
        assertTrue(skipListIterator.valid());
        assertEquals(keys.last(), skipListIterator.key());

        // Forward iteration test
        for (int i = 0; i < R; i++) {
            ISkipListIterator<Long> iter = list.iterator();
            iter.seek((long)i);

            // Compare against model skipListIterator
            java.util.Iterator<Long> iterator1 = (keys.tailSet((long)i)).iterator();
            for (int j = 0; j < 3; j++) {
                if (iterator1.hasNext()) {
                    assertTrue(iter.valid());
                    assertEquals(iterator1.next(), iter.key());
                    iter.next();
                } else {
                    assertTrue(!iter.valid());
                    break;
                }
            }
        }

        // Backward iteration test
        ISkipListIterator<Long> iter = list.iterator();
        iter.seekToLast();

        // Compare against model skipListIterator
        SortedSet<Long> reverseKeys = new TreeSet<>(Collections.reverseOrder());
        reverseKeys.addAll(keys);
        java.util.Iterator<Long> iterator1 = reverseKeys.iterator();
        while(iterator1.hasNext()) {
            assertTrue(iter.valid());
            assertEquals(iterator1.next(), iter.key());
            iter.prev();
        }
        assertTrue(!iter.valid());
    }
}