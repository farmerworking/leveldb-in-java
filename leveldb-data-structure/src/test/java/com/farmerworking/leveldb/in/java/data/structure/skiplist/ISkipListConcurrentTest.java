package com.farmerworking.leveldb.in.java.data.structure.skiplist;

import javafx.util.Pair;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import static org.junit.Assert.*;

public abstract class ISkipListConcurrentTest extends ISkipListTest {
    // We want to make sure that with a single writer and multiple
    // concurrent readers (with no synchronization other than when a
    // reader's iterator is created), the reader always observes all the
    // data that was present in the skip list when the iterator was
    // constructor.  Because insertions are happening concurrently, we may
    // also observe new values that were inserted since the iterator was
    // constructed, but we should never miss any values that were present
    // at iterator construction time.
    //
    // We generate multi-part keys:
    //     <key,gen,hash>
    // where:
    //     key is in range [0..K-1]
    //     gen is a generation number for key
    //     hash is hash(key,gen)
    //
    // The insertion code picks a random key, sets gen to be 1 + the last
    // generation number inserted for that key, and sets hash to Hash(key,gen).
    //
    // At the beginning of a read, we snapshot the last inserted
    // generation number for each key.  We then iterate, including random
    // calls to Next() and Seek().  For every key we encounter, we
    // check that it is either expected given the initial snapshot or has
    // been concurrently added since the iterator started.
    class ConcurrentTest {
        class ConcurrentTestNode {
            public Integer key;
            public Integer generation;
            public Integer hash;

            public ConcurrentTestNode(Integer key, Integer generation) {
                this.key = key;
                this.generation = generation;
                this.hash = Arrays.hashCode(new Integer[]{key, generation});
            }
        }

        class ConcurrentTestComparator implements Comparator<ConcurrentTestNode> {
            @Override
            public int compare(ConcurrentTestNode o1, ConcurrentTestNode o2) {
                if (o2 == null) {
                    return 1;
                }

                if (o1.key > o2.key) {
                    return 1;
                } else if (o1.key < o2.key) {
                    return -1;
                } else {
                    if (o1.generation > o2.generation) {
                        return 1;
                    } else if (o1.generation < o2.generation) {
                        return -1;
                    } else {
                        return 0;
                    }
                }
            }
        }

        private static final int K = 4;

        private ConcurrentHashMap<Integer, Integer> current;
        private ISkipList<ConcurrentTestNode> skipList;

        public ConcurrentTest() {
            this.skipList = getImpl(new ConcurrentTestComparator());
            this.current = new ConcurrentHashMap<>();
            for (int i = 0; i < K; i++) {
                current.put(i, 0);
            }
        }

        // REQUIRES: External synchronization
        public void writeStep(Random random) {
            Integer k = Math.abs(random.nextInt()) % K;
            Integer g = current.get(k) + 1;
            skipList.insert(new ConcurrentTestNode(k, g));
            current.put(k, g);
        }

        public void readStep(Random random) {
            // Remember the initial committed state of the skiplist.
            ConcurrentHashMap<Integer, Integer> initialState = snapshot();

            ISkipListIterator<ConcurrentTestNode> iter = skipList.iterator();
            ConcurrentTestComparator comparator = new ConcurrentTestComparator();

            ConcurrentTestNode testNode = null;
            while(true) {
                Pair<ConcurrentTestNode, ConcurrentTestNode> pair = randomSeekOrNext(random, iter, comparator, testNode);
                testNode = pair.getKey();
                ConcurrentTestNode currentNode = pair.getValue();

                assertTrue(currentNode.toString(), isValidKey(currentNode)); // check read partial record
                assertTrue("should not go backwards", comparator.compare(testNode, currentNode) <= 0);

                // Verify that everything in [pos,current) was not present in
                // initial_state.
                while(comparator.compare(testNode, currentNode) < 0) {
                    assertTrue(testNode.key < K);

                    // for seek case, testNode.generation == 0
                    // for next case:
                    // 1. testNode.key == preTestNode.key && testNode.generation == preTestNode.generation + 1
                    // 2. currentNode == preCurrentNode.next()
                    // 3. preTestNode ==  preCurrentNode
                    //
                    // so, here, testNode.key = currentNode.key - 1, testNode.generation = preCurrentNode.generation + 1
                    // which should larger than snapshot
                    // The node with generation(preCurrentNode.generation + 1) is missing if assert failed
                    assertTrue(
                            testNode.generation == 0 // true for seek case
                                    || testNode.generation > initialState.get(testNode.key));

                    // Advance to next key in the valid key space
                    if (testNode.key < currentNode.key) {
                        testNode = makeKey(testNode.key + 1, 0);
                    } else {
                        testNode = makeKey(testNode.key, testNode.generation + 1);
                    }
                }

                if (!iter.valid()) {
                    break;
                }
            }
        }

        // for the first time, it will be a seek. After that random choose seek or next
        // for seek, return a random node, iter.key point to the least node larger or equal than return node
        // for next, return a node with previous node's key and generation + 1, iter.key point to the next node;
        private Pair<ConcurrentTestNode, ConcurrentTestNode> randomSeekOrNext(Random random,
                                                                              ISkipListIterator<ConcurrentTestNode> iter,
                                                                              ConcurrentTestComparator comparator,
                                                                              ConcurrentTestNode pos) {
            if (pos == null // first time
                    || Math.abs(random.nextInt()) % 2 > 0) {
                ConcurrentTestNode newTarget = randomTarget(random);
                if (comparator.compare(newTarget, pos) > 0) {
                    pos = newTarget;
                    iter.seek(newTarget);
                }
            } else {
                iter.next();
                pos = makeKey(pos.key, pos.generation + 1);
            }

            if (iter.valid()) {
                return new Pair(pos, iter.key());
            } else {
                return new Pair(pos, makeKey(K, 0));
            }
        }

        private ConcurrentHashMap<Integer, Integer> snapshot() {
            ConcurrentHashMap<Integer, Integer> initialState = new ConcurrentHashMap<>();
            for (int i = 0; i < K; i++) {
                initialState.put(i, current.get(i));
            }
            return initialState;
        }

        private boolean isValidKey(ConcurrentTestNode k) {
            return k.hash == Arrays.hashCode(new Integer[]{k.key, k.generation});
        }

        private ConcurrentTestNode randomTarget(Random random) {
            switch (Math.abs(random.nextInt()) % 10) {
                case 0:
                    // Seek to beginning
                    return makeKey(0, 0);
                case 1:
                    // Seek to end
                    return makeKey(K, 0);
                default:
                    // Seek to middle
                    return makeKey(Math.abs(random.nextInt()) % K, 0);
            }
        }

        private ConcurrentTestNode makeKey(Integer k, Integer g) {
            return new ConcurrentTestNode(k, g);
        }
    }

    class TestState {
        private volatile String state;
        public ConcurrentTest concurrentTest;
        public volatile boolean quit;

        public TestState() {
            this.state = "STARTING";
            this.concurrentTest = new ConcurrentTest();
            this.quit = false;
        }

        public synchronized void waitState(String state) throws InterruptedException {
            while(!this.state.equalsIgnoreCase(state)) {
                wait();
            }
        }

        public synchronized void changeState(String state) {
            this.state = state;
            notify();
        }
    }

    private synchronized void runConcurrent(int run) throws InterruptedException {
        final Random random = new Random(run * 100);
        int N = 1000;
        int kSize = 1000;
        for (int i = 0; i < N; i++) {
            if ((i % 100) == 0) {
                System.out.println(String.format("Run %d of %d", i, N));
            }
            final TestState testState = new TestState();

            (new Thread(new Runnable() {
                @Override
                public void run() {
                    testState.changeState("RUNNING");
                    while(!testState.quit) {
                        testState.concurrentTest.readStep(random);
                    }
                    testState.changeState("DONE");
                }
            })).start();
            testState.waitState("RUNNING");
            for (int j = 0; j < kSize; j++) {
                testState.concurrentTest.writeStep(random);
            }
            testState.quit = true;
            testState.waitState("DONE");
        }
    }

    @Test
    public void testConcurrentWithoutThreads() throws Exception {
        ConcurrentTest test = new ConcurrentTest();
        Random random = new Random();
        for (int i = 0; i < 10000; i++) {
            test.readStep(random);
        }
    }

    @Test
    public void testConcurrent1() throws InterruptedException {
        runConcurrent(1);
    }

    @Test
    public void testConcurrent2() throws InterruptedException {
        runConcurrent(2);
    }

    @Test
    public void testConcurrent3() throws InterruptedException {
        runConcurrent(3);
    }

    @Test
    public void testConcurrent4() throws InterruptedException {
        runConcurrent(4);
    }

    @Test
    public void testConcurrent5() throws InterruptedException {
        runConcurrent(5);
    }
}
