package com.farmerworking.leveldb.in.java.data.structure.iterator;

import com.farmerworking.leveldb.in.java.api.Iterator;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public abstract class AbstractIteratorTest {
    protected abstract Iterator getImpl();
    Iterator iter;

    @Before
    public void setUp() throws Exception {
        iter = getImpl();
    }

    @Test(expected = AssertionError.class)
    public void testValidAfterClose() {
        iter.close();
        iter.valid();
    }

    @Test(expected = AssertionError.class)
    public void testSeekToFirstAfterClose() {
        iter.close();
        iter.seekToFirst();
    }

    @Test(expected = AssertionError.class)
    public void testSeekToLastAfterClose() {
        iter.close();
        iter.seekToLast();
    }

    @Test(expected = AssertionError.class)
    public void testSeekAfterClose() {
        iter.close();
        iter.seek(null);
    }

    @Test(expected = AssertionError.class)
    public void testNextAfterClose() {
        iter.close();
        iter.next();
    }

    @Test(expected = AssertionError.class)
    public void testPrevAfterClose() {
        iter.close();
        iter.prev();
    }

    @Test(expected = AssertionError.class)
    public void testKeyAfterClose() {
        iter.close();
        iter.key();
    }

    @Test(expected = AssertionError.class)
    public void testValueAfterClose() {
        iter.close();
        iter.value();
    }

    @Test(expected = AssertionError.class)
    public void testStatusAfterClose() {
        iter.close();
        iter.status();
    }

    @Test(expected = AssertionError.class)
    public void testCloseAfterClose() {
        iter.close();
        iter.close();
    }

    @Test(expected = AssertionError.class)
    public void testRegisterCleanupAfterClose() {
        iter.close();
        iter.registerCleanup(null);
    }

    @Test(expected = AssertionError.class)
    public void testRegisterCleanupNull() {
        iter.registerCleanup(null);
    }

    @Test
    public void testRegisterAndCleanup() {
        AtomicInteger runTimes = new AtomicInteger(0);

        iter.registerCleanup(new Runnable() {
            @Override
            public void run() {
                runTimes.getAndIncrement();
            }
        });

        assertEquals(runTimes.get(), 0);
        iter.close();
        assertEquals(runTimes.get(), 1);
    }
}