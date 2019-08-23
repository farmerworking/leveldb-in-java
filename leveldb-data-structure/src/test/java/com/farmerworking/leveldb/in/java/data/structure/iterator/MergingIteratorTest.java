package com.farmerworking.leveldb.in.java.data.structure.iterator;

import com.farmerworking.leveldb.in.java.api.BytewiseComparator;
import com.farmerworking.leveldb.in.java.api.Iterator;
import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.data.structure.block.BlockIterator;
import com.farmerworking.leveldb.in.java.data.structure.block.EmptyIterator;
import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKeyComparator;
import com.farmerworking.leveldb.in.java.data.structure.version.MergingIterator;
import com.google.common.collect.Lists;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.util.ArrayList;

public class MergingIteratorTest extends AbstractIteratorTest {
    @Override
    protected Iterator getImpl() {
        return new MergingIterator(new InternalKeyComparator(new BytewiseComparator()), new ArrayList<>());
    }

    @Test
    public void testInit() {
        MergingIterator iterator = new MergingIterator(new InternalKeyComparator(new BytewiseComparator()), new ArrayList<>());
        assertFalse(iterator.valid());
        assertTrue(iterator.status().isOk());
    }

    @Test
    public void testFactoryMethod() {
        Iterator<String, String> iterator = MergingIterator.newMergingIterator(
                new InternalKeyComparator(new BytewiseComparator()), new ArrayList<>());
        assertTrue(iterator instanceof EmptyIterator);

        iterator = MergingIterator.newMergingIterator(
                new InternalKeyComparator(new BytewiseComparator()), Lists.newArrayList(new BlockIterator(null, null, 1, 1)));
        assertTrue(iterator instanceof BlockIterator);

        iterator = MergingIterator.newMergingIterator(
                new InternalKeyComparator(new BytewiseComparator()), Lists.newArrayList(
                        new BlockIterator(null, null, 1, 1),
                        new BlockIterator(null, null, 1, 1)
                ));
        assertTrue(iterator instanceof MergingIterator);
    }

    @Test
    public void testStatus() {
        iter = spy(new BlockIterator(null, null, 1, 1));

        MergingIterator iterator = new MergingIterator(new InternalKeyComparator(new BytewiseComparator()), Lists.newArrayList(
                iter
        ));

        assertTrue(iterator.status().isOk());
        doReturn(Status.Corruption("force status error")).when(iter).status();
        assertTrue(iterator.status().isCorruption());
        assertEquals("force status error", iterator.status().getMessage());
    }
}
