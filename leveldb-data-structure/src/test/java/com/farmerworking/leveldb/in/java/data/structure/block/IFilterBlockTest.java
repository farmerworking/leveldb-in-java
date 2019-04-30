package com.farmerworking.leveldb.in.java.data.structure.block;

import com.farmerworking.leveldb.in.java.api.FilterPolicy;
import com.farmerworking.leveldb.in.java.common.ICoding;
import org.junit.Test;

import java.util.List;
import static org.junit.Assert.*;

public abstract class IFilterBlockTest {
    class TestHashFilter implements FilterPolicy {
        @Override
        public String name() {
            return "TestHashFilter";
        }

        @Override
        public String createFilter(List<String> keys) {
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < keys.size(); i++) {
                int h = keys.get(i).hashCode();
                ICoding.getInstance().putFixed32(builder, h);
            }

            return builder.toString();
        }

        @Override
        public boolean keyMayMatch(String key, String filter) {
            int h = key.hashCode();
            for (int i = 0; i + 4 <= filter.length(); i += 4) {
                if (h == ICoding.getInstance().decodeFixed32(filter.toCharArray(), i)) {
                    return true;
                }
            }
            return false;
        }
    }

    protected abstract IFilterBlockBuilder getBuilderImpl(FilterPolicy filterPolicy);
    protected abstract IFilterBlockReader getReaderImpl(FilterPolicy filterPolicy, String blockContent);

    @Test
    public void testEmptyBuilder() {
        FilterPolicy policy = new TestHashFilter();
        IFilterBlockBuilder builder = getBuilderImpl(policy);
        String block = builder.finish();
        assertEquals(new String(new char[]{0x00, 0x00, 0x00, 0x00, 0x0b}), block);
        IFilterBlockReader reader = getReaderImpl(policy, block);
        assertTrue(reader.keyMayMatch(0, "foo"));
        assertTrue(reader.keyMayMatch(100000, "foo"));
    }


    @Test
    public void testSingleChunk() {
        FilterPolicy policy = new TestHashFilter();
        IFilterBlockBuilder builder = getBuilderImpl(policy);
        builder.startBlock(100);
        builder.addKey("foo");
        builder.addKey("bar");
        builder.addKey("box");
        builder.startBlock(200);
        builder.addKey("box");
        builder.startBlock(300);
        builder.addKey("hello");
        String block = builder.finish();
        IFilterBlockReader reader = getReaderImpl(policy, block);
        assertTrue(reader.keyMayMatch(100, "foo"));
        assertTrue(reader.keyMayMatch(100, "bar"));
        assertTrue(reader.keyMayMatch(100, "box"));
        assertTrue(reader.keyMayMatch(100, "hello"));
        assertTrue(reader.keyMayMatch(100, "foo"));
        assertTrue(! reader.keyMayMatch(100, "missing"));
        assertTrue(! reader.keyMayMatch(100, "other"));
    }


    @Test
    public void testMultiChunk() {
        FilterPolicy policy = new TestHashFilter();
        IFilterBlockBuilder builder = getBuilderImpl(policy);

        // First filter
        builder.startBlock(0);
        builder.addKey("foo");
        builder.startBlock(2000);
        builder.addKey("bar");

        // Second filter
        builder.startBlock(3100);
        builder.addKey("box");

        // Third filter is empty

        // Last filter
        builder.startBlock(9000);
        builder.addKey("box");
        builder.addKey("hello");

        String block = builder.finish();
        IFilterBlockReader reader = getReaderImpl(policy, block);

        // Check first filter
        assertTrue(reader.keyMayMatch(0, "foo"));
        assertTrue(reader.keyMayMatch(2000, "bar"));
        assertTrue(! reader.keyMayMatch(0, "box"));
        assertTrue(! reader.keyMayMatch(0, "hello"));

        // Check second filter
        assertTrue(reader.keyMayMatch(3100, "box"));
        assertTrue(! reader.keyMayMatch(3100, "foo"));
        assertTrue(! reader.keyMayMatch(3100, "bar"));
        assertTrue(! reader.keyMayMatch(3100, "hello"));

        // Check third filter (empty)
        assertTrue(! reader.keyMayMatch(4100, "foo"));
        assertTrue(! reader.keyMayMatch(4100, "bar"));
        assertTrue(! reader.keyMayMatch(4100, "box"));
        assertTrue(! reader.keyMayMatch(4100, "hello"));

        // Check last filter
        assertTrue(reader.keyMayMatch(9000, "box"));
        assertTrue(reader.keyMayMatch(9000, "hello"));
        assertTrue(! reader.keyMayMatch(9000, "foo"));
        assertTrue(! reader.keyMayMatch(9000, "bar"));

    }
}
