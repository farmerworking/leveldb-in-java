package com.farmerworking.leveldb.in.java.data.structure.table;

import com.farmerworking.leveldb.in.java.api.Status;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import static org.junit.Assert.*;

public class FooterTest {
    @Test
    public void testEncodeDecode() {
        Footer footer = new Footer();

        BlockHandle metaIndexBlockHandle = new BlockHandle();
        metaIndexBlockHandle.setOffset((long) Integer.MAX_VALUE);
        metaIndexBlockHandle.setSize((long) Integer.MAX_VALUE);

        footer.setMetaIndexBlockHandle(metaIndexBlockHandle);

        BlockHandle indexBlockHandle = new BlockHandle();
        indexBlockHandle.setOffset((long) Byte.MIN_VALUE);
        indexBlockHandle.setSize((long) Byte.MAX_VALUE);

        footer.setIndexBlockHandle(indexBlockHandle);

        StringBuilder builder = new StringBuilder();
        footer.encodeTo(builder);

        String footerContent = builder.toString();
        assertEquals(footerContent.length(), Footer.kEncodedLength);

        Footer another = new Footer();
        another.decodeFrom(footerContent.toCharArray());

        assertEquals(another.getIndexBlockHandle().getSize().longValue(), Byte.MAX_VALUE);
        assertEquals(another.getIndexBlockHandle().getOffset().longValue(), Byte.MIN_VALUE);

        assertEquals(another.getMetaIndexBlockHandle().getSize().longValue(), Integer.MAX_VALUE);
        assertEquals(another.getMetaIndexBlockHandle().getOffset().longValue(), Integer.MAX_VALUE);
    }

    @Test(expected = AssertionError.class)
    public void testEncodeWithoutSet() {
        Footer footer = new Footer();
        footer.encodeTo(new StringBuilder());
    }

    @Test
    public void testMagicNumberError() {
        char[] chars = StringUtils.repeat((char)0x01, Footer.kEncodedLength).toCharArray();
        Footer footer = new Footer();
        Status status = footer.decodeFrom(chars);
        assertNotNull(status);
        assertTrue(status.isCorruption());
    }
}