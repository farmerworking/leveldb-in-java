package com.farmerworking.leveldb.in.java.data.structure.table;

import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.common.ICoding;
import javafx.util.Pair;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

@Data
public class Footer {
    private static long kTableMagicNumber = 0xdb4775248b80fb57L;
    public static int kEncodedLength = 2 * BlockHandle.kMaxEncodedLength + 2 * ICoding.getInstance().getFixed32Length();

    private BlockHandle metaIndexBlockHandle;
    private BlockHandle indexBlockHandle;

    public void encodeTo(StringBuilder builder) {
        assert metaIndexBlockHandle != null;
        assert indexBlockHandle != null;

        metaIndexBlockHandle.encodeTo(builder);
        indexBlockHandle.encodeTo(builder);
        // Padding
        builder.append(StringUtils.repeat((char)0, 2 * BlockHandle.kMaxEncodedLength - builder.length()));
        ICoding.getInstance().putFixed32(builder, (int) (kTableMagicNumber & 0xffffffff));
        ICoding.getInstance().putFixed32(builder, (int) (kTableMagicNumber >>> 32));
    }

    public Status decodeFrom(char[] buffer) {
        int offset = kEncodedLength - 2 * ICoding.getInstance().getFixed32Length();
        int magic_lo = ICoding.getInstance().decodeFixed32(buffer, offset);
        int magic_hi = ICoding.getInstance().decodeFixed32(buffer, offset + ICoding.getInstance().getFixed32Length());

        long magic = (Integer.toUnsignedLong(magic_hi) << 32) | Integer.toUnsignedLong(magic_lo);

        if (magic != kTableMagicNumber) {
            return Status.Corruption("not an sstable (bad magic number)");
        }

        metaIndexBlockHandle = new BlockHandle();
        Pair<Status, Integer> pair = metaIndexBlockHandle.decodeFrom(buffer, 0);
        if (pair.getKey().isNotOk()) {
            return pair.getKey();
        }

        indexBlockHandle = new BlockHandle();
        return indexBlockHandle.decodeFrom(buffer, pair.getValue()).getKey();
    }
}
