package com.farmerworking.leveldb.in.java.data.structure.table;

import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.common.ICoding;
import javafx.util.Pair;
import lombok.Data;

@Data
public class BlockHandle {
    public static int kMaxEncodedLength = 2 * ICoding.getInstance().getMaxVarint64Length();

    private Long offset;
    private Long size;

    public void encodeTo(StringBuilder builder) {
        assert offset != null;
        assert size != null;

        ICoding.getInstance().putVarint64(builder, offset);
        ICoding.getInstance().putVarint64(builder, size);
    }

    public Pair<Status, Integer> decodeFrom(char[] buffer, int bufferOffset) {
        Pair<Long, Integer> pair = ICoding.getInstance().decodeVarint64(buffer, bufferOffset);
        if (pair == null) {
            return new Pair<>(Status.Corruption("bad block handle"), null);
        } else {
            offset = pair.getKey();
        }

        pair = ICoding.getInstance().decodeVarint64(buffer, pair.getValue());
        if (pair == null) {
            return new Pair<>(Status.Corruption("bad block handle"), null);
        } else {
            size = pair.getKey();
        }

        return new Pair<>(Status.OK(), pair.getValue());
    }
}
