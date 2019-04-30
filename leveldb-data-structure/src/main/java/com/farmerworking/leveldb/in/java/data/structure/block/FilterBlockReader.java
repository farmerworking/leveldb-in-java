package com.farmerworking.leveldb.in.java.data.structure.block;

import com.farmerworking.leveldb.in.java.api.FilterPolicy;
import com.farmerworking.leveldb.in.java.common.ICoding;

public class FilterBlockReader implements IFilterBlockReader {
    private FilterPolicy policy;
    private char[] data;        // Pointer to filter data (at block-start)
    private Integer offset;     // Pointer to beginning of offset array (at block-end)
    private int num;            // Number of entries in offset array
    private int baseLg;         // Encoding parameter (see kFilterBaseLg in .cc file)

    public FilterBlockReader(FilterPolicy policy, String content) {
        this.policy = policy;
        this.data = null;
        this.offset = null;
        this.num = 0;
        this.baseLg = 0;

        if (content.length() < 5) {
            return; // 1 byte for baseLg and 4 for start of offset array
        }

        char[] chars = content.toCharArray();
        this.baseLg = chars[chars.length - 1];

        int offsetOfOffset = ICoding.getInstance().decodeFixed32(chars, chars.length - ICoding.getInstance().getFixed32Length() - 1);
        if (offsetOfOffset > chars.length - ICoding.getInstance().getFixed32Length() - 1) {
            return;
        }

        data = chars;
        offset = offsetOfOffset;
        num = (chars.length - ICoding.getInstance().getFixed32Length() - 1 - offsetOfOffset) / ICoding.getInstance().getFixed32Length();
    }

    @Override
    public boolean keyMayMatch(long blockOffset, String key) {
        int index = (int) (blockOffset >>> baseLg);
        if (index < num) {
            int start = ICoding.getInstance().decodeFixed32(data, offset + index * ICoding.getInstance().getFixed32Length());
            int limit = ICoding.getInstance().decodeFixed32(data, offset + index * ICoding.getInstance().getFixed32Length() + ICoding.getInstance().getFixed32Length());
            if (start < limit && limit <= offset) {
                String filter = new String(data, start, limit - start);
                return policy.keyMayMatch(key, filter);
            } else if (start == limit) {
                // Empty filters do not match any keys
                return false;
            }
        }
        return true;  // Errors are treated as potential matches
    }
}
