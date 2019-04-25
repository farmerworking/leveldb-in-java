package com.farmerworking.leveldb.in.java.data.structure.writebatch;

import com.farmerworking.leveldb.in.java.common.ICoding;
import com.farmerworking.leveldb.in.java.common.Status;
import com.farmerworking.leveldb.in.java.data.structure.memory.ValueType;
import com.farmerworking.leveldb.in.java.data.structure.skiplist.Sizable;

import java.util.ArrayList;
import java.util.List;

public class WriteBatch {
    private long sequence;
    private List<WriteBatchItem> commands = new ArrayList<>();

    class WriteBatchItem implements Sizable {
        public ValueType valueType;
        public String key;
        public String value;

        public WriteBatchItem(ValueType valueType, String key, String value) {
            this.valueType = valueType;
            this.key = key;
            this.value = value;
        }

        @Override
        public int memoryUsage() {
            if (valueType == ValueType.kTypeDeletion) {
                return 1 + ICoding.getInstance().varintLength(key.length()) + key.length();
            } else {
                return 1 +
                        ICoding.getInstance().varintLength(key.length()) + key.length() +
                        ICoding.getInstance().varintLength(value.length()) + value.length();
            }
        }
    }

    public WriteBatch() {
        clear();
    }

    public void clear() {
        sequence = 0;
        commands.clear();
    }

    public void put(String key, String value) {
        this.commands.add(new WriteBatchItem(ValueType.kTypeValue, key, value));
    }

    public void delete(String key) {
        this.commands.add(new WriteBatchItem(ValueType.kTypeDeletion, key, null));
    }

    public int approximateSize() {
        int result = 0;
        for(WriteBatchItem item : commands) {
            result += item.memoryUsage();
        }
        return result;
    }

    public Status iterate(WriteBatchIterateHandler handler) {
        for(WriteBatchItem item : commands) {
            if (item.valueType == ValueType.kTypeValue) {
                handler.put(item.key, item.value);
            } else {
                handler.delete(item.key);
            }
        }

        return Status.OK();
    }

    int getCount() {
        return commands.size();
    }

    long getSequence() {
        return sequence;
    }

    void setSequence(long sequence) {
        this.sequence = sequence;
    }

    void append(WriteBatch writeBatch) {
        this.commands.addAll(writeBatch.commands);
    }
}
