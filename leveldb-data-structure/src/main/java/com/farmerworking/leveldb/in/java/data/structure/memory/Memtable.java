package com.farmerworking.leveldb.in.java.data.structure.memory;

import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.api.Iterator;
import com.farmerworking.leveldb.in.java.data.structure.skiplist.ISkipList;
import com.farmerworking.leveldb.in.java.data.structure.skiplist.ISkipListIterator;
import com.farmerworking.leveldb.in.java.data.structure.skiplist.JDKSkipList;
import javafx.util.Pair;

public class Memtable implements IMemtable {
    private ISkipList<MemtableEntry> table;
    private MemtableEntryComparator comparator;

    public Memtable(InternalKeyComparator comparator) {
        this.comparator = new MemtableEntryComparator(comparator);
        this.table = new JDKSkipList<>(this.comparator);
    }

    @Override
    public Iterator iterator() {
        return new MemtableIterator(table);
    }

    @Override
    public void add(long sequence, ValueType type, String key, String value) {
        table.insert(new MemtableEntry(sequence, type, key, value));
    }

    @Override
    public Pair<Boolean, Pair<Status, String>> get(String userKey, long sequence) {
        ISkipListIterator<MemtableEntry> iter = table.iterator();
        MemtableEntry seek = new MemtableEntry(sequence, ValueType.kTypeValue, userKey, null);
        iter.seek(seek);

        if (iter.valid()) {
            // Check that it belongs to same user key.  We do not check the
            // sequence number since the Seek() call above should have skipped
            // all entries with overly large sequence numbers.
            MemtableEntry entry = iter.key();

            if (comparator.comparator.userComparator.compare(entry.internalKey.userKeyChar, userKey.toCharArray()) == 0) {
                if (entry.internalKey.type == ValueType.kTypeValue) {
                    return new Pair<>(true, new Pair<>(Status.OK(), entry.value));
                } else {
                    return new Pair<>(true, new Pair<>(Status.NotFound(""), ""));
                }
            } else {
                return new Pair<>(false, null);
            }
        } else {
            return new Pair<>(false, null);
        }
    }

    @Override
    public int approximateMemoryUsage() {
        return table.approximateMemoryUsage();
    }
}
