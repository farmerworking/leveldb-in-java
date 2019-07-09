package com.farmerworking.leveldb.in.java.data.structure.version;

import com.farmerworking.leveldb.in.java.api.*;
import com.farmerworking.leveldb.in.java.api.Comparator;
import com.farmerworking.leveldb.in.java.api.Iterator;
import com.farmerworking.leveldb.in.java.data.structure.harness.Constructor;
import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKey;
import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKeyComparator;
import com.farmerworking.leveldb.in.java.data.structure.memory.ValueType;
import javafx.util.Pair;

import java.util.*;

public class MergingIteratorConstructor extends Constructor {

    class SimpleIterator implements Iterator<String, String> {
        private Integer index;
        private List<Pair<String, String>> data;
        private Comparator comparator;

        public SimpleIterator(List<Pair<String, String>> data, Comparator comparator) {
            assert data != null;
            this.index = null;
            this.data = data;
            this.comparator = comparator;
        }

        @Override
        public boolean valid() {
            return index != null;
        }

        @Override
        public void seekToFirst() {
            if (data.size() > 0) {
                this.index = 0;
            }
        }

        @Override
        public void seekToLast() {
            if (data.size() > 0) {
                this.index = data.size() - 1;
            }
        }

        @Override
        public void seek(String target) {
            this.index = null;
            for (int i = 0; i < data.size(); i++) {
                Pair<String, String> pair = data.get(i);
                if (comparator.compare(pair.getKey().toCharArray(), target.toCharArray()) >= 0) {
                    this.index = i;
                    break;
                }
            }
        }

        @Override
        public void next() {
            this.index ++;
            if (this.index >= data.size()) {
                this.index = null;
            }
        }

        @Override
        public void prev() {
            this.index --;
            if (this.index < 0) {
                this.index = null;
            }
        }

        @Override
        public String key() {
            return this.data.get(this.index).getKey();
        }

        @Override
        public String value() {
            return this.data.get(this.index).getValue();
        }

        @Override
        public Status status() {
            return Status.OK();
        }

        @Override
        public void close() {
        }

        @Override
        public void registerCleanup(Runnable runnable) {
        }
    }

    class InternalKeyStringComparator implements java.util.Comparator<String> {
        private final InternalKeyComparator internalKeyComparator;

        public InternalKeyStringComparator(InternalKeyComparator internalKeyComparator) {
            this.internalKeyComparator = internalKeyComparator;
        }

        @Override
        public int compare(String o1, String o2) {
            return internalKeyComparator.compare(InternalKey.decode(o1), InternalKey.decode(o2));
        }
    }

    private List<Iterator<String, String>> iteratorList = new ArrayList<>();

    public MergingIteratorConstructor(Comparator comparator) {
        super(comparator);
    }

    @Override
    public Iterator<String, String> iterator() {
        return new MergingIterator(this.comparator, iteratorList);
    }

    @Override
    public Status finishImpl(Options options, Map<String, String> data) {
        iteratorList.clear();

        List<Pair<String, String>> data1 = new ArrayList<>();
        List<Pair<String, String>> data2 = new ArrayList<>();

        SimpleIterator iterator1 = new SimpleIterator(data1, this.comparator);
        SimpleIterator iterator2 = new SimpleIterator(data2, this.comparator);

        iteratorList.add(iterator1);
        iteratorList.add(iterator2);

        Random random = new Random();
        for(String key : data.keySet()) {
            if (random.nextBoolean()) {
                data1.add(new Pair<>(key, data.get(key)));
            } else {
                data2.add(new Pair<>(key, data.get(key)));
            }
        }
        return Status.OK();
    }
}
