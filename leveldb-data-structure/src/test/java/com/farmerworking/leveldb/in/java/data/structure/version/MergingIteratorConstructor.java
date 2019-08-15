package com.farmerworking.leveldb.in.java.data.structure.version;

import com.farmerworking.leveldb.in.java.api.*;
import com.farmerworking.leveldb.in.java.api.Comparator;
import com.farmerworking.leveldb.in.java.api.Iterator;
import com.farmerworking.leveldb.in.java.data.structure.harness.Constructor;
import com.farmerworking.leveldb.in.java.data.structure.memory.InternalKeyComparator;
import com.farmerworking.leveldb.in.java.data.structure.utils.SimpleIterator;
import javafx.util.Pair;

import java.util.*;

public class MergingIteratorConstructor extends Constructor {

    class InternalKeyStringComparator implements java.util.Comparator<String> {
        private final InternalKeyComparator internalKeyComparator;

        public InternalKeyStringComparator(InternalKeyComparator internalKeyComparator) {
            this.internalKeyComparator = internalKeyComparator;
        }

        @Override
        public int compare(String o1, String o2) {
            return internalKeyComparator.compare(o1.toCharArray(), o2.toCharArray());
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
