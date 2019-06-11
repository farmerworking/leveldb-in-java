package com.farmerworking.leveldb.in.java.data.structure.table;

import com.farmerworking.leveldb.in.java.api.*;
import com.farmerworking.leveldb.in.java.data.structure.harness.Constructor;
import com.farmerworking.leveldb.in.java.data.structure.utils.StringDest;
import com.farmerworking.leveldb.in.java.data.structure.utils.StringRandomAccessSource;

import java.util.Map;

class TableConstructor extends Constructor {
    private Options tableOptions;
    private ITableReader itableReader;

    public TableConstructor(Comparator comparator, Options options) {
        super(comparator);
        this.tableOptions = options;
    }

    public TableConstructor(Comparator comparator) {
        super(comparator);
    }

    @Override
    public Iterator<String, String> iterator() {
        return itableReader.iterator(new ReadOptions());
    }

    public Iterator<String, String> iterator(Deleter deleter) {
        return ((TableReader)itableReader).iterator(new ReadOptions(), deleter);
    }


    @Override
    public Status finishImpl(Options options, Map<String, String> data) {
        StringDest stringDest = new StringDest();
        ITableBuilder builder = ITableBuilder.getDefaultImpl(options, stringDest);

        for(String key : data.keySet()) {
            builder.add(key, data.get(key));
            assert builder.status().isOk();
        }

        Status status = builder.finish();
        assert status.isOk();
        assert stringDest.getContent().length() == builder.fileSize();

        StringRandomAccessSource source = new StringRandomAccessSource(stringDest.getContent());
        itableReader = ITableReader.getDefaultImpl();

        if (tableOptions == null) {
            tableOptions = new Options();
        }

        tableOptions.setComparator(options.getComparator());
        status = itableReader.open(tableOptions, source, source.getContent().length());
        return status;
    }

    public ITableReader getItableReader() {
        return itableReader;
    }

    public long approximateOffsetOf(String key) {
        return itableReader.approximateOffsetOf(key);
    }
}
