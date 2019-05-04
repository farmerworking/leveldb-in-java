package com.farmerworking.leveldb.in.java.data.structure.utils;

import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.file.SequentialFile;
import javafx.util.Pair;

import static org.junit.Assert.assertTrue;

public class StringSource implements SequentialFile {
    private String contents = "";
    private boolean forceError;
    private boolean returnedPartial;

    public StringSource() {
        this.forceError = false;
        this.returnedPartial = false;
    }

    public StringSource(String contents) {
        this.contents = contents;
        this.forceError = false;
        this.returnedPartial = false;
    }

    @Override
    public Pair<Status, String> read(int n) {
        assertTrue("must not Read() after eof/error", !returnedPartial);

        if (forceError) {
            forceError = false;
            returnedPartial = true;
            return new Pair<>(Status.Corruption("read error"), "");
        }

        if (contents.length() < n) {
            n = contents.length();
            returnedPartial = true;
        }
        String result = contents.substring(0, n);
        contents = contents.substring(n);
        return new Pair<>(Status.OK(), result);
    }

    @Override
    public Status skip(long n) {
        if (forceError) {
            forceError = false;
            return Status.Corruption("skip error");
        }

        if (n > contents.length()) {
            contents = "";
            return Status.NotFound("in-memory file skipped past end");
        }

        contents = contents.substring((int) n);
        return Status.OK();
    }

    public String getContents() {
        return contents;
    }

    public void setContents(String contents) {
        this.contents = contents;
    }

    public boolean isForceError() {
        return forceError;
    }

    public void setForceError(boolean forceError) {
        this.forceError = forceError;
    }

    public boolean isReturnedPartial() {
        return returnedPartial;
    }

    public void setReturnedPartial(boolean returnedPartial) {
        this.returnedPartial = returnedPartial;
    }
}
