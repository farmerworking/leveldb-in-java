package com.farmerworking.leveldb.in.java.data.structure.db;

import java.util.Random;

import com.farmerworking.leveldb.in.java.api.ReadOptions;
import com.farmerworking.leveldb.in.java.api.Status;
import com.farmerworking.leveldb.in.java.api.WriteOptions;
import javafx.util.Pair;
import org.apache.commons.lang3.StringUtils;
import static org.junit.Assert.*;

public class MultiThreadRunnable implements Runnable {
    private MultiThread multiThread;

    public MultiThreadRunnable(MultiThread multiThread) {
        this.multiThread = multiThread;
    }

    @Override
    public void run() {
        int id = multiThread.getId();
        DB db = multiThread.getState().getDbTest().db;
        int counter = 0;
        System.out.println(String.format("... starting thread %d", id));
        Random random = new Random(1000 + id);
        while(!multiThread.getState().getStop().get()) {
            multiThread.getState().getCounter()[id].set(counter);
            int keyLength = random.nextInt(MultiThreadState.kNumKeys);
            String key = key(keyLength);

            if (random.nextBoolean()) {
                // Write values of the form <key, my id, counter>.
                // We add some padding for force compactions.
                assertTrue(db.put(new WriteOptions(), key, value(keyLength, id, counter)).isOk());
            } else {
                // Read a value and verify that it matches the pattern written above.
                Pair<Status, String> pair = db.get(new ReadOptions(), key);
                if (pair.getKey().isNotFound()) {
                    // Key has not yet been written
                } else {
                    // Check that the writer thread counter is >= the counter in the value
                    assertTrue(pair.getKey().isOk());
                    String value = pair.getValue();
                    String[] valueArray = value.split("\\.");
                    assertEquals(3, valueArray.length);
                    int k = Integer.parseInt(valueArray[0]), w = Integer.parseInt(valueArray[1]), c = Integer.parseInt(valueArray[2].trim());
                    assertEquals(k, keyLength);
                    assertTrue(w >= 0);
                    assertTrue(w < MultiThreadState.kNumThread);
                    assertTrue(c <= multiThread.getState().getCounter()[w].get());
                }
            }
            counter ++;
        }
        multiThread.getState().getThreadDone()[id].set(true);
        System.out.println(String.format("... stopping thread %d after %d ops", id, counter));
    }

    private String key(int i) {
        String s = String.valueOf(i);
        if (s.length() < 16) {
            s = StringUtils.repeat('0', 16 - s.length()) + s;
        } else if (s.length() > 16) {
            s = s.substring(s.length() - 16);
        }

        return s;
    }

    private String value(int keyLength, int id, int counter) {
        String s = String.valueOf(counter);
        if (s.length() < 1000) {
            s = s + StringUtils.repeat(' ', 1000 - s.length());
        }
        return String.format("%d.%d.%s", keyLength, id, s);
    }
}
