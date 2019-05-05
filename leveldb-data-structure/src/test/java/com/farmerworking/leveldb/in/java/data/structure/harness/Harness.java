package com.farmerworking.leveldb.in.java.data.structure.harness;

import com.farmerworking.leveldb.in.java.api.*;
import com.farmerworking.leveldb.in.java.api.Comparator;
import com.farmerworking.leveldb.in.java.api.Iterator;

import java.util.*;

import static org.junit.Assert.*;

public class Harness {
    private Options options;
    private Constructor constructor;
    
    void init(TestArg arg) {
        options = new Options();

        options.setBlockRestartInterval(arg.restartInterval);
        // Use shorter block size for tests to exercise block boundary
        // conditions more.
        options.setBlockSize(256);
        if (arg.reverseCompare) {
            options.setComparator(new ReverseKeyComparator());
        }
    }

    public Options getOptions() {
        return options;
    }

    public void setConstructor(Constructor constructor) {
        this.constructor = constructor;
    }

    void add(String key, String value) {
        constructor.add(key, value);
    }

    void test() {
        Random random = new Random();
        List<String> keys = constructor.finish(this.options);
        Map<String, String> data = constructor.getData();

        testForwardScan(keys, data);
        testBackwardScan(keys, data);
        testRandomAccess(random, keys, data);
    }

    private void testRandomAccess(Random random, List<String> keys, Map<String, String> data) {
        boolean kVerbose = false;
        Iterator<String, String> iter = constructor.iterator();
        assertTrue(!iter.valid());
        if (kVerbose) {
            System.out.println("---");
        }

        Integer index = null;
        for (int i = 0; i < 200; i++) {
            int toss = random.nextInt(5);
            switch (toss) {
                case 0: {
                    if (iter.valid()) {
                        if (kVerbose) {
                            System.out.println("Next");
                        }
                        iter.next();
                        index ++;
                        assertEquals(toString(index, keys, data), toString(iter));
                    }
                    break;
                }
                case 1: {
                    if (kVerbose) {
                        System.out.println("SeekToFirst");
                    }
                    iter.seekToFirst();
                    index = 0;
                    assertEquals(toString(index, keys, data), toString(iter));
                    break;
                }

                case 2: {
                    String key = pickRandomKey(random, keys);
                    if (kVerbose) {
                        System.out.println(String.format("Seek '%s'", key));
                    }
                    iter.seek(key);
                    index = lowerBound(key, keys);
                    assertEquals(toString(index, keys, data), toString(iter));
                    break;
                }

                case 3: {
                    if (iter.valid()) {
                        if (kVerbose) {
                            System.out.println("Prev");
                        }
                        iter.prev();
                        index --;
                        assertEquals(toString(index, keys, data), toString(iter));
                    }
                    break;
                }

                case 4: {
                    if (kVerbose) {
                        System.out.println("SeekToLast");
                    }
                    iter.seekToLast();
                    index = keys.size() - 1;
                    assertEquals(toString(index, keys, data), toString(iter));
                    break;
                }
            }
        }
    }

    private Integer lowerBound(String randomKey, List<String> keys) {
        char[] keyChars = randomKey.toCharArray();
        for (int i = 0; i < keys.size(); i++) {
            if (options.getComparator().compare(keys.get(i).toCharArray(), keyChars) >= 0) {
                return i;
            }
        }
        return keys.size();
    }

    private String pickRandomKey(Random random, List<String> keys) {
        if (keys.isEmpty()) {
            return "foo";
        } else {
            int index = random.nextInt(keys.size());
            String result = keys.get(index);
            switch (random.nextInt(3)) {
                case 0:
                    // Return an existing key
                    break;
                case 1: {
                    // Attempt to return something smaller than an existing key
                    if (result.length() > 0 && result.charAt(result.length()-1) > (char)0) {
                        char[] chars = result.toCharArray();
                        chars[chars.length - 1] = (char)(chars[chars.length - 1] - 1);
                        result = new String(chars);
                    }
                    break;
                }
                case 2: {
                    // Return something larger than an existing key
                    result = increment(options.getComparator(), result);
                    break;
                }
            }
            return result;
        }
    }

    private String increment(Comparator comparator, String s) {
        if (comparator instanceof BytewiseComparator) {
            return s + '\0';
        } else {
            assert(comparator instanceof ReverseKeyComparator);
            char[] result = new char[s.length() + 1];
            result[0] = '\0';
            System.arraycopy(s.toCharArray(), 0, result, 1, s.length());
            return new String(result);
        }
    }

    private void testBackwardScan(List<String> keys, Map<String, String> data) {
        Iterator<String, String> iter = constructor.iterator();
        assertTrue(!iter.valid());
        iter.seekToLast();

        ArrayList<String> reverse = new ArrayList<>(keys);
        Collections.reverse(reverse);

        for(String key : reverse) {
            assertEquals(toString(key, data.get(key)), toString(iter));
            iter.prev();
        }
        assertEquals("END", toString(iter));
        assertTrue(!iter.valid());
    }

    private void testForwardScan(List<String> keys, Map<String, String> data) {
        Iterator<String, String> iter = constructor.iterator();
        assertTrue(!iter.valid());
        iter.seekToFirst();
        for(String key : keys) {
            assertEquals(toString(key, data.get(key)), toString(iter));
            iter.next();
        }
        assertEquals("END", toString(iter));
        assertTrue(!iter.valid());
    }

    private String toString(Iterator<String, String> iter) {
        if (!iter.valid()) {
            return "END";
        } else {
            return toString(iter.key(), iter.value());
        }
    }

    private String toString(Integer index, List<String> keys, Map<String, String> data) {
        if (index >= keys.size() || index < 0) {
            return "END";
        } else {
            return toString(keys.get(index), data.get(keys.get(index)));
        }
    }

    private String toString(String key, String value) {
        return "'" + key + "->" + value + "'";
    }
}
