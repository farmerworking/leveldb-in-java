package com.farmerworking.leveldb.in.java.data.structure.harness;

import com.farmerworking.leveldb.in.java.api.Comparator;
import com.farmerworking.leveldb.in.java.data.structure.utils.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Random;

public abstract class HarnessTest {
    private Harness instance;

    protected abstract List<TestArg> getTestArgList();

    protected abstract Constructor getConstructor(Comparator comparator);

    @Before
    public void setUp() throws Exception {
        instance = new Harness();
    }

    @Test
    public void testEmpty() {
        for(TestArg testArg : getTestArgList()) {
            instance.init(testArg);
            instance.setConstructor(getConstructor(instance.getOptions().getComparator()));
            instance.test();
        }
    }

    @Test
    public void testSimpleEmptyKey() {
        for(TestArg testArg : getTestArgList()) {
            instance.init(testArg);
            instance.setConstructor(getConstructor(instance.getOptions().getComparator()));
            instance.add("", "v");
            instance.test();
        }
    }

    @Test
    public void testSimpleSingle() {
        for(TestArg testArg : getTestArgList()) {
            instance.init(testArg);
            instance.setConstructor(getConstructor(instance.getOptions().getComparator()));
            instance.add("abc", "v");
            instance.test();
        }
    }

    @Test
    public void testSimpleMulti() {
        for(TestArg testArg : getTestArgList()) {
            instance.init(testArg);
            instance.setConstructor(getConstructor(instance.getOptions().getComparator()));
            instance.add("abc", "v");
            instance.add("abcd", "v");
            instance.add("ac", "v2");
            instance.test();
        }
    }

    @Test
    public void testSimpleSpecialKey() {
        for(TestArg testArg : getTestArgList()) {
            instance.init(testArg);
            instance.setConstructor(getConstructor(instance.getOptions().getComparator()));
            instance.add(new String(new char[]{(char)255, (char)255}), "v3");
            instance.test();
        }
    }

    @Test
    public void testRandomized() {
        List<TestArg> list = getTestArgList();
        Random random = new Random();
        for (int i = 0; i < list.size(); i++) {
            TestArg testArg = list.get(i);
            instance.init(testArg);
            instance.setConstructor(getConstructor(instance.getOptions().getComparator()));
            for (int num_entries = 0; num_entries < 2000; num_entries += (num_entries < 50 ? 1 : 200)) {
                if ((num_entries % 10) == 0) {
                    System.out.println(String.format("case %d of %d: num_entries = %d", i + 1, list.size(), num_entries));
                }
                for (int e = 0; e < num_entries; e++) {
                    instance.add(TestUtils.randomKey(random.nextInt(4)), TestUtils.randomString(random.nextInt(5)));
                }
                instance.test();
            }
        }
    }
}
