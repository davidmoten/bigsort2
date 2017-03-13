package org.davidmoten.bigsort2;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;

import org.junit.Test;

import com.github.davidmoten.guavamini.Lists;

import io.reactivex.Flowable;
import io.reactivex.internal.functions.Functions;

public class SorterTest {

    @Test
    public void test() {
        final Sorter<Integer, Integer, Integer> sorter = createSorter();
        final File file = sorter.sort(Flowable.range(1, 20).map(x -> 101 - x)).blockingGet();
        sorter.entries(file).doOnNext(System.out::println).subscribe();
    }

    @Test
    public void testWriteToFile() {
        final Sorter<Integer, Integer, Integer> sorter = createSorter();
        final List<Integer> list = Lists.newArrayList(1, 2, 3, 4, 5);
        final File file = sorter.writeToNewFile(list);
        assertEquals(list, sorter.entries(file).toList().blockingGet());
    }

    @Test
    public void testMerge() {
        final Sorter<Integer, Integer, Integer> sorter = createSorter();
        final File a = sorter.writeToNewFile(Lists.newArrayList(2));
        final File b = sorter.writeToNewFile(Lists.newArrayList(1, 3));
        final File file = sorter.mergeThese(Lists.newArrayList(a, b));
        assertEquals(Lists.newArrayList(1, 2, 3), sorter.entries(file).toList().blockingGet());

    }

    private static Sorter<Integer, Integer, Integer> createSorter() {
        final Serializer<Integer> serializer = createSerializer();
        final Options<Integer, Integer, Integer> options = new Options<>(10, Comparator.naturalOrder(),
                Functions.identity(), serializer, "target");
        final Sorter<Integer, Integer, Integer> sorter = new Sorter<Integer, Integer, Integer>(options);
        return sorter;
    }

    private static Serializer<Integer> createSerializer() {
        return new Serializer<Integer>() {

            @Override
            public byte[] serialize(Integer t) throws IOException {
                return Util.intToBytes(t);
            }

            @Override
            public Integer deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
                return Util.intFromBytes(bytes);
            }
        };
    }

}
