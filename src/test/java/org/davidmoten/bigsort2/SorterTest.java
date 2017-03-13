package org.davidmoten.bigsort2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import org.junit.Test;

import com.github.davidmoten.guavamini.Lists;

import io.reactivex.Flowable;
import io.reactivex.internal.functions.Functions;

public class SorterTest {

    @Test
    public void testSmall() {
        final int N = 100;
        final Sorter<Integer, Integer, Integer> sorter = createSorter();
        final File file = sorter.sort(Flowable.range(1, N).map(x -> N + 1 - x)).blockingGet();
        assertTrue(Flowable.sequenceEqual(sorter.entries(file), Flowable.range(1, N)).blockingGet());
    }

    @Test
    public void testFixedSize() {
        final int N = 100;
        final Sorter<Integer, Integer, Integer> sorter = createSorter(false, 10);
        final File file = sorter.sort(Flowable.range(1, N).map(x -> N + 1 - x)).blockingGet();
        assertTrue(Flowable.sequenceEqual(sorter.entries(file), Flowable.range(1, N)).blockingGet());
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

    @Test
    public void testLarge() {
        final int N = 1_000_000;
        final int maxInMemorySort = 1000;
        final Sorter<Integer, Integer, Integer> sorter = createSorter(false, maxInMemorySort);
        final File file = sorter.sort(Flowable.range(1, N).map(x -> N + 1 - x)).blockingGet();
        assertTrue(Flowable.sequenceEqual(sorter.entries(file), Flowable.range(1, N)).blockingGet());
    }

    private static Sorter<Integer, Integer, Integer> createSorter(boolean dynamicSize, int maxInMemorySort) {
        final Serializer<Integer> serializer = createSerializer(dynamicSize);
        final Options<Integer, Integer, Integer> options = new Options<>(maxInMemorySort, Comparator.naturalOrder(),
                Functions.identity(), serializer, "target");
        final Sorter<Integer, Integer, Integer> sorter = new Sorter<Integer, Integer, Integer>(options);
        return sorter;
    }

    private static Sorter<Integer, Integer, Integer> createSorter() {
        return createSorter(true, 10);
    }

    private static Serializer<Integer> createSerializer(boolean dynamicSize) {
        return new Serializer<Integer>() {

            @Override
            public byte[] serialize(Integer t) throws IOException {
                return Util.intToBytes(t);
            }

            @Override
            public Integer deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
                return Util.intFromBytes(bytes);
            }

            @Override
            public Optional<Integer> size() {
                if (dynamicSize) {
                    return Optional.of(4);
                } else {
                    return Optional.empty();
                }
            }
        };
    }

}
