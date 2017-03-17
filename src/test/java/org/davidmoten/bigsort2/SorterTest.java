package org.davidmoten.bigsort2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import com.github.davidmoten.guavamini.Lists;

import io.reactivex.Flowable;

public class SorterTest {

    @Test
    public void testSmall() {
        final int N = 100;
        final Sorter<Integer> sorter = createSorter();
        final File file = sorter.sort(Flowable.range(1, N).map(x -> N + 1 - x)).blockingGet();
        assertTrue(Flowable.sequenceEqual(sorter.entries(file), Flowable.range(1, N)).blockingGet());
    }

    @Test
    public void testFixedSize() {
        final int N = 100;
        final Sorter<Integer> sorter = createSorter(false, 10, 2);
        final File file = sorter.sort(Flowable.range(1, N).map(x -> N + 1 - x)).blockingGet();
        assertTrue(Flowable.sequenceEqual(sorter.entries(file), Flowable.range(1, N)).blockingGet());
    }

    @Test
    public void testWriteToFile() {
        final Sorter<Integer> sorter = createSorter();
        final List<Integer> list = Lists.newArrayList(1, 2, 3, 4, 5);
        final File file = sorter.writeToNewFile(list);
        assertEquals(list, sorter.entries(file).toList().blockingGet());
    }

    @Test
    public void testMerge() {
        final Sorter<Integer> sorter = createSorter();
        final File a = sorter.writeToNewFile(Lists.newArrayList(2));
        final File b = sorter.writeToNewFile(Lists.newArrayList(1, 3));
        final File file = sorter.mergeThese(Lists.newArrayList(a, b));
        assertEquals(Lists.newArrayList(1, 2, 3), sorter.entries(file).toList().blockingGet());
    }

    @Test
    public void testLargePerformance() {
        final int N = 10_000_000;
        final int maxInMemorySort = 1_000_000;
        long t = System.currentTimeMillis();
        final Sorter<Integer> sorter = createSorter(false, maxInMemorySort, 10);
        final File file = sorter.sort(Flowable.range(1, N).map(x -> N + 1 - x)).blockingGet();
        t = System.currentTimeMillis() - t;
        System.out.println(new DecimalFormat("#.000").format(N / 1_000_000.0 / t * 1000) + "m records/s");
        assertTrue(Flowable.sequenceEqual(sorter.entries(file), Flowable.range(1, N)).blockingGet());
    }

    @Test
    public void closeQuietlyNoException() {
        final AtomicBoolean closed = new AtomicBoolean();
        Sorter.closeQuietly(() -> closed.set(true));
        assertTrue(closed.get());
    }

    @Test
    public void closeQuietlyWithException() {
        final AtomicBoolean closed = new AtomicBoolean();
        Sorter.closeQuietly(() -> {
            closed.set(true);
            throw new IOException("boo");
        });
        assertTrue(closed.get());
    }

    private static Sorter<Integer> createSorter(boolean variableSize, int maxInMemorySort, int filesPerMerge) {
        return Sorter //
                .serializer(createSerializer(variableSize)) //
                .maxInMemorySort(maxInMemorySort) //
                .filesPerMerge(filesPerMerge)//
                .comparator(Comparator.<Integer>naturalOrder()) //
                .directory("target") //
                .build();
    }

    private static Sorter<Integer> createSorter() {
        return createSorter(true, 10, 2);
    }

    private static Serializer<Integer> createSerializer(boolean variableSize) {
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
                if (variableSize) {
                    return Optional.of(4);
                } else {
                    return Optional.empty();
                }
            }
        };
    }

}
