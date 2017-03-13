package org.davidmoten.bigsort2;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;

import org.junit.Test;

import io.reactivex.Flowable;
import io.reactivex.internal.functions.Functions;

public class SorterTest {

    @Test
    public void test() {
        final Serializer<Integer> serializer = new Serializer<Integer>() {

            @Override
            public byte[] serialize(Integer t) throws IOException {
                return Util.intToBytes(t);
            }

            @Override
            public Integer deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
                return Util.intFromBytes(bytes);
            }
        };
        final Options<Integer, Integer, Integer> options = new Options<>(10, Comparator.naturalOrder(),
                Functions.identity(), Functions.identity(), serializer, "target");
        final Sorter<Integer, Integer, Integer> sorter = new Sorter<Integer, Integer, Integer>(options);
        final File file = sorter.sort(Flowable.range(1, 100).map(x -> 101 - x)).blockingGet();
        sorter.entries(file).doOnNext(System.out::println).subscribe();
    }

}
