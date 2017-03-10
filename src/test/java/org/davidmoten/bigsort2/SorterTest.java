package org.davidmoten.bigsort2;

import java.io.IOException;
import java.util.Comparator;

import org.junit.Test;

import io.reactivex.Flowable;
import io.reactivex.internal.functions.Functions;

public class SorterTest {

    @Test
    public void test() {
        Serializer<Integer> serializer = new Serializer<Integer>() {

            @Override
            public byte[] serialize(Integer t) throws IOException {
                return Util.toBytes(t);
            }

            @Override
            public Integer deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
                return Util.fromBytes(bytes);
            }
        };
        Options<Integer, Integer, Integer> options = new Options<>(1000, Comparator.naturalOrder(),
                Functions.identity(), Functions.identity(), serializer, "target");
        Sorter<Integer, Integer, Integer> sorter = new Sorter<Integer, Integer, Integer>(options);
        sorter.sort(Flowable.range(1, 10000)).blockingGet();
    }

    

}
