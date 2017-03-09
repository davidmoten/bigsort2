package org.davidmoten.bigsort2;

import java.util.Collections;

import io.reactivex.Flowable;
import io.reactivex.Single;

public final class Sorter<Entry, Key, Value> {

    private final Options<Entry, Key, Value> options;

    public Sorter(Options<Entry, Key, Value> options) {
        this.options = options;
    }

    public Single<Long> sort(Flowable<Entry> source) {
        return source.buffer(options.maxInMemorySort) //
                .map(list -> {
                    Collections.sort(list, options.entryComparator());
                    return list;
                }) //
                .count();
    }
}
