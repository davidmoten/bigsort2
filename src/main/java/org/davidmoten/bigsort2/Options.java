package org.davidmoten.bigsort2;

import java.util.Comparator;

import io.reactivex.functions.Function;

public final class Options<Entry, Key, Value> {

    private final int maxInMemorySort;
    private final Comparator<Key> comparator;
    private final Function<Entry, Key> keyMapper;
    private final Function<Entry, Value> valueMapper;
    private final Serializer<Entry> serializer;
    private final Comparator<Entry> entryComparator;
    private String directory;

    public Options(int maxInMemorySort, Comparator<Key> comparator, //
            Function<Entry, Key> keyMapper, //
            Function<Entry, Value> valueMapper, //
            Serializer<Entry> serializer, String directory) {
        this.maxInMemorySort = maxInMemorySort;
        this.comparator = comparator;
        this.keyMapper = keyMapper;
        this.valueMapper = valueMapper;
        this.serializer = serializer;
        this.entryComparator = (o1, o2) -> {
            try {
                return comparator.compare(keyMapper.apply(o1), keyMapper.apply(o2));
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        };
        this.directory = directory;
    }

    public Comparator<Entry> entryComparator() {
        return entryComparator;
    }

    public String directory() {
        return directory;
    }

    public int maxInMemorySort() {
        return maxInMemorySort;
    }

    public Serializer<Entry> serializer() {
        return serializer;
    }

}
