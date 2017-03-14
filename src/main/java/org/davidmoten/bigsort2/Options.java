package org.davidmoten.bigsort2;

import java.util.Comparator;

import io.reactivex.functions.Function;

public final class Options<Entry, Key, Value> {

    private final int maxInMemorySort;
    private final Comparator<Key> comparator;
    private final Serializer<Entry> serializer;
    private final Comparator<Entry> entryComparator;
    private final String directory;
    private final int filesPerMerge;

    public Options(int maxInMemorySort, Comparator<Key> comparator, //
            Function<Entry, Key> keyMapper, //
            Serializer<Entry> serializer, String directory, int filesPerMerge) {
        this.maxInMemorySort = maxInMemorySort;
        this.comparator = comparator;
        this.serializer = serializer;
        this.filesPerMerge = filesPerMerge;
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

    // TODO unused?
    public Comparator<Key> comparator() {
        return comparator;
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

    public int filesPerMerge() {
        return filesPerMerge;
    }

    public static final class Builder {

        private int maxInMemorySort;
        private String directory;
        private int filesPerMerge = 2;

        private Builder() {
        }
        
        public Builder directory(String directory) {
            this.directory = directory;
            return this;
        }
    }
}
