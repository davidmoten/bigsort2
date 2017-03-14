package org.davidmoten.bigsort2;

import java.util.Comparator;

import io.reactivex.functions.Function;

public final class Options<Entry, Key> {

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

    public static final class Builder<Entry> {

        private int maxInMemorySort = 1000000;
        private String directory;
        private int filesPerMerge = 2;
        private Serializer<Entry> serializer;

        public Builder(Serializer<Entry> serializer) {
            this.serializer = serializer;
        }

        public Builder<Entry> directory(String directory) {
            this.directory = directory;
            return this;
        }

        public Builder<Entry> maxInMemorySort(int value) {
            this.maxInMemorySort = value;
            return this;
        }

        public Builder<Entry> filesPerMerge(int value) {
            this.filesPerMerge = value;
            return this;
        }

        public <Key> Builder2<Entry, Key> comparator(Comparator<Key> comparator) {
            return new Builder2<Entry, Key>(this, comparator);
        }

    }

    public static final class Builder2<Entry, Key> {

        private final Builder<Entry> builder;
        private final Comparator<Key> comparator;
        private Function<Entry, Key> keyMapper;

        public Builder2(Builder<Entry> builder, Comparator<Key> comparator) {
            this.builder = builder;
            this.comparator = comparator;
        }

        public Builder2<Entry, Key> keyMapper(Function<Entry, Key> keyMapper) {
            this.keyMapper = keyMapper;
            return this;
        }

        public Builder2<Entry, Key> directory(String directory) {
            builder.directory = directory;
            return this;
        }

        public Builder2<Entry, Key> maxInMemorySort(int value) {
            builder.maxInMemorySort = value;
            return this;
        }

        public Builder2<Entry, Key> filesPerMerge(int value) {
            builder.filesPerMerge = value;
            return this;
        }

        public Sorter<Entry, Key> build() {
            Options<Entry, Key> options = new Options<Entry, Key>(builder.maxInMemorySort,
                    comparator, keyMapper, builder.serializer, builder.directory,
                    builder.filesPerMerge);
            return new Sorter<Entry, Key>(options);
        }

    }

}
