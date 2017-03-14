package org.davidmoten.bigsort2;

import java.util.Comparator;

final class Options<Entry> {

    private final int maxInMemorySort;
    private final Serializer<Entry> serializer;
    private final Comparator<Entry> comparator;
    private final String directory;
    private final int filesPerMerge;

    Options(int maxInMemorySort, Comparator<Entry> comparator, //
            Serializer<Entry> serializer, String directory, int filesPerMerge) {
        this.maxInMemorySort = maxInMemorySort;
        this.comparator = comparator;
        this.serializer = serializer;
        this.filesPerMerge = filesPerMerge;
        this.directory = directory;
    }

    public Comparator<Entry> comparator() {
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
        private String directory = System.getProperty("java.io.tmpdir");
        private int filesPerMerge = 2;
        private final Serializer<Entry> serializer;
        private Comparator<Entry> comparator;

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

        public Builder<Entry> comparator(Comparator<Entry> comparator) {
            this.comparator = comparator;
            return this;
        }

        public Sorter<Entry> build() {
            final Options<Entry> options = new Options<Entry>(maxInMemorySort, comparator, serializer, directory,
                    filesPerMerge);
            return new Sorter<Entry>(options);
        }

    }

}
