package org.davidmoten.bigsort2;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

import com.github.davidmoten.guavamini.Preconditions;
import com.github.davidmoten.guavamini.annotations.VisibleForTesting;

import io.reactivex.Flowable;
import io.reactivex.Single;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;
import io.reactivex.schedulers.Schedulers;

public final class Sorter<Entry, Key, Value> {

    private final Options<Entry, Key, Value> options;

    private final AtomicLong index = new AtomicLong();

    public Sorter(Options<Entry, Key, Value> options) {
        this.options = options;
    }

    public Single<File> sort(Flowable<Entry> source) {
        return source //
                .buffer(options.maxInMemorySort()) //
                .flatMap(list -> Flowable
                        .fromCallable(() -> sortInPlace(list, options.entryComparator()))
                        .map(sorted -> writeToNewFile(sorted)) //
                        .subscribeOn(Schedulers.computation()))
                .toList().map(files -> merge(files));
    }

    public Flowable<Entry> entries(File file) {
        final Callable<InputStream> resourceSupplier = () -> new BufferedInputStream(
                new FileInputStream(file));
        final Function<InputStream, Flowable<Entry>> sourceSupplier = is -> Flowable.generate(c -> {
            if (options.serializer().size().isPresent()) {
                final byte[] bytes = new byte[options.serializer().size().get()];
                final int num = is.read(bytes);
                if (num == -1) {
                    c.onComplete();
                } else {
                    final Entry entry = options.serializer().deserialize(bytes);
                    c.onNext(entry);
                }
            } else {
                final byte a = (byte) is.read();
                if (a == -1) {
                    c.onComplete();
                } else {
                    final int length = Util.intFromBytes(a, (byte) is.read(), (byte) is.read(),
                            (byte) is.read());
                    final byte[] bytes = new byte[length];
                    is.read(bytes);
                    final Entry entry = options.serializer().deserialize(bytes);
                    c.onNext(entry);
                }
            }
        });
        final Consumer<InputStream> resourceDisposer = is -> is.close();
        return Flowable.using(resourceSupplier, sourceSupplier, resourceDisposer);
    }

    @VisibleForTesting
    File writeToNewFile(List<Entry> list) {
        final File file = new File(options.directory(), index.incrementAndGet() + ".sort");
        try (BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(file))) {
            for (final Entry entry : list) {
                final byte[] bytes = options.serializer().serialize(entry);
                if (!options.serializer().size().isPresent()) {
                    out.write(Util.intToBytes(bytes.length));
                }
                out.write(bytes);
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        return file;
    }

    private File merge(List<File> files) {
        Preconditions.checkArgument(!files.isEmpty());
        if (files.size() <= options.filesPerMerge()) {
            return mergeThese(files);
        } else {
            int i = 0;
            final int size = files.size();
            final List<File> list = new ArrayList<>();
            while (i < size) {
                list.add(merge(files.subList(i, Math.min(size, i + options.filesPerMerge()))));
                i += options.filesPerMerge();
            }
            for (final File f : files) {
                f.delete();
            }
            return merge(list);
        }
    }

    @VisibleForTesting
    File mergeThese(List<File> files) {
        final File file = new File(options.directory(), index.incrementAndGet() + ".merge");
        final DataInputStream[] fileStream = new DataInputStream[files.size()];
        try (OutputStream out = new BufferedOutputStream(new FileOutputStream(file))) {
            for (int i = 0; i < files.size(); i++) {
                fileStream[i] = new DataInputStream(
                        new BufferedInputStream(new FileInputStream(files.get(i))));
            }
            // holds the current entry for each fileStream (null if not read
            // yet)
            @SuppressWarnings("unchecked")
            final Entry[] entry = (Entry[]) new Object[files.size()];
            while (true) {
                Entry leastEntry = null;
                int leastIndex = -1;
                for (int i = 0; i < files.size(); i++) {
                    if (fileStream[i] != null) {
                        if (entry[i] == null) {
                            // latest entry not read yet so read it
                            if (options.serializer().size().isPresent()) {
                                // fixed size records
                                final byte[] bytes = new byte[options.serializer().size().get()];
                                int count = fileStream[i].read(bytes);
                                if (count == -1) {
                                    fileStream[i].close();
                                    fileStream[i] = null;
                                    entry[i] = null;
                                } else {
                                    entry[i] = options.serializer().deserialize(bytes);
                                }
                            } else {
                                // variable size records
                                final int entrySize = readInt(fileStream[i]);
                                if (entrySize == -1) {
                                    fileStream[i].close();
                                    fileStream[i] = null;
                                    entry[i] = null;
                                } else {
                                    final byte[] bytes = new byte[entrySize];
                                    fileStream[i].read(bytes);
                                    entry[i] = options.serializer().deserialize(bytes);
                                }
                            }
                        }
                        if (entry[i] != null) {
                            if (leastEntry == null) {
                                leastEntry = entry[i];
                                leastIndex = i;
                            } else if (options.entryComparator().compare(entry[i],
                                    leastEntry) < 0) {
                                leastEntry = entry[i];
                                leastIndex = i;
                            }
                        }
                    }
                }
                if (leastIndex == -1) {
                    break;
                }
                final byte[] bytes = options.serializer().serialize(leastEntry);
                leastEntry = null;
                entry[leastIndex] = null;
                if (!options.serializer().size().isPresent()) {
                    out.write(Util.intToBytes(bytes.length));
                }
                out.write(bytes);
            }
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        } finally {
            // cleanup the input streams in case of failure
            for (DataInputStream d : fileStream) {
                closeQuietly(d);
            }
        }
        for (final File f : files) {
            f.delete();
        }
        return file;
    }

    public final int readInt(InputStream is) throws IOException {
        int ch1 = is.read();
        if (ch1 == -1)
            return -1;
        int ch2 = is.read();
        int ch3 = is.read();
        int ch4 = is.read();
        if ((ch1 | ch2 | ch3 | ch4) < 0)
            throw new EOFException();
        return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
    }

    private static void closeQuietly(DataInputStream d) {
        if (d != null) {
            try {
                d.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }

    private static <T> List<T> sortInPlace(List<T> list, Comparator<T> comparator) {
        Collections.sort(list, comparator);
        return list;
    }
}
