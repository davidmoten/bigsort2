package org.davidmoten.bigsort2;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.Flowable;
import io.reactivex.Single;
import io.reactivex.schedulers.Schedulers;

public final class Sorter<Entry, Key, Value> {

    private static final Logger log = LoggerFactory.getLogger(Sorter.class);

    private final Options<Entry, Key, Value> options;

    private final AtomicLong index = new AtomicLong();

    public Sorter(Options<Entry, Key, Value> options) {
        this.options = options;
    }

    public Single<Long> sort(Flowable<Entry> source) {
        return source //
                .buffer(options.maxInMemorySort()) //
                .flatMap(list -> Flowable
                        .fromCallable(() -> sortInPlace(list, options.entryComparator()))
                        .map(sorted -> writeToNewFile(sorted)) //
                        .subscribeOn(Schedulers.computation()))
                .buffer(options.filesPerMerge()) //
                .map(files -> merge(files)).count();
    }

    private File writeToNewFile(List<Entry> list) {
        File file = new File(options.directory(), index.incrementAndGet() + ".sort");
        try (BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(file))) {
            for (Entry entry : list) {
                byte[] bytes = options.serializer().serialize(entry);
                out.write(Util.toBytes(bytes.length));
                out.write(bytes);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        log.info("" + list.size() + " entries written to " + file);
        return file;
    }

    private File merge(List<File> files) {
        File file = new File(options.directory(), index.incrementAndGet() + ".merge");
        try (OutputStream out = new BufferedOutputStream(new FileOutputStream(file))) {

            LongMappedByteBuffer[] bb = new LongMappedByteBuffer[files.size()];
            for (int i = 0; i < files.size(); i++) {
                File f = files.get(i);
                bb[i] = new LongMappedByteBuffer(f);
            }
            long[] sizes = new long[files.size()];
            for (int i = 0; i < sizes.length; i++) {
                sizes[i] = files.get(i).length();
            }
            long[] positions = new long[files.size()];
            @SuppressWarnings("unchecked")
            Entry[] entry = (Entry[]) new Object[files.size()];
            while (true) {
                Entry leastEntry = null;
                int leastIndex = -1;
                for (int i = 0; i < positions.length; i++) {
                    long pos = positions[i];
                    if (pos > 0) {
                        if (entry[i] == null) {
                            bb[i].position(pos);
                            int entrySize = bb[i].readInt();
                            byte[] bytes = new byte[entrySize];
                            bb[i].get(bytes);
                            entry[i] = options.serializer().deserialize(bytes);
                        }
                        if (leastEntry == null) {
                            leastEntry = entry[i];
                            leastIndex = -1;
                        } else {
                            if (options.entryComparator().compare(entry[i], leastEntry) < 0) {
                                leastEntry = entry[i];
                                leastIndex = i;
                            }
                        }
                    }
                }
                if (leastIndex == -1) {
                    break;
                }
                byte[] bytes = options.serializer().serialize(leastEntry);
                out.write(Util.toBytes(bytes.length));
                out.write(bytes);
                long next = positions[leastIndex] + 4 + bytes.length;
                if (next < sizes[leastIndex]) {
                    positions[leastIndex] = next;
                } else {
                    bb[leastIndex].close();
                    positions[leastIndex] = -1;
                }
            }
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return file;
    }

    private static <T> List<T> sortInPlace(List<T> list, Comparator<T> comparator) {
        Collections.sort(list, comparator);
        return list;
    }
}
