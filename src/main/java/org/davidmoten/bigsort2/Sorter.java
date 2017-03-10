package org.davidmoten.bigsort2;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
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
                .count();
    }

    private File writeToNewFile(List<Entry> list) {
        File file = new File(options.directory(), index.incrementAndGet() + ".sort");
        try (BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(file))) {
            for (Entry entry : list) {
                byte[] bytes = options.serializer().serialize(entry);
                out.write(bytes);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        log.info(""+ list.size() + " entries written to "+ file);
        return file;
    }

    private static <T> List<T> sortInPlace(List<T> list, Comparator<T> comparator) {
        Collections.sort(list, comparator);
        return list;
    }
}
