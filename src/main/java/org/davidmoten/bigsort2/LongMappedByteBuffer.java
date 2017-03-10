package org.davidmoten.bigsort2;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

public class LongMappedByteBuffer implements Closeable {

    private static final long MARGIN = 1000000;

    private int MAX_SIZE = 1024 * 1024 * 100;

    private final FileChannel c;

    // mutable
    private MappedByteBuffer mm;
    private long start;
    private long size;
    private RandomAccessFile r;

    public LongMappedByteBuffer(File file) {
        try {
            r = new RandomAccessFile(file, "r");
            c = r.getChannel();
            start = 0;
            updateMM();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void updateMM() {
        try {
            size = Math.min(MAX_SIZE, c.size() - start);
            mm = c.map(MapMode.READ_ONLY, start, size);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void position(long pos) {
        if (pos < start || pos >= start + size - MARGIN) {
            start = pos;
            updateMM();
        }
        mm.position((int) (pos - start));
    }
    
    public int readInt() {
        return mm.getInt();
    }
    
    public void get(byte[] bytes) {
        mm.get(bytes);
    }

    @Override
    public void close() throws IOException {
        r.close();
    }

}
