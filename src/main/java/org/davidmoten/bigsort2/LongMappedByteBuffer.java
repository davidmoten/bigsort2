package org.davidmoten.bigsort2;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

public final class LongMappedByteBuffer implements Closeable {

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
        if (pos < start || pos >= start + size) {
            start = pos;
            updateMM();
        }
        mm.position((int) (pos - start));
    }

    public int readInt() {
        checkPosition(4);
        return mm.getInt();
    }

    private void checkPosition(int numBytesToRead) {
        if (start + mm.position() + numBytesToRead > size) {
            start += mm.position();
            updateMM();
        }
    }

    public void get(byte[] bytes) {
        checkPosition(bytes.length);
        mm.get(bytes);
    }

    @Override
    public void close() throws IOException {
        mm.force();
        r.close();
    }

}
