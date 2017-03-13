package org.davidmoten.bigsort2;

public class Util {
    public static int intFromBytes(byte[] bytes) {
        return intFromBytes(bytes[0], bytes[1], bytes[2], bytes[3]);
    }

    public static int intFromBytes(byte a, byte b, byte c, byte d) {
        return a << 24 | (b & 0xFF) << 16 | (c & 0xFF) << 8 | (d & 0xFF);
    }

    public static final byte[] intToBytes(int value) {
        return new byte[] { (byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value };
    }
}
