package nl.nfi.djpcfg.common.stream;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

import static java.lang.Math.min;

public final class BitOutputStream implements Closeable {

    private final OutputStream output;

    private int bitBuffer;
    private int usedCount;

    private BitOutputStream(final OutputStream output) {
        this.output = output;
    }

    public static BitOutputStream forOutput(final OutputStream output) {
        return new BitOutputStream(output);
    }

    public void write(final int bits, final int count) throws IOException {
        int remaining = count;
        while (remaining > 0) {
            final int toWrite = min(8 - usedCount, remaining);
            bitBuffer <<= toWrite;
            bitBuffer |= ((bits >>> (remaining - toWrite)) & ((1 << toWrite) - 1));
            usedCount += toWrite;
            remaining -= toWrite;

            if (usedCount == 8) {
                flushBits();
            }
        }
    }

    public void write(final long bits, final int count) throws IOException {
        int remaining = count;
        while (remaining > 0) {
            final int toWrite = min(8 - usedCount, remaining);
            bitBuffer <<= toWrite;
            bitBuffer |= (int) ((bits >>> (remaining - toWrite)) & ((1 << toWrite) - 1));
            usedCount += toWrite;
            remaining -= toWrite;

            if (usedCount == 8) {
                flushBits();
            }
        }
    }

    private void flushBits() throws IOException {
        if (usedCount > 0) {
            output.write(bitBuffer << (8 - usedCount));
            bitBuffer = 0;
            usedCount = 0;
        }
    }

    @Override
    public void close() throws IOException {
        flushBits();
        output.close();
    }
}
