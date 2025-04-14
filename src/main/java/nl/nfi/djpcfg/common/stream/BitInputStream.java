package nl.nfi.djpcfg.common.stream;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

public final class BitInputStream implements Closeable {

    private final InputStream input;

    private int bitBuffer;
    private int restCount;

    private BitInputStream(final InputStream input) {
        this.input = input;
    }

    public static BitInputStream forInput(final byte[] input) {
        return forInput(new ByteArrayInputStream(input));
    }

    public static BitInputStream forInput(final InputStream input) {
        return new BitInputStream(input);
    }

    // returns the next bit in the stream, or throws EOF when none available
    public int read() throws IOException {
        if (restCount == 0) {
            final int next = input.read();
            if (next == -1) {
                throw new EOFException();
            }
            bitBuffer = next;
            restCount = 8;
        }
        restCount--;
        return (bitBuffer >>> restCount) & 1;
    }

    @Override
    public void close() throws IOException {
        input.close();
    }
}
