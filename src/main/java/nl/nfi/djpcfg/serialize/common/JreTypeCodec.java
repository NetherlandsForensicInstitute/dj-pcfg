package nl.nfi.djpcfg.serialize.common;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.function.Function;

import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;
import static nl.nfi.djpcfg.serialize.common.JreTypeCodec.Decoder;
import static nl.nfi.djpcfg.serialize.common.JreTypeCodec.Encoder;

public abstract sealed class JreTypeCodec implements Closeable permits Encoder, Decoder {

    public static Encoder forOutput(final Path path) throws IOException {
        return new Encoder(new BufferedOutputStream(new FileOutputStream(path.toFile())));
    }

    public static Encoder forOutput(final OutputStream output) {
        return new Encoder(output);
    }

    public static Decoder forInput(final Path path) throws FileNotFoundException {
        return new Decoder(new BufferedInputStream(new FileInputStream(path.toFile())));
    }

    public static Decoder forInput(final InputStream input) {
        return new Decoder(input);
    }


    public static final class Encoder extends JreTypeCodec {

        private final OutputStream output;

        Encoder(final OutputStream output) {
            this.output = output;
        }

        public void writeString(final String value) throws IOException {
            final byte[] bytes = value.getBytes(UTF_8);
            writeVarInt(bytes.length);
            output.write(bytes);
        }

        public void writeDouble(final double value) throws IOException {
            writeLong(Double.doubleToLongBits(value));
        }

        public void writeByte(final byte value) throws IOException {
            output.write(value);
        }

        public void writeBytes(final byte[] value) throws IOException {
            writeVarInt(value.length);
            output.write(value);
        }

        public void writeInt(final int value) throws IOException {
            int bits = value;
            for (int shift = 0; shift < 32; shift += 8) {
                output.write(bits & 0xff);
                bits >>>= 8;
            }
        }

        public void writeVarInt(final int value) throws IOException {
            int bits = value;
            while (true) {
                if ((bits & ~0x7f) == 0) {
                    output.write(bits);
                    return;
                }
                output.write((bits & 0x7F | 0x80));
                bits >>>= 7;
            }
        }

        public void writeLong(final long value) throws IOException {
            long bits = value;
            for (long shift = 0; shift < 64; shift += 8) {
                output.write((int) (bits & 0xff));
                bits >>>= 8;
            }
        }

        public void writeVarLong(final long value) throws IOException {
            long bits = value;
            while (true) {
                if ((bits & ~0x7fL) == 0) {
                    output.write(toIntExact(bits));
                    return;
                }
                output.write(toIntExact((bits & 0x7FL | 0x80L)));
                bits >>>= 7;
            }
        }
        public void writeIntArray(final int[] values) throws IOException {
            writeVarInt(values.length);
            for (final int value : values) {
                writeInt(value);
            }
        }

        public void writeVarIntArray(final int[] values) throws IOException {
            writeVarInt(values.length);
            for (final int value : values) {
                writeVarInt(value);
            }
        }

        public <T> void writeCollection(final Collection<T> values, final DelegateWriter<T> valueWriter) throws IOException {
            writeVarInt(values.size());
            for (final T value : values) {
                valueWriter.write(value);
            }
        }

        public <T> void writeList(final List<T> values, final DelegateWriter<T> valueWriter) throws IOException {
            writeVarInt(values.size());
            for (final T value : values) {
                valueWriter.write(value);
            }
        }

        public <K, V> void writeMap(final Map<K, V> values, final DelegateWriter<K> keyWriter, final DelegateWriter<V> valueWriter) throws IOException {
            writeVarInt(values.size());
            for (final Entry<K, V> entry : values.entrySet()) {
                keyWriter.write(entry.getKey());
                valueWriter.write(entry.getValue());
            }
        }

        @Override
        public void close() throws IOException {
            output.close();
        }
    }

    public static final class Decoder extends JreTypeCodec {

        private final InputStream input;

        Decoder(final InputStream input) {
            this.input = input;
        }

        public String readString() throws IOException {
            final int length = readVarInt();
            return new String(input.readNBytes(length), UTF_8);
        }

        public double readDouble() throws IOException {
            return Double.longBitsToDouble(readLong());
        }

        public byte readByte() throws IOException {
            return (byte) readUnsignedByte();
        }

        public byte[] readBytes() throws IOException {
            return input.readNBytes(readVarInt());
        }

        public int readUnsignedByte() throws IOException {
            final int read = input.read();
            if (read == -1) {
                throw new EOFException("End of stream reached");
            }
            return read;
        }

        public int readInt() throws IOException {
            int bits = 0;
            for (int shift = 0; shift < 32; shift += 8) {
                bits |= readUnsignedByte() << shift;
            }
            return bits;
        }

        public int readVarInt() throws IOException {
            int value = 0;
            int shift = 0;

            while (true) {
                final int b = readUnsignedByte();
                value |= (b & 0x7f) << shift;
                if ((b & 0x80) == 0) {
                    return value;
                }
                shift += 7;
            }
        }

        public long readLong() throws IOException {
            long bits = 0;
            for (long shift = 0; shift < 64; shift += 8) {
                bits |= ((long) readUnsignedByte()) << shift;
            }
            return bits;
        }

        public long readVarLong() throws IOException {
            long value = 0;
            long shift = 0;

            while (true) {
                final int b = readUnsignedByte();
                value |= (b & 0x7fL) << shift;
                if ((b & 0x80L) == 0) {
                    return value;
                }
                shift += 7;
            }
        }

        public int[] readIntArray() throws IOException {
            final int size = readVarInt();
            final int[] values = new int[size];
            for (int i = 0; i < size; i++) {
                values[i] = readInt();
            }
            return values;
        }

        public int[] readVarIntArray() throws IOException {
            final int size = readVarInt();
            final int[] values = new int[size];
            for (int i = 0; i < size; i++) {
                values[i] = readVarInt();
            }
            return values;
        }

        public <T> List<T> readList(final DelegateReader<T> delegateReader) throws IOException {
            final int size = readVarInt();
            final List<T> values = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                values.add(delegateReader.read());
            }
            return values;
        }

        public <E, Q extends Queue<E>> Q readQueue(final DelegateReader<E> delegateReader, final Function<Integer, Q> queueFactory) throws IOException {
            final int size = readVarInt();
            final Q values = queueFactory.apply(size);
            for (int i = 0; i < size; i++) {
                values.add(delegateReader.read());
            }
            return values;
        }

        public <K, V> Map<K, V> readMap(final DelegateReader<K> keyReader, final DelegateReader<V> valueReader) throws IOException {
            final int size = readVarInt();
            final Map<K, V> values = new LinkedHashMap<>(size);
            for (int i = 0; i < size; i++) {
                values.put(keyReader.read(), valueReader.read());
            }
            return values;
        }

        @Override
        public void close() throws IOException {
            input.close();
        }
    }
}
