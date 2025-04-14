package nl.nfi.djpcfg.serialize;

import nl.nfi.djpcfg.guess.cache.CacheIndex;
import nl.nfi.djpcfg.serialize.common.JreTypeCodec;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;

import static nl.nfi.djpcfg.serialize.CacheIndexCodec.Decoder;
import static nl.nfi.djpcfg.serialize.CacheIndexCodec.Encoder;

public abstract sealed class CacheIndexCodec implements Closeable permits Encoder, Decoder {

    private static final String MAGIC = "pbi";

    public static Encoder forOutput(final Path path) throws IOException {
        return new Encoder(JreTypeCodec.forOutput(new BufferedOutputStream(new FileOutputStream(path.toFile()))));
    }

    public static Encoder forOutput(final OutputStream output) {
        return new Encoder(JreTypeCodec.forOutput(output));
    }

    public static Decoder forInput(final Path path) throws FileNotFoundException {
        return new Decoder(JreTypeCodec.forInput(new BufferedInputStream(new FileInputStream(path.toFile()))));
    }

    public static Decoder forInput(final InputStream input) {
        return new Decoder(JreTypeCodec.forInput(input));
    }

    public static final class Encoder extends CacheIndexCodec {

        private final JreTypeCodec.Encoder encoder;

        private Encoder(final JreTypeCodec.Encoder encoder) {
            this.encoder = encoder;
        }

        public void write(final CacheIndex index) throws IOException {
            encoder.writeString(MAGIC);
            encoder.writeString(nl.nfi.djpcfg.serialize.v0_0_7.CacheIndexCodec.VERSION);

            nl.nfi.djpcfg.serialize.v0_0_7.CacheIndexCodec.encodeUsing(encoder).write(index);
        }

        @Override
        public void close() throws IOException {
            encoder.close();
        }
    }

    public static final class Decoder extends CacheIndexCodec {

        private final JreTypeCodec.Decoder decoder;

        private Decoder(final JreTypeCodec.Decoder decoder) {
            this.decoder = decoder;
        }

        public CacheIndex read() throws IOException {
            final String magic = decoder.readString();
            if (!magic.equals(MAGIC)) {
                throw new UnsupportedOperationException(magic);
            }

            final String version = decoder.readString();
            if (!version.equals(nl.nfi.djpcfg.serialize.v0_0_7.CacheIndexCodec.VERSION)) {
                throw new UnsupportedOperationException(version);
            }

            return nl.nfi.djpcfg.serialize.v0_0_7.CacheIndexCodec.decodeUsing(decoder).read();
        }

        @Override
        public void close() throws IOException {
            decoder.close();
        }
    }
}