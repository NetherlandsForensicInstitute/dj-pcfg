package nl.nfi.djpcfg.serialize;

import nl.nfi.djpcfg.guess.pcfg.Pcfg;
import nl.nfi.djpcfg.serialize.common.JreTypeCodec;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;

import static nl.nfi.djpcfg.serialize.PcfgCodec.Decoder;
import static nl.nfi.djpcfg.serialize.PcfgCodec.Encoder;

public abstract sealed class PcfgCodec implements Closeable permits Encoder, Decoder {

    private static final String MAGIC = "pbm";

    public static Encoder forOutput(final OutputStream output) {
        return new Encoder(JreTypeCodec.forOutput(output));
    }

    public static Decoder forInput(final Path path) throws IOException {
        return new Decoder(JreTypeCodec.forInput(new BufferedInputStream(new FileInputStream(path.toFile()))));
    }

    public static Decoder forInput(final InputStream input) {
        return new Decoder(JreTypeCodec.forInput(input));
    }

    public static final class Encoder extends PcfgCodec {

        private final JreTypeCodec.Encoder encoder;

        private Encoder(final JreTypeCodec.Encoder encoder) {
            this.encoder = encoder;
        }

        public void write(final Pcfg pcfg) throws IOException {
            encoder.writeString(MAGIC);
            encoder.writeString(nl.nfi.djpcfg.serialize.v0_0_7.PcfgCodec.VERSION);

            nl.nfi.djpcfg.serialize.v0_0_7.PcfgCodec.encodeUsing(encoder).write(pcfg);
        }

        @Override
        public void close() throws IOException {
            encoder.close();
        }
    }

    public static final class Decoder extends PcfgCodec {

        private final JreTypeCodec.Decoder decoder;

        private Decoder(final JreTypeCodec.Decoder decoder) {
            this.decoder = decoder;
        }

        public Pcfg read() throws IOException {
            final String magic = decoder.readString();
            if (!magic.equals(MAGIC)) {
                throw new UnsupportedOperationException(magic);
            }

            final String version = decoder.readString();
            if (!version.equals(nl.nfi.djpcfg.serialize.v0_0_7.PcfgCodec.VERSION)) {
                throw new UnsupportedOperationException(version);
            }

            return nl.nfi.djpcfg.serialize.v0_0_7.PcfgCodec.decodeUsing(decoder).read();
        }

        @Override
        public void close() throws IOException {
            decoder.close();
        }
    }
}
