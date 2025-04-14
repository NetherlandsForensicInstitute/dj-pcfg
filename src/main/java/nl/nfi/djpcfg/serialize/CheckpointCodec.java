package nl.nfi.djpcfg.serialize;

import nl.nfi.djpcfg.guess.cache.Checkpoint;
import nl.nfi.djpcfg.guess.pcfg.Pcfg;
import nl.nfi.djpcfg.serialize.common.JreTypeCodec;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;

import static nl.nfi.djpcfg.serialize.CheckpointCodec.Decoder;
import static nl.nfi.djpcfg.serialize.CheckpointCodec.Encoder;

public abstract sealed class CheckpointCodec implements Closeable permits Encoder, Decoder {

    private static final String MAGIC = "pbc";

    public static Encoder forOutput(final Path path) throws IOException {
        return new Encoder(JreTypeCodec.forOutput(new BufferedOutputStream(new FileOutputStream(path.toFile()))));
    }

    public static Encoder forOutput(final OutputStream output) {
        return new Encoder(JreTypeCodec.forOutput(output));
    }

    public static Decoder forInput(final Path path) throws IOException {
        return new Decoder(JreTypeCodec.forInput(new BufferedInputStream(new FileInputStream(path.toFile()))));
    }

    public static Decoder forInput(final InputStream input) {
        return new Decoder(JreTypeCodec.forInput(input));
    }

    public static final class Encoder extends CheckpointCodec {

        private final JreTypeCodec.Encoder encoder;

        private Encoder(final JreTypeCodec.Encoder encoder) {
            this.encoder = encoder;
        }

        public void writeCheckpointUsingBaseRefs(final Pcfg pcfg, final Checkpoint state) throws IOException {
            encoder.writeString(MAGIC);
            encoder.writeString(nl.nfi.djpcfg.serialize.v0_4_0.CheckpointCodec.VERSION);

            nl.nfi.djpcfg.serialize.v0_4_0.CheckpointCodec.encodeUsing(encoder).writeCheckpointUsingBaseRefs(pcfg, state);
        }

        @Override
        public void close() throws IOException {
            encoder.close();
        }
    }

    public static final class Decoder extends CheckpointCodec {

        private final JreTypeCodec.Decoder decoder;

        private Decoder(final JreTypeCodec.Decoder decoder) {
            this.decoder = decoder;
        }

        public Checkpoint readCheckpointUsingBaseRefs(final Pcfg pcfg) throws IOException {
            final String magic = decoder.readString();
            if (!magic.equals(MAGIC)) {
                throw new UnsupportedOperationException(magic);
            }

            final String version = decoder.readString();
            if (version.equals(nl.nfi.djpcfg.serialize.v0_0_7.CheckpointCodec.VERSION)) {
                return nl.nfi.djpcfg.serialize.v0_0_7.CheckpointCodec.decodeUsing(decoder).readCheckpointUsingBaseRefs(pcfg);
            }
            if (version.equals(nl.nfi.djpcfg.serialize.v0_4_0.CheckpointCodec.VERSION)) {
                return nl.nfi.djpcfg.serialize.v0_4_0.CheckpointCodec.decodeUsing(decoder).readCheckpointUsingBaseRefs(pcfg);
            }
            throw new UnsupportedOperationException(version);
        }

        @Override
        public void close() throws IOException {
            decoder.close();
        }
    }
}