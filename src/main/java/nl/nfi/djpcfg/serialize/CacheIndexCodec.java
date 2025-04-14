package nl.nfi.djpcfg.serialize;

import nl.nfi.djpcfg.guess.cache.CacheIndex;
import nl.nfi.djpcfg.guess.cache.LRUEntry;

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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

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
            encoder.writeString(Config.SERIALIZED_FORMAT_VERSION);

            encoder.writeLong(index.nextSeqNum());

            encoder.writeMap(index.index(),
                    uuid -> encoder.writeString(uuid.toString()),
                    positions -> encoder.writeList(new ArrayList<>(positions), encoder::writeLong)
            );

            encoder.writeList(index.lru(), entry -> {
                encoder.writeString(entry.uuid().toString());
                encoder.writeLong(entry.keyspacePosition());
                encoder.writeLong(entry.sequenceNumber());
            });
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
            if (!version.equals(Config.SERIALIZED_FORMAT_VERSION)) {
                throw new UnsupportedOperationException(version);
            }

            final long nextSeqNum = decoder.readLong();

            final Map<UUID, Set<Long>> idx = decoder.readMap(
                    () -> UUID.fromString(decoder.readString()),
                    () -> new HashSet<>(decoder.readList(decoder::readLong))
            );

            final List<LRUEntry> lru = decoder.readList(
                    () -> new LRUEntry(
                            UUID.fromString(decoder.readString()),
                            decoder.readLong(),
                            decoder.readLong()
                    )
            );
            return CacheIndex.init(nextSeqNum, idx, lru);
        }

        @Override
        public void close() throws IOException {
            decoder.close();
        }
    }
}