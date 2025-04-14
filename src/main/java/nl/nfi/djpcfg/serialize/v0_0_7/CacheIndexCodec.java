package nl.nfi.djpcfg.serialize.v0_0_7;

import nl.nfi.djpcfg.guess.cache.CacheIndex;
import nl.nfi.djpcfg.guess.cache.LRUEntry;
import nl.nfi.djpcfg.serialize.common.JreTypeCodec;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static nl.nfi.djpcfg.serialize.v0_0_7.CacheIndexCodec.Decoder;
import static nl.nfi.djpcfg.serialize.v0_0_7.CacheIndexCodec.Encoder;

public abstract sealed class CacheIndexCodec implements Closeable permits Encoder, Decoder {

    public static final String VERSION = "0.0.7";

    public static Encoder encodeUsing(final JreTypeCodec.Encoder encoder) {
        return new Encoder(encoder);
    }

    public static Decoder decodeUsing(final JreTypeCodec.Decoder decoder) {
        return new Decoder(decoder);
    }

    public static final class Encoder extends CacheIndexCodec {

        private final JreTypeCodec.Encoder encoder;

        private Encoder(final JreTypeCodec.Encoder encoder) {
            this.encoder = encoder;
        }

        public void write(final CacheIndex index) throws IOException {
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