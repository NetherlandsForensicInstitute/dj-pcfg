package nl.nfi.djpcfg.common.serialize;

import nl.nfi.djpcfg.guess.cache.CacheIndex;
import nl.nfi.djpcfg.serialize.CacheIndexCodec;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

class CacheIndexCodecTest {

    @Disabled
    @Test
    void hammer() throws IOException {
        final Random random = new Random(0);

        CacheIndex index = CacheIndex.emptyWithSequenceNumber(1000000000000000000L);
        for (int i = 0; i < 1000000000; i++) {
            if (random.nextInt(16) >= 12) {
                index.removeOldestEntry();
                while (random.nextInt(16) >= 10) {
                    index.removeOldestEntry();
                }
            } else {
                final UUID uuid;
                if (random.nextInt(16) < 13 && !index.index().isEmpty()) {
                    final List<UUID> uuids = new ArrayList<>(index.index().keySet());
                    uuid = uuids.get(random.nextInt(uuids.size()));
                } else {
                    uuid = UUID.randomUUID();
                }
                final long offset;
                if (random.nextInt(16) >= 14 && index.getKeyspaceOffsets(uuid) != null) {
                    final List<Long> offsets = new ArrayList<>(index.getKeyspaceOffsets(uuid));
                    offset = offsets.get(random.nextInt(offsets.size()));
                } else {
                    offset = random.nextLong(Long.MAX_VALUE);
                }
                index.update(uuid, offset);
            }
            index = fromBytes(toBytes(index));
        }
    }

    private static byte[] toBytes(final CacheIndex index) throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        CacheIndexCodec.forOutput(out).write(index);
        return out.toByteArray();
    }

    private static CacheIndex fromBytes(final byte[] bytes) throws IOException {
        return CacheIndexCodec.forInput(new ByteArrayInputStream(bytes)).read();
    }
}