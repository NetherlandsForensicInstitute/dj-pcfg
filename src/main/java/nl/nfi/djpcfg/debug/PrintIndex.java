package nl.nfi.djpcfg.debug;

import nl.nfi.djpcfg.guess.cache.CacheIndex;
import nl.nfi.djpcfg.serialize.CacheIndexCodec;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public final class PrintIndex {

    public static void main(final String... args) throws IOException {
        final Path cacheIndexPath = Paths.get("...");
        final CacheIndex index = CacheIndexCodec.forInput(new FileInputStream(cacheIndexPath.toFile())).read();

        System.out.printf("%s%n", cacheIndexPath);
        System.out.printf("  Sequence number: %d%n", index.nextSeqNum());

        System.out.printf("  Index:%n");
        index.index().forEach((uuid, offsets) -> {
            System.out.printf("    Rule: %s%n", uuid);
            System.out.printf("    Offsets: %n");
            offsets.stream().mapToLong(x -> x).sorted().forEachOrdered(offset -> {
                System.out.printf("      %d%n", offset);
            });
        });

        System.out.printf("  LRU:%n");
        index.lru().forEach(lruEntry -> {
            System.out.printf("    %s %d %d%n", lruEntry.uuid(), lruEntry.sequenceNumber(), lruEntry.keyspacePosition());
        });
    }
}
