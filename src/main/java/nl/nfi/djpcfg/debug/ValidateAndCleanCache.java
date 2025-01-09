package nl.nfi.djpcfg.debug;

import nl.nfi.djpcfg.common.lock.DirectoryLock;
import nl.nfi.djpcfg.guess.cache.CacheIndex;
import nl.nfi.djpcfg.serialize.CacheIndexCodec;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.UUID;
import java.util.stream.Stream;

// only works with the directory cache
public final class ValidateAndCleanCache {

    public static void main(final String... args) throws IOException {
        final Path cachePath = Paths.get(args[0]);
        final Path cacheIndexPath = cachePath.resolve("cache.idx");

        System.out.println("Locking cache...");
        try (final DirectoryLock _ = DirectoryLock.lock(cachePath)) {

            System.out.println("Loading index...");
            try (final InputStream indexInput = new BufferedInputStream(new FileInputStream(cacheIndexPath.toFile()))) {
                final CacheIndex index = CacheIndexCodec.forInput(indexInput).read();

                System.out.println();
                System.out.printf("State of %s%n".formatted(cacheIndexPath));
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
                System.out.println();

                System.out.println("Checkpoint in index, not in LRU:");
                index.index().forEach((uuid, offsets) -> {
                    offsets.forEach(offset -> {
                        final boolean found = index.lru().stream()
                                .anyMatch(entry -> entry.uuid().equals(uuid) && entry.keyspacePosition() == offset);

                        if (!found) {
                            System.out.printf("  %s %d%n".formatted(uuid, offset));
                        }
                    });
                });
                System.out.println("Checkpoint in LRU, not in index:");
                index.lru().forEach(lruEntry -> {
                    final UUID uuid = lruEntry.uuid();
                    final long offset = lruEntry.keyspacePosition();
                    if (!index.containsGrammarEntry(uuid) || !index.getKeyspaceOffsets(uuid).contains(offset)) {
                        System.out.printf("  %s %d (seqnum %d)%n".formatted(uuid, offset, lruEntry.sequenceNumber()));
                    }
                });

                System.out.println("Checkpoint files in index, not on FS:");
                index.index().forEach((uuid, offsets) -> {
                    offsets.forEach(offset -> {
                        final Path path = cachePath.resolve(toStateKey(uuid, offset));
                        if (!Files.exists(path)) {
                            System.out.printf("  %s %d (file not found: %s)%n".formatted(uuid, offset, path));
                        }
                    });
                });
                System.out.println("Checkpoint files on FS, not in index:");
                final List<Path> toDelete = new ArrayList<>();
                try (final Stream<Path> list = Files.list(cachePath)) {
                    list
                        .filter(Files::isRegularFile)
                        .filter(file -> isCheckpointFile(file))
                        .forEachOrdered(file -> {
                            final String[] parts = file.getFileName().toString().split("_");
                            final UUID uuid = UUID.fromString(parts[0]);
                            final long offset = Long.parseLong(parts[1]);
                            if (!index.containsGrammarEntry(uuid) || !index.getKeyspaceOffsets(uuid).contains(offset)) {
                                System.out.printf("  %s %d (file %s)%n".formatted(uuid, offset, file));
                                toDelete.add(file);
                            }
                        });
                }
                System.out.println("  Delete from filesystem? (yes/no)");
                final String response = new Scanner(System.in).nextLine();
                if (response.equals("yes")) {
                    for (final Path file : toDelete) {
                        Files.delete(file);
                    }
                }
            }
        }
    }

    private static String toStateKey(final UUID uuid, final long keyspacePosition) {
        return "%s_%d".formatted(uuid, keyspacePosition);
    }

    private static boolean isCheckpointFile(final Path path) {
        final String fileName = path.getFileName().toString();
        // just a hack
        return fileName.length() > 32;
    }
}
