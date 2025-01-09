package nl.nfi.djpcfg.guess.cache.directory;

import nl.nfi.djpcfg.common.lock.DirectoryLock;
import nl.nfi.djpcfg.guess.cache.CacheIndex;
import nl.nfi.djpcfg.guess.cache.Checkpoint;
import nl.nfi.djpcfg.guess.cache.CheckpointCache;
import nl.nfi.djpcfg.guess.cache.LRUEntry;
import nl.nfi.djpcfg.guess.pcfg.Pcfg;
import nl.nfi.djpcfg.serialize.CacheIndexCodec;
import nl.nfi.djpcfg.serialize.CheckpointCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.UUID;

import static java.lang.Math.max;
import static java.nio.file.Files.*;

public final class DirectoryCheckpointCache implements CheckpointCache {

    private static final Logger LOG = LoggerFactory.getLogger(DirectoryCheckpointCache.class);

    public static long MAX_CACHE_SIZE = 128L * 1024 * 1024 * 1024;

    private final Path cachePath;
    private final Path indexPath;

    // TODO: handle abort of process when writing to cache
    private DirectoryCheckpointCache(final Path cachePath) throws IOException {
        this.cachePath = cachePath;
        this.indexPath = cachePath.resolve("cache.idx");

        try (final DirectoryLock _ = DirectoryLock.lock(cachePath)) {
            LOG.debug("Checking cache: {}", cachePath);
            if (!isDirectory(cachePath) || !isRegularFile(indexPath) || Files.size(indexPath) == 0) {
                LOG.debug("Initializing new cache at: {}", cachePath);
                createDirectories(cachePath);
                // TODO: re-init files?
                if (!Files.isRegularFile(indexPath)) {
                    createFile(indexPath);
                }
                final CacheIndex index = CacheIndex.emptyWithSequenceNumber(1000000000000000000L);
                try (final OutputStream output = new BufferedOutputStream(new FileOutputStream(indexPath.toFile()))) {
                    CacheIndexCodec.forOutput(output).write(index);
                }
            }
        }
    }

    public static DirectoryCheckpointCache createOrLoadFrom(final Path cachePath) throws IOException {
        return new DirectoryCheckpointCache(cachePath);
    }

    @Override
    public Optional<Checkpoint> getFurthestBefore(final Pcfg pcfg, final UUID grammarUuid, final long keyspacePosition) throws IOException {
        try (final DirectoryLock _ = DirectoryLock.lock(cachePath)) {
            LOG.debug("Trying to load checkpoint from: {}", cachePath);
            try (final InputStream indexInput = new BufferedInputStream(new FileInputStream(indexPath.toFile()))) {
                final CacheIndex index = CacheIndexCodec.forInput(indexInput).read();

                // no previous cached run found
                if (!index.containsGrammarEntry(grammarUuid)) {
                    LOG.debug("No checkpoint for grammar found found: uuid {} at {}", grammarUuid, indexPath);
                    return Optional.empty();
                }

                // try to find a cached state which stopped somewhere before
                // where we want to continue now
                long closestStatePosition = -1;
                for (final long statePosition : index.getKeyspaceOffsets(grammarUuid)) {
                    if (statePosition <= keyspacePosition) {
                        closestStatePosition = max(closestStatePosition, statePosition);
                    }
                }

                // all cached states have advanced too far in the guess space
                if (closestStatePosition == -1) {
                    LOG.debug("No previous checkpoint for grammar found: uuid {}, offset {} at {}", grammarUuid, keyspacePosition, indexPath);
                    return Optional.empty();
                }

                final Path path = cachePath.resolve(toStateKey(grammarUuid, closestStatePosition));
                LOG.debug("Loading checkpoint from cache: uuid {}, offset {} at path {}", grammarUuid, closestStatePosition, path);
                try (final InputStream stateInput = new BufferedInputStream(new FileInputStream(path.toFile()))) {
                    return Optional.of(CheckpointCodec.forInput(stateInput).readCacheUsingBaseRefs(pcfg));
                }
            }
        }
    }

    @Override
    public boolean store(final Pcfg pcfg, final UUID grammarUuid, final long keyspacePosition, final Checkpoint state) throws IOException {
        try (final DirectoryLock _ = DirectoryLock.lock(cachePath)) {
            LOG.debug("Updating checkpoint: uuid {}, offset {} at path {}", grammarUuid, keyspacePosition, cachePath);
            try (final InputStream indexInput = new BufferedInputStream(new FileInputStream(indexPath.toFile()))) {
                final CacheIndex index = CacheIndexCodec.forInput(indexInput).read();
                final boolean stateExists = index.update(grammarUuid, keyspacePosition);
                // store new state (if it did not already exist)
                if (!stateExists) {
                    final Path path = cachePath.resolve(toStateKey(grammarUuid, keyspacePosition));
                    LOG.debug("Writing new checkpoint file: uuid {}, offset {} at path {}", grammarUuid, keyspacePosition, path);
                    try (final OutputStream stateOutput = new BufferedOutputStream(new FileOutputStream(path.toFile()))) {
                        CheckpointCodec.forOutput(stateOutput).writeCacheUsingBaseRefs(pcfg, state);
                    }
                }

                LOG.debug("Updating index after store, at: {}", indexPath);
                try (final OutputStream output = new BufferedOutputStream(new FileOutputStream(indexPath.toFile()))) {
                    CacheIndexCodec.forOutput(output).write(index);
                }

                while (sizeOnDisk() > MAX_CACHE_SIZE && index.numberOfCheckpoints() > 1) { // always keep last one
                    final LRUEntry entry = index.removeOldestEntry();
                    final Path path = cachePath.resolve(toStateKey(entry.uuid(), entry.keyspacePosition()));

                    // update index each time t
                    LOG.debug("Updating index before remove, at: {}", indexPath);
                    try (final OutputStream output = new BufferedOutputStream(new FileOutputStream(indexPath.toFile()))) {
                        CacheIndexCodec.forOutput(output).write(index);
                    }

                    LOG.debug("Reducing size of cache, removing checkpoint: uuid {}, offset {} at path {}", entry.uuid(), entry.keyspacePosition(), path);
                    Files.delete(path);
                    LOG.debug("Current index:\nseqnum {}\noffsets {}\nlru {}", index.nextSeqNum(), index.index(), index.lru());
                }
                return !stateExists;
            }
        }
    }

    private long sizeOnDisk() throws IOException {
        try (final var files = Files.list(cachePath)) {
            return files.filter(Files::isRegularFile)
                    .mapToLong(file -> {
                        try {
                            return Files.size(file);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .sum();
        }
    }

    private String toStateKey(final UUID uuid, final long keyspacePosition) {
        return "%s_%d".formatted(uuid, keyspacePosition);
    }
}
