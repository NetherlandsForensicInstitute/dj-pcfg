package nl.nfi.djpcfg.guess.cache.distributed;

import nl.nfi.djpcfg.guess.cache.CacheIndex;
import nl.nfi.djpcfg.guess.cache.LRUEntry;
import nl.nfi.djpcfg.serialize.CacheIndexCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.lang.Math.max;
import static java.nio.file.Files.*;

final class CheckpointFileCache {

    private static final Logger LOG = LoggerFactory.getLogger(CheckpointFileCache.class);

    public static long MAX_CACHE_SIZE = 128L * 1024 * 1024 * 1024;

    private final ReadWriteLock lock;
    private final Path cachePath;
    private final Path indexPath;

    // TODO: handle abort of process when writing to cache
    private CheckpointFileCache(final Path cachePath) throws IOException {
        this.lock = new ReentrantReadWriteLock(true);
        this.cachePath = cachePath;
        this.indexPath = cachePath.resolve("cache.idx");

        LOG.debug("Checking cache: {}", cachePath);
        if (!isDirectory(cachePath) || !isRegularFile(indexPath) || size(indexPath) == 0) {
            LOG.debug("Initializing new cache at: {}", cachePath);
            createDirectories(cachePath);
            // TODO: re-init files?
            if (!isRegularFile(indexPath)) {
                createFile(indexPath);
            }
            final CacheIndex index = CacheIndex.emptyWithSequenceNumber(1000000000000000000L);
            try (final OutputStream output = new BufferedOutputStream(new FileOutputStream(indexPath.toFile()))) {
                CacheIndexCodec.forOutput(output).write(index);
            }
        }
    }

    static CheckpointFileCache createOrLoadFrom(final Path cachePath) throws IOException {
        return new CheckpointFileCache(cachePath);
    }

    boolean tryRefresh(final UUID grammmarUuid, final long keyspaceOffset) throws IOException {
        lock.writeLock().lock();
        try {
            final CacheIndex index = loadIndex();
            if (index.update(grammmarUuid, keyspaceOffset)) {
                try (final OutputStream output = new BufferedOutputStream(new FileOutputStream(indexPath.toFile()))) {
                    CacheIndexCodec.forOutput(output).write(index);
                }
                return true;
            }
            return false;
        } finally {
            lock.writeLock().unlock();
        }
    }

    boolean getFurthestBefore(final UUID grammarUuid, final long keyspacePosition, final ReadCheckpointCallback callback) throws IOException {
        lock.readLock().lock();
        try {
            LOG.debug("Trying to load checkpoint from: {}", cachePath);
            final CacheIndex index = loadIndex();

            // no previous cached run found
            if (!index.containsGrammarEntry(grammarUuid)) {
                LOG.debug("No checkpoint for grammar found found: uuid {} at {}", grammarUuid, indexPath);
                return false;
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
                return false;
            }

            final Path path = cachePath.resolve(toStateKey(grammarUuid, closestStatePosition));
            LOG.debug("Loading checkpoint from cache: uuid {}, offset {} at path {}", grammarUuid, closestStatePosition, path);
            try (final InputStream stateInput = new BufferedInputStream(new FileInputStream(path.toFile()))) {
                callback.read(grammarUuid, closestStatePosition, stateInput);
                return true;
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    void store(final UUID grammarUuid, final long keyspacePosition, final Path checkpointFile) throws IOException {
        lock.writeLock().lock();
        try {
            LOG.debug("Updating checkpoint: uuid {}, offset {} at path {}", grammarUuid, keyspacePosition, cachePath);
            final CacheIndex index = loadIndex();
            index.update(grammarUuid, keyspacePosition);
            // store new state (if it did not already exist)
            final Path path = cachePath.resolve(toStateKey(grammarUuid, keyspacePosition));
            LOG.debug("Writing new checkpoint file: uuid {}, offset {} at path {}", grammarUuid, keyspacePosition, path);
            move(checkpointFile, path);
            updateIndexAndClampCacheSize(index, indexPath, cachePath);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void updateIndexAndClampCacheSize(final CacheIndex index, final Path indexPath, final Path cachePath) throws IOException {
        LOG.debug("Updating index after store, at: {}", indexPath);
        try (final OutputStream output = new BufferedOutputStream(new FileOutputStream(indexPath.toFile()))) {
            CacheIndexCodec.forOutput(output).write(index);
        }

        while (sizeOnDisk() > MAX_CACHE_SIZE && index.numberOfCheckpoints() > 1) { // always keep last one
            final LRUEntry entry = index.removeOldestEntry();
            final Path path = cachePath.resolve(toStateKey(entry.uuid(), entry.keyspacePosition()));

            LOG.debug("Updating index before remove, at: {}", indexPath);
            try (final OutputStream output = new BufferedOutputStream(new FileOutputStream(indexPath.toFile()))) {
                CacheIndexCodec.forOutput(output).write(index);
            }

            LOG.debug("Reducing size of cache, removing checkpoint: uuid {}, offset {} at path {}", entry.uuid(), entry.keyspacePosition(), path);
            delete(path);
            LOG.debug("Current index:\nseqnum {}\noffsets {}\nlru {}", index.nextSeqNum(), index.index(), index.lru());
        }
    }

    private long sizeOnDisk() throws IOException {
        try (final var files = list(cachePath)) {
            return files.filter(Files::isRegularFile)
                    .mapToLong(file -> {
                        try {
                            return size(file);
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


    private CacheIndex loadIndex() throws IOException {
        try (final InputStream indexInput = new BufferedInputStream(new FileInputStream(indexPath.toFile()))) {
            return CacheIndexCodec.forInput(indexInput).read();
        }
    }

    interface ReadCheckpointCallback {
        void read(UUID grammarUuid, long keyspaceOffset, InputStream stateInput);
    }
}
