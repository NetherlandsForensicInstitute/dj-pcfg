package nl.nfi.djpcfg.guess.cache.distributed;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Path;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import static java.lang.Runtime.getRuntime;
import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.exists;
import static java.util.Collections.newSetFromMap;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.MINUTES;
import static nl.nfi.djpcfg.guess.cache.distributed.CheckpointServiceOuterClass.*;
import static nl.nfi.djpcfg.guess.cache.distributed.CheckpointServiceOuterClass.Response.*;
import static nl.nfi.djpcfg.guess.cache.distributed.Config.CHUNK_SIZE;
import static nl.nfi.djpcfg.guess.cache.distributed.Config.MAX_MESSAGE_SIZE;
import static nl.nfi.djpcfg.guess.cache.distributed.MessageFactory.*;
import static nl.nfi.djpcfg.guess.cache.distributed.UnsafeByteStringOperations.getBytes;

public final class CheckpointCacheServer implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(CheckpointCacheServer.class);

    private final Server server;

    private CheckpointCacheServer(final int port, final Path checkpointDirectory, final ExecutorService executorService) throws IOException {
        final CheckpointFileCache cache = CheckpointFileCache.createOrLoadFrom(checkpointDirectory);
        final Path workingDirectory = checkpointDirectory.resolve("tmp");
        if (!exists(workingDirectory)) {
            createDirectories(workingDirectory);
        }

        this.server = ServerBuilder.forPort(port)
                .addService(new CheckpointService(workingDirectory, cache))
                .maxInboundMessageSize(MAX_MESSAGE_SIZE)
                .executor(executorService) // TODO: improve
                .build();

        server.start();
        LOG.info("Server started, listening on {}", port);
    }

    public static CheckpointCacheServer start(final int port, final Path checkpointDirectory) throws IOException {
        return start(port, checkpointDirectory, newFixedThreadPool(getRuntime().availableProcessors()));
    }

    public static CheckpointCacheServer start(final int port, final Path checkpointDirectory, final ExecutorService executorService) throws IOException {
        return new CheckpointCacheServer(port, checkpointDirectory, executorService);
    }

    public void shutdown() {
        try {
            server.shutdown().awaitTermination(1, MINUTES);
            LOG.info("Server stopped");
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        shutdown();
    }

    private static final class CheckpointService extends CheckpointServiceGrpc.CheckpointServiceImplBase {

        private final Path workingDirectory;
        private final CheckpointFileCache cache;

        private final Set<String> checkpointsBeingProcessed = newSetFromMap(new ConcurrentHashMap<>());

        private CheckpointService(final Path workingDirectory, final CheckpointFileCache cache) {
            this.workingDirectory = workingDirectory;
            this.cache = cache;
        }

        @Override
        public StreamObserver<CheckpointPart> storeCheckpoint(final StreamObserver<StorageResponse> responseObserver) {
            return new StreamObserver<>() {

                private volatile String clientName;
                private volatile String checkpointKey;
                private volatile CheckpointInfo checkpointInfo;
                private volatile OutputStream checkpointOutput;

                @Override
                public void onNext(final CheckpointPart part) {
                    if (part.hasMetadata()) {
                        final RequestMetadata metadata = part.getMetadata();
                        clientName = metadata.getSenderName();

                        final CheckpointMetadata checkpointMetadata = metadata.getCheckpointMetadata();
                        final UUID grammmarUuid = UUID.fromString(checkpointMetadata.getGrammarUuid());
                        final long keyspaceOffset = checkpointMetadata.getKeyspaceOffset();

                        final String key = "%s_%d".formatted(grammmarUuid, keyspaceOffset);

                        // check if we are already receiving the checkpoint data
                        if (!checkpointsBeingProcessed.add(key)) {
                            responseObserver.onNext(response(IN_USE));
                            responseObserver.onCompleted();
                            return;
                        }

                        try {
                            // check if we already have the checkpoint stored locally (and update LRU)
                            if (cache.tryRefresh(grammmarUuid, keyspaceOffset)) {
                                checkpointsBeingProcessed.remove(key);
                                responseObserver.onNext(response(ALREADY_EXISTS));
                                responseObserver.onCompleted();
                                return;
                            }

                            final Path checkpointFile = workingDirectory.resolve(key);

                            // open stream to file where we will write the data chunks to
                            checkpointKey = key;
                            checkpointInfo = new CheckpointInfo(grammmarUuid, keyspaceOffset, checkpointFile);
                            checkpointOutput = new BufferedOutputStream(new FileOutputStream(checkpointFile.toFile()));

                            // signal we are ready to receive the data chunks
                            responseObserver.onNext(response(OK));
                        } catch (final IOException e) {
                            // TODO: is this necessary?
                            checkpointsBeingProcessed.remove(key);
                            throw new UncheckedIOException(e);
                        }
                    } else {
                        try {
                            // write next chunk to checkpoint file
                            LOG.debug("Received chunk from {}", clientName);
                            checkpointOutput.write(getBytes(part.getChunk().getData()));
                        } catch (final IOException e) {
                            // TODO: is this necessary?
                            checkpointsBeingProcessed.remove(checkpointKey);
                            throw new UncheckedIOException(e);
                        }
                    }
                }

                @Override
                public void onError(final Throwable t) {
                    LOG.warn("Error while receiving checkpoint data", t);
                    try {
                        if (checkpointOutput != null) {
                            checkpointOutput.close();
                        }
                    } catch (final IOException e) {
                        LOG.warn("Error while closing checkpoint output", t);
                        throw new UncheckedIOException(e);
                    } finally {
                        checkpointsBeingProcessed.remove(checkpointKey);
                    }
                }

                @Override
                public void onCompleted() {
                    try {
                        if (checkpointOutput != null) {
                            LOG.debug("Finished receiving checkpoint data, adding to cache index...");
                            checkpointOutput.close();
                            cache.store(checkpointInfo.grammarUuid(), checkpointInfo.keyspaceOffset(), checkpointInfo.path());
                            LOG.debug("Added checkpoint {} at offset {} to cache", checkpointInfo.grammarUuid, checkpointInfo.keyspaceOffset);
                        }
                    } catch (final IOException e) {
                        LOG.warn("Failed to store checkpoint", e);
                        throw new UncheckedIOException(e);
                    } finally {
                        checkpointsBeingProcessed.remove(checkpointKey);
                        responseObserver.onCompleted();
                    }
                }
            };
        }

        @Override
        public void getCheckpoint(final RequestMetadata request, final StreamObserver<CheckpointPart> responseObserver) {
            try {
                final CheckpointMetadata checkpointMetadata = request.getCheckpointMetadata();

                final UUID grammarUuid = UUID.fromString(checkpointMetadata.getGrammarUuid());
                final long keyspaceOffset = checkpointMetadata.getKeyspaceOffset();

                // search for checkpoint, if so, execute the callback, streaming the data to the client
                final boolean foundCheckpoint = cache.getFurthestBefore(grammarUuid, keyspaceOffset, (uuid, closestStatePosition, stateInput) -> {
                    try {
                        responseObserver.onNext(responsePart(OK));
                        responseObserver.onNext(metadataPart(uuid, closestStatePosition));
                        final byte[] chunk = new byte[CHUNK_SIZE];
                        int read;
                        while ((read = stateInput.read(chunk)) > 0) {
                            LOG.debug("Sending chunk to {}", request.getSenderName());
                            responseObserver.onNext(chunkPart(chunk, read));
                        }
                        LOG.debug("Done sending chunks to {}", request.getSenderName());
                        responseObserver.onCompleted();
                    } catch (final IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });

                // if no applicable checkpoint found, callback is not executed, and we just send a 'not found'
                if (!foundCheckpoint) {
                    responseObserver.onNext(responsePart(NOT_FOUND));
                    responseObserver.onCompleted();
                }
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    record CheckpointInfo(UUID grammarUuid, long keyspaceOffset, Path path) {
    }
}
