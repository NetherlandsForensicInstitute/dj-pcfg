package nl.nfi.djpcfg.guess.cache.distributed;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
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
import static nl.nfi.djpcfg.guess.cache.distributed.CheckpointServiceOuterClass.CheckpointMetadata;
import static nl.nfi.djpcfg.guess.cache.distributed.CheckpointServiceOuterClass.CheckpointRequest;
import static nl.nfi.djpcfg.guess.cache.distributed.CheckpointServiceOuterClass.Response.ALREADY_EXISTS;
import static nl.nfi.djpcfg.guess.cache.distributed.CheckpointServiceOuterClass.Response.IN_USE;
import static nl.nfi.djpcfg.guess.cache.distributed.CheckpointServiceOuterClass.Response.NOT_FOUND;
import static nl.nfi.djpcfg.guess.cache.distributed.CheckpointServiceOuterClass.Response.OK;
import static nl.nfi.djpcfg.guess.cache.distributed.CheckpointServiceOuterClass.StorageResponse;
import static nl.nfi.djpcfg.guess.cache.distributed.CheckpointServiceOuterClass.StreamMessage;
import static nl.nfi.djpcfg.guess.cache.distributed.Config.CHUNK_SIZE;
import static nl.nfi.djpcfg.guess.cache.distributed.Config.MAX_MESSAGE_SIZE;
import static nl.nfi.djpcfg.guess.cache.distributed.MessageFactory.chunkMessage;
import static nl.nfi.djpcfg.guess.cache.distributed.MessageFactory.metadataMessage;
import static nl.nfi.djpcfg.guess.cache.distributed.MessageFactory.response;
import static nl.nfi.djpcfg.guess.cache.distributed.MessageFactory.responseMessage;
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
        public StreamObserver<StreamMessage> storeCheckpoint(final StreamObserver<StorageResponse> responseObserver) {
            return new StreamObserver<>() {

                private volatile String clientName;
                private volatile String checkpointKey;
                private volatile CheckpointInfo checkpointInfo;
                private volatile OutputStream checkpointOutput;

                @Override
                public void onNext(final StreamMessage message) {
                    if (message.hasRequest()) {
                        final CheckpointRequest request = message.getRequest();
                        clientName = request.getSenderName();

                        final CheckpointMetadata metadata = request.getCheckpointMetadata();
                        final UUID grammmarUuid = UUID.fromString(metadata.getGrammarUuid());
                        final long keyspaceOffset = metadata.getKeyspaceOffset();

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
                            checkpointOutput.write(getBytes(message.getChunk().getData()));
                        } catch (final IOException e) {
                            // TODO: is this necessary?
                            checkpointsBeingProcessed.remove(checkpointKey);
                            throw new UncheckedIOException(e);
                        }
                    }
                }

                @Override
                public void onError(final Throwable t) {
                    LOG.warn("Error while receiving checkpoint data from client: {}", clientName, t);
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
        public void getCheckpoint(final CheckpointRequest request, final StreamObserver<StreamMessage> responseObserver) {
            try {
                final CheckpointMetadata metadata = request.getCheckpointMetadata();

                final UUID grammarUuid = UUID.fromString(metadata.getGrammarUuid());
                final long keyspaceOffset = metadata.getKeyspaceOffset();

                // search for checkpoint, if so, execute the callback, streaming the data to the client
                final boolean foundCheckpoint = cache.getFurthestBefore(grammarUuid, keyspaceOffset, (uuid, closestStatePosition, stateInput) -> {
                    try {
                        responseObserver.onNext(responseMessage(OK));
                        responseObserver.onNext(metadataMessage(uuid, closestStatePosition));
                        final byte[] chunk = new byte[CHUNK_SIZE];
                        int read;
                        while ((read = stateInput.read(chunk)) > 0) {
                            LOG.debug("Sending chunk to {}", request.getSenderName());
                            responseObserver.onNext(chunkMessage(chunk, read));
                        }
                        LOG.debug("Done sending chunks to {}", request.getSenderName());
                        responseObserver.onCompleted();
                    } catch (final IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });

                // if no applicable checkpoint found, callback is not executed, and we just send a 'not found'
                if (!foundCheckpoint) {
                    responseObserver.onNext(responseMessage(NOT_FOUND));
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
