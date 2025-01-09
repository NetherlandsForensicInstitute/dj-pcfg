package nl.nfi.djpcfg.guess.cache.distributed;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import nl.nfi.djpcfg.guess.cache.Checkpoint;
import nl.nfi.djpcfg.guess.cache.CheckpointCache;
import nl.nfi.djpcfg.guess.pcfg.Pcfg;
import nl.nfi.djpcfg.serialize.CheckpointCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Iterator;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static java.io.InputStream.nullInputStream;
import static java.util.concurrent.TimeUnit.MINUTES;
import static nl.nfi.djpcfg.common.HostUtils.hostname;
import static nl.nfi.djpcfg.common.HostUtils.pid;
import static nl.nfi.djpcfg.guess.cache.distributed.CheckpointServiceGrpc.*;
import static nl.nfi.djpcfg.guess.cache.distributed.CheckpointServiceOuterClass.*;
import static nl.nfi.djpcfg.guess.cache.distributed.CheckpointServiceOuterClass.Response.OK;
import static nl.nfi.djpcfg.guess.cache.distributed.Config.CHUNK_SIZE;
import static nl.nfi.djpcfg.guess.cache.distributed.Config.MAX_MESSAGE_SIZE;
import static nl.nfi.djpcfg.guess.cache.distributed.MessageFactory.*;

public final class CheckpointCacheClient implements Closeable, CheckpointCache {

    private static final Logger LOG = LoggerFactory.getLogger(CheckpointCacheClient.class);

    private final String name;
    private final ManagedChannel channel;
    private final CheckpointServiceBlockingStub blockingStub;
    private final CheckpointServiceStub asyncStub;

    public CheckpointCacheClient(final String name, final String serverTarget) {
        this.name = name;
        this.channel = ManagedChannelBuilder
                .forTarget(serverTarget)
                .maxInboundMessageSize(MAX_MESSAGE_SIZE)
                .usePlaintext()
                .build();

        this.blockingStub = newBlockingStub(channel);
        this.asyncStub = newStub(channel);
    }

    public static CheckpointCacheClient connect(final String name, final String serverTarget) {
        return new CheckpointCacheClient(name, serverTarget);
    }

    public static CheckpointCacheClient connect(final String serverTarget) {
        return connect("%s (%d)".formatted(hostname(), pid()), serverTarget);
    }

    public static CheckpointCacheClient connect(final String serverAddress, final int port) {
        return connect(serverAddress + ":" + port);
    }

    public static CheckpointCacheClient connect(final String name, final String serverAddress, final int port) {
        return connect(name, serverAddress + ":" + port);
    }

    public void shutdown() {
        try {
            channel.shutdown().awaitTermination(4, MINUTES);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        shutdown();
    }

    @Override
    public Optional<Checkpoint> getFurthestBefore(final Pcfg pcfg, final UUID grammarUuid, final long keyspacePosition) throws IOException {
        final Iterator<CheckpointPart> parts = blockingStub.getCheckpoint(request(name, grammarUuid, keyspacePosition));

        // stream always starts with response message, check it
        final Response response = parts.next().getResponse();
        if (!response.equals(OK)) {
            LOG.info("No checkpoint received for grammar {} at offset {}, reason: {}", grammarUuid, keyspacePosition, response);
            return Optional.empty();
        }

        // first part is metadata, retrieve it
        final RequestMetadata metadata = parts.next().getMetadata();
        final CheckpointMetadata checkpointMetadata = metadata.getCheckpointMetadata();
        if (!checkpointMetadata.getGrammarUuid().equals(grammarUuid.toString()) || checkpointMetadata.getKeyspaceOffset() > keyspacePosition) {
            throw new RuntimeException("Invalid checkpoint received from server: requested %s_%d, received %s_%d".formatted(
                    checkpointMetadata.getGrammarUuid(), checkpointMetadata.getKeyspaceOffset(), grammarUuid, keyspacePosition
            ));
        }

        // rest are the serialized chunk parts, read and deserialize
        final InputStream input = toInputStream(parts);
        return Optional.of(CheckpointCodec.forInput(input).readCacheUsingBaseRefs(pcfg));
    }

    @Override
    public boolean store(final Pcfg pcfg, final UUID grammarUuid, final long keyspacePosition, final Checkpoint state) throws IOException {
        final AtomicBoolean didStore = new AtomicBoolean(true);
        final CountDownLatch latch = new CountDownLatch(1);

        // TODO: hack
        final AtomicReference<StreamObserver<CheckpointPart>> requestObserver = new AtomicReference<>();
        requestObserver.set(
                asyncStub.storeCheckpoint(new StreamObserver<>() {

                    @Override
                    public void onNext(final StorageResponse storageResponse) {
                        // don't send checkpoint data if not necessary
                        if (!storageResponse.getResponse().equals(OK)) {
                            LOG.debug("Not sending checkpoint to server, reason: {}", storageResponse.getResponse());
                            didStore.set(false);
                            requestObserver.get().onCompleted();
                            return;
                        }

                        // else, send the serialized data in chunks to the server
                        try {
                            try (final BufferedOutputStream output = new BufferedOutputStream(new OutputStream() {

                                @Override
                                public void write(final int b) {
                                    throw new UnsupportedOperationException();
                                }

                                @Override
                                public void write(final byte[] buffer, final int offset, final int length) {
                                    requestObserver.get().onNext(chunkPart(buffer, offset, length));
                                }
                            }, CHUNK_SIZE)) {
                                CheckpointCodec.forOutput(output).writeCacheUsingBaseRefs(pcfg, state);
                            }
                        } catch (final IOException e) {
                            LOG.warn("Failed to send checkpoint {} at offset {}", grammarUuid, keyspacePosition, e);
                            throw new UncheckedIOException(e);
                        } finally {
                            LOG.debug("Finished sending checkpoint {} at offset {}", grammarUuid, keyspacePosition);
                            requestObserver.get().onCompleted();
                        }
                    }

                    @Override
                    public void onError(final Throwable t) {
                        LOG.warn("Error while sending checkpoint", t);
                        didStore.set(false);
                        latch.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        latch.countDown();
                    }
                })
        );

        requestObserver.get().onNext(metadataPart(name, grammarUuid, keyspacePosition));
        try {
            // request for storing a checkpoint, then wait and let async stream run to completion
            // TODO: ensure latch is always counted down
            if (!latch.await(4, MINUTES)) {
                LOG.warn("Storing checkpoint took too long, stopping");
                // TODO: handle in some way
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }

        return didStore.get();
    }

    // TODO: improve/optimize
    private static InputStream toInputStream(final Iterator<CheckpointPart> parts) {
        InputStream head = nullInputStream();
        while (parts.hasNext()) {
            final ByteString data = parts.next().getChunk().getData();
            head = new SequenceInputStream(head, new ByteArrayInputStream(UnsafeByteStringOperations.getBytes(data)));
        }
        return head;
    }
}