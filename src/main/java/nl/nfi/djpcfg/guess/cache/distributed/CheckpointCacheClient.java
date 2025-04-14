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

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.io.UncheckedIOException;
import java.time.Duration;
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
import static nl.nfi.djpcfg.common.Timers.time;
import static nl.nfi.djpcfg.guess.cache.distributed.CheckpointServiceGrpc.CheckpointServiceBlockingStub;
import static nl.nfi.djpcfg.guess.cache.distributed.CheckpointServiceGrpc.CheckpointServiceStub;
import static nl.nfi.djpcfg.guess.cache.distributed.CheckpointServiceGrpc.newBlockingStub;
import static nl.nfi.djpcfg.guess.cache.distributed.CheckpointServiceGrpc.newStub;
import static nl.nfi.djpcfg.guess.cache.distributed.CheckpointServiceOuterClass.CheckpointMetadata;
import static nl.nfi.djpcfg.guess.cache.distributed.CheckpointServiceOuterClass.CheckpointRequest;
import static nl.nfi.djpcfg.guess.cache.distributed.CheckpointServiceOuterClass.Response;
import static nl.nfi.djpcfg.guess.cache.distributed.CheckpointServiceOuterClass.Response.OK;
import static nl.nfi.djpcfg.guess.cache.distributed.CheckpointServiceOuterClass.StorageResponse;
import static nl.nfi.djpcfg.guess.cache.distributed.CheckpointServiceOuterClass.StreamMessage;
import static nl.nfi.djpcfg.guess.cache.distributed.Config.CHUNK_SIZE;
import static nl.nfi.djpcfg.guess.cache.distributed.Config.MAX_MESSAGE_SIZE;
import static nl.nfi.djpcfg.guess.cache.distributed.MessageFactory.chunkMessage;
import static nl.nfi.djpcfg.guess.cache.distributed.MessageFactory.metadataMessage;
import static nl.nfi.djpcfg.guess.cache.distributed.MessageFactory.request;

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
        final Iterator<StreamMessage> messages = blockingStub.getCheckpoint(request(name, grammarUuid, keyspacePosition));

        LOG.debug("Requesting checkpoint for grammar {} at offset {}", grammarUuid, keyspacePosition);

        // stream always starts with response message, check it
        final Response response = messages.next().getResponse();
        if (!response.equals(OK)) {
            LOG.info("No checkpoint received for grammar {} at offset {}, reason: {}", grammarUuid, keyspacePosition, response);
            return Optional.empty();
        }

        // first part is metadata, retrieve it
        final CheckpointRequest request = messages.next().getRequest();
        final CheckpointMetadata metadata = request.getCheckpointMetadata();
        if (!metadata.getGrammarUuid().equals(grammarUuid.toString()) || metadata.getKeyspaceOffset() > keyspacePosition) {
            throw new RuntimeException("Invalid checkpoint received from server: requested %s_%d, received %s_%d".formatted(
                    metadata.getGrammarUuid(), metadata.getKeyspaceOffset(), grammarUuid, keyspacePosition
            ));
        }

        LOG.debug("Receiving checkpoint data for grammar {} at offset {}",
                metadata.getGrammarUuid(),
                metadata.getKeyspaceOffset()
        );

        // rest are the serialized chunk parts, read and deserialize
        final InputStream input = toInputStream(messages);
        return Optional.of(CheckpointCodec.forInput(input).readCheckpointUsingBaseRefs(pcfg));
    }

    @Override
    public boolean store(final Pcfg pcfg, final UUID grammarUuid, final long keyspacePosition, final Checkpoint state) throws IOException {
        final AtomicBoolean didStore = new AtomicBoolean(true);
        final CountDownLatch latch = new CountDownLatch(1);

        // TODO: hack
        final AtomicReference<StreamObserver<StreamMessage>> requestObserver = new AtomicReference<>();
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
                                    int currentOffset = offset;
                                    int remainingLength = length;

                                    while (remainingLength > 0) {
                                        // larger than chunk size happens when we directly write a byte array
                                        // to the output stream which would be too large
                                        final int chunkSize = Math.min(remainingLength, CHUNK_SIZE);
                                        requestObserver.get().onNext(chunkMessage(buffer, currentOffset, chunkSize));
                                        currentOffset += chunkSize;
                                        remainingLength -= chunkSize;
                                    }
                                }
                            }, CHUNK_SIZE)) {
                                final Duration duration = time(
                                    () -> CheckpointCodec.forOutput(output).writeCheckpointUsingBaseRefs(pcfg, state)
                                );
                                LOG.debug("Serializing checkpoint to server done, time taken: {}", duration);
                            }
                        } catch (final IOException e) {
                            LOG.warn("Failed to send checkpoint {} at offset {}", grammarUuid, keyspacePosition, e);
                            throw new UncheckedIOException(e);
                        } catch (final Throwable t) {
                            LOG.warn("Failed to send checkpoint {} at offset {}", grammarUuid, keyspacePosition, t);
                            throw new RuntimeException(t);
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

        requestObserver.get().onNext(metadataMessage(name, grammarUuid, keyspacePosition));
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
    private static InputStream toInputStream(final Iterator<StreamMessage> messages) {
        InputStream head = nullInputStream();
        while (messages.hasNext()) {
            final ByteString data = messages.next().getChunk().getData();
            head = new SequenceInputStream(head, new ByteArrayInputStream(UnsafeByteStringOperations.getBytes(data)));
        }
        return head;
    }
}
