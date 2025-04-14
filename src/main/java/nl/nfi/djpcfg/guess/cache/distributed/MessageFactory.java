package nl.nfi.djpcfg.guess.cache.distributed;

import java.util.UUID;

import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;
import static nl.nfi.djpcfg.guess.cache.distributed.CheckpointServiceOuterClass.CheckpointMetadata;
import static nl.nfi.djpcfg.guess.cache.distributed.CheckpointServiceOuterClass.CheckpointRequest;
import static nl.nfi.djpcfg.guess.cache.distributed.CheckpointServiceOuterClass.Chunk;
import static nl.nfi.djpcfg.guess.cache.distributed.CheckpointServiceOuterClass.Response;
import static nl.nfi.djpcfg.guess.cache.distributed.CheckpointServiceOuterClass.StorageResponse;
import static nl.nfi.djpcfg.guess.cache.distributed.CheckpointServiceOuterClass.StreamMessage;

public final class MessageFactory {

    static StreamMessage responseMessage(final Response response) {
        return StreamMessage.newBuilder()
                .setResponse(response)
                .build();
    }

    static StreamMessage metadataMessage(final String senderName, final UUID uuid, final long keyspaceOffset) {
        return StreamMessage.newBuilder()
                .setRequest(request(senderName, uuid, keyspaceOffset))
                .build();
    }

    static StreamMessage metadataMessage(final UUID uuid, final long keyspaceOffset) {
        return metadataMessage("", uuid, keyspaceOffset);
    }

    static StreamMessage chunkMessage(final byte[] chunk, final int length) {
        return chunkMessage(chunk, 0, length);
    }

    static StreamMessage chunkMessage(final byte[] chunk, final int offset, final int length) {
        return StreamMessage.newBuilder()
                .setChunk(Chunk.newBuilder().setData(unsafeWrap(chunk, offset, length))) // .setData(ByteString.copyFrom(chunk, offset, length))
                .build();
    }

    static CheckpointRequest request(final String senderName, final UUID uuid, final long keyspaceOffset) {
        return CheckpointRequest.newBuilder()
                .setSenderName(senderName)
                .setCheckpointMetadata(
                        CheckpointMetadata.newBuilder()
                                .setGrammarUuid(uuid.toString())
                                .setKeyspaceOffset(keyspaceOffset)
                ).build();
    }

    static StorageResponse response(final Response response) {
        return StorageResponse.newBuilder()
                .setResponse(response)
                .build();
    }
}
