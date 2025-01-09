package nl.nfi.djpcfg.guess.cache.distributed;

import java.util.UUID;

import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;
import static nl.nfi.djpcfg.guess.cache.distributed.CheckpointServiceOuterClass.*;

public final class MessageFactory {

    static CheckpointPart responsePart(final Response response) {
        return CheckpointPart.newBuilder()
                .setResponse(response)
                .build();
    }

    static CheckpointPart metadataPart(final String senderName, final UUID uuid, final long keyspaceOffset) {
        return CheckpointPart.newBuilder()
                .setMetadata(request(senderName, uuid, keyspaceOffset))
                .build();
    }

    static CheckpointPart metadataPart(final UUID uuid, final long keyspaceOffset) {
        return metadataPart("", uuid, keyspaceOffset);
    }

    static CheckpointPart chunkPart(final byte[] chunk, final int length) {
        return chunkPart(chunk, 0, length);
    }

    static CheckpointPart chunkPart(final byte[] chunk, final int offset, final int length) {
        return CheckpointPart.newBuilder()
                .setChunk(Chunk.newBuilder().setData(unsafeWrap(chunk, offset, length))) // .setData(ByteString.copyFrom(chunk, offset, length))
                .build();
    }

    static RequestMetadata request(final String senderName, final UUID uuid, final long keyspaceOffset) {
        return RequestMetadata.newBuilder()
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
