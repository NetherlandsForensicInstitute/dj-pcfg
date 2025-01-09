package nl.nfi.djpcfg.guess.cache;

import nl.nfi.djpcfg.guess.pcfg.Pcfg;

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

public interface CheckpointCache {

    Optional<Checkpoint> getFurthestBefore(final Pcfg pcfg, final UUID grammarUuid, final long keyspacePosition) throws IOException;

    boolean store(final Pcfg pcfg, final UUID grammarUuid, final long keyspacePosition, final Checkpoint checkpoint) throws IOException;

    static CheckpointCache noop() {
        return new CheckpointCache() {
            @Override
            public Optional<Checkpoint> getFurthestBefore(final Pcfg pcfg, final UUID grammarUuid, final long keyspacePosition) throws IOException {
                return Optional.empty();
            }

            @Override
            public boolean store(final Pcfg pcfg, final UUID grammarUuid, final long keyspacePosition, final Checkpoint checkpoint) throws IOException {
                return false;
            }
        };
    }
}