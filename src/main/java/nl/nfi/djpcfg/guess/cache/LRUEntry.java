package nl.nfi.djpcfg.guess.cache;

import java.util.UUID;

// TODO: add timestamp for debugging
public record LRUEntry(UUID uuid, long keyspacePosition, long sequenceNumber) {

}
