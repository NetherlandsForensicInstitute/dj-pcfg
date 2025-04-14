package nl.nfi.djpcfg.guess.cache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Comparator.comparing;

public final class CacheIndex {

    // for determining latest entries
    private final AtomicLong nextSeqNum;
    // map from rule UUID to set of keyspace offsets
    private final Map<UUID, Set<Long>> index;
    // list of (ordered by sequence number) references to each (UUID, offset) above
    private final List<LRUEntry> lru;

    private CacheIndex(final long nextSeqNum, final Map<UUID, Set<Long>> index, final List<LRUEntry> lru) {
        this.nextSeqNum = new AtomicLong(nextSeqNum);
        this.index = index;
        this.lru = lru;
    }

    public static CacheIndex emptyWithSequenceNumber(final long nextSeqNum) {
        return new CacheIndex(nextSeqNum, new HashMap<>(), new ArrayList<>());
    }

    public static CacheIndex init(final long nextSeqNum, final Map<UUID, Set<Long>> index, final List<LRUEntry> lru) {
        return new CacheIndex(nextSeqNum, index, lru);
    }

    public long nextSeqNum() {
        return nextSeqNum.get();
    }

    public Map<UUID, Set<Long>> index() {
        return index;
    }

    public List<LRUEntry> lru() {
        return lru;
    }

    public int numberOfCheckpoints() {
        return lru.size();
    }

    public boolean containsGrammarEntry(final UUID grammarUuid) {
        return index.containsKey(grammarUuid);
    }

    public Set<Long> getKeyspaceOffsets(final UUID grammarUuid) {
        return index.get(grammarUuid);
    }

    public LRUEntry removeOldestEntry() {
        final LRUEntry entry = lru.removeFirst();

        index.get(entry.uuid()).remove(entry.keyspacePosition());

        if (index.get(entry.uuid()).isEmpty()) {
            index.remove(entry.uuid());
        }

        return entry;
    }

    public boolean update(final UUID grammarUuid, final long keyspacePosition) {
        // check if we already stored this state before, if so,
        // remove from lru in order to let it be refreshed in the next step
        boolean stateExists = false;

        if (index.containsKey(grammarUuid) && index.get(grammarUuid).contains(keyspacePosition)) {
            // TODO: stateExists should always be true here? check for that
            stateExists = lru.removeIf(entry -> entry.uuid().equals(grammarUuid) && entry.keyspacePosition() == keyspacePosition);
        }

        // update LRU queue, ordering based on sequence number
        lru.add(new LRUEntry(grammarUuid, keyspacePosition, nextSeqNum.getAndIncrement()));
        // TODO: priority queue TODO-2: actually, this is not necessary?
        lru.sort(comparing(LRUEntry::sequenceNumber));

        if (!index.containsKey(grammarUuid)) {
            index.put(grammarUuid, new HashSet<>());
        }
        index.get(grammarUuid).add(keyspacePosition);

        return stateExists;
    }
}
