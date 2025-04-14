package nl.nfi.djpcfg.guess.cache.directory;

import nl.nfi.djpcfg.guess.cache.Checkpoint;
import nl.nfi.djpcfg.guess.pcfg.ParseTree;
import nl.nfi.djpcfg.guess.pcfg.Pcfg;
import nl.nfi.djpcfg.guess.pcfg.ReplacementSet;
import nl.nfi.djpcfg.guess.pcfg.grammar.BaseStructure;
import nl.nfi.djpcfg.serialize.CheckpointCodec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.PriorityQueue;
import java.util.UUID;

import static java.util.Collections.emptyList;
import static java.util.Comparator.comparing;
import static org.assertj.core.api.Assertions.assertThat;

class DirectoryCheckpointCacheTest {

    @TempDir
    Path tempWorkDir;

    @Test
    void entryRemovalOrder() throws IOException {
        // exact size (for now)
        // final int singleSerializedStateSize = 8232;
        final int singleSerializedStateSize = sizeOfSingleSerializedState();
        // way overestimated
        final int idxEstimatedSize = 1000;
        // let's allow at most 3 state entries
        DirectoryCheckpointCache.MAX_CACHE_SIZE = 3L * singleSerializedStateSize + idxEstimatedSize;

        final UUID uuid = UUID.fromString("00000000-0000-0000-0000-000000000000");

        final PriorityQueue<ParseTree> queue = new PriorityQueue<>(comparing(ParseTree::probability).reversed());
        queue.add(new ParseTree(0.0, 0.0, new ReplacementSet(emptyList(), new int[0])));
        final ParseTree next = new ParseTree(0.0, 0.0, new ReplacementSet(emptyList(), new int[8192]));

        final Pcfg pcfg = Pcfg.create(null, null, List.of(new BaseStructure(1.0, emptyList())));

        final DirectoryCheckpointCache checkpointCache = DirectoryCheckpointCache.createOrLoadFrom(tempWorkDir);
        checkpointCache.store(pcfg, uuid, 0, new Checkpoint(queue, next, 0));
        checkpointCache.store(pcfg, uuid, 128, new Checkpoint(queue, next, 128));
        checkpointCache.store(pcfg, uuid, 555, new Checkpoint(queue, next, 555));

        {
            // sanity check
            final List<String> expectedFiles = List.of(
                    uuid + "_0",
                    uuid + "_128",
                    uuid + "_555",
                    "cache.idx"
            );
            assertThat(getActualFiles()).isEqualTo(expectedFiles);
        }
        {
            // now adding a fourth one should trigger the reduction, and should remove the position 0 one
            checkpointCache.store(pcfg, uuid, Long.MAX_VALUE, new Checkpoint(queue, next, Long.MAX_VALUE));
            final List<String> expectedFiles = List.of(
                    uuid + "_128",
                    uuid + "_555",
                    uuid + "_" + Long.MAX_VALUE,
                    "cache.idx"
            );
            assertThat(getActualFiles()).isEqualTo(expectedFiles);
        }
        {
            // now adding a same one should refresh the creation time
            checkpointCache.store(pcfg, uuid, 128, new Checkpoint(queue, next, 128));
            final List<String> expectedFiles = List.of(
                    uuid + "_128",
                    uuid + "_555",
                    uuid + "_" + Long.MAX_VALUE,
                    "cache.idx"
            );
            assertThat(getActualFiles()).isEqualTo(expectedFiles);
        }
        {
            // now adding a new one should delete the one at 555, because it was the oldest entry
            checkpointCache.store(pcfg, uuid, 123456789, new Checkpoint(queue, next, 123456789));
            final List<String> expectedFiles = List.of(
                    uuid + "_123456789",
                    uuid + "_128",
                    uuid + "_" + Long.MAX_VALUE,
                    "cache.idx"
            );
            assertThat(getActualFiles()).isEqualTo(expectedFiles);
        }
        {
            checkpointCache.store(pcfg, UUID.fromString("d53ddf2a-541a-4902-adde-69e164737f20"), 1, new Checkpoint(queue, next, 1));
            checkpointCache.store(pcfg, UUID.fromString("11111111-1111-1111-1111-111111111111"), 9999, new Checkpoint(queue, next, 9999));
            final List<String> expectedFiles = List.of(
                    uuid + "_123456789",
                    "11111111-1111-1111-1111-111111111111_9999",
                    "cache.idx",
                    "d53ddf2a-541a-4902-adde-69e164737f20_1"
            );
            assertThat(getActualFiles()).isEqualTo(expectedFiles);
        }
        {
            checkpointCache.store(pcfg, uuid, 0, new Checkpoint(queue, next, 0));
            final List<String> expectedFiles = List.of(
                    uuid + "_0",
                    "11111111-1111-1111-1111-111111111111_9999",
                    "cache.idx",
                    "d53ddf2a-541a-4902-adde-69e164737f20_1"
            );
            assertThat(getActualFiles()).isEqualTo(expectedFiles);
        }
    }

    private List<String> getActualFiles() throws IOException {
        return Files.walk(tempWorkDir)
                .filter(Files::isRegularFile)
                .map(Path::getFileName)
                .map(Path::toString)
                .filter(fileName -> !fileName.startsWith("."))
                .sorted()
                .toList();
    }

    private static int sizeOfSingleSerializedState() throws IOException {
        final PriorityQueue<ParseTree> queue = new PriorityQueue<>(comparing(ParseTree::probability).reversed());
        queue.add(new ParseTree(0.0, 0.0, new ReplacementSet(emptyList(), new int[0])));
        final ParseTree next = new ParseTree(0.0, 0.0, new ReplacementSet(emptyList(), new int[8192]));
        final Pcfg pcfg = Pcfg.create(null, null, List.of(new BaseStructure(1.0, emptyList())));

        final Checkpoint state = new Checkpoint(queue, next, 0);

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        CheckpointCodec.forOutput(out).writeCheckpointUsingBaseRefs(pcfg, state);
        return out.size();
    }
}