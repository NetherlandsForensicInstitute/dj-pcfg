package nl.nfi.djpcfg.guess.cache.distributed;

import nl.nfi.djpcfg.guess.cache.Checkpoint;
import nl.nfi.djpcfg.guess.pcfg.ParseTree;
import nl.nfi.djpcfg.guess.pcfg.Pcfg;
import nl.nfi.djpcfg.guess.pcfg.ReplacementSet;
import nl.nfi.djpcfg.serialize.PcfgCodec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;

import static java.util.Comparator.comparing;
import static org.assertj.core.api.Assertions.assertThat;

class CheckpointCacheClientTest {

    private static final int RANDOM_PORT = 63535;

    private static final Path TEST_RESOURCES_PATH = Paths.get("src/test/resources").toAbsolutePath();

    private static final Path DEFAULT_RULE_PATH = TEST_RESOURCES_PATH.resolve("rules/Default.pbm");

    @TempDir
    Path tempWorkDir;

    @Test
    void writeByteArrayLargerThanMaxMessageSize() throws IOException {
        final Pcfg pcfg = PcfgCodec.forInput(DEFAULT_RULE_PATH).read();

        try (final CheckpointCacheServer server = CheckpointCacheServer.start(RANDOM_PORT, tempWorkDir);
             final CheckpointCacheClient client = CheckpointCacheClient.connect("localhost", RANDOM_PORT)) {

            final Random random = new Random(0);
            final PriorityQueue<ParseTree> queue = new PriorityQueue<>(comparing(ParseTree::probability).reversed());
            for (int i = 0; i < 20000000; i++) {
                final List<String> variables = pcfg.baseStructures().get(0).variables();
                queue.add(new ParseTree(
                    1.0,
                    // generate a lot of random doubles, should compress less good
                    random.nextDouble(),
                    new ReplacementSet(variables, new int[variables.size()]))
                );
            }

            final Checkpoint checkpoint = new Checkpoint(queue, queue.poll(), 42);

            final boolean didStore = client.store(pcfg, pcfg.ruleInfo().uuid(), checkpoint.keyspacePosition(), checkpoint);
            assertThat(didStore).isTrue();
        }
    }
}