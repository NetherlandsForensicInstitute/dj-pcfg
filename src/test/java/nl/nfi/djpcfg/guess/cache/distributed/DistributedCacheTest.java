package nl.nfi.djpcfg.guess.cache.distributed;

import nl.nfi.djpcfg.common.Timers.TimedResult;
import nl.nfi.djpcfg.guess.cache.CacheIndex;
import nl.nfi.djpcfg.guess.cache.Checkpoint;
import nl.nfi.djpcfg.guess.cache.directory.DirectoryCheckpointCache;
import nl.nfi.djpcfg.guess.pcfg.Pcfg;
import nl.nfi.djpcfg.guess.pcfg.PcfgGuesser;
import nl.nfi.djpcfg.guess.pcfg.PcfgQueue;
import nl.nfi.djpcfg.guess.pcfg.RuleInfo;
import nl.nfi.djpcfg.guess.pcfg.generate.TrueProbOrder;
import nl.nfi.djpcfg.guess.pcfg.grammar.Grammar;
import nl.nfi.djpcfg.serialize.CacheIndexCodec;
import nl.nfi.djpcfg.serialize.CheckpointCodec;
import nl.nfi.djpcfg.serialize.PcfgCodec;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.LongStream;

import static java.io.OutputStream.nullOutputStream;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.copy;
import static java.nio.file.Files.isRegularFile;
import static java.nio.file.Files.list;
import static java.util.Collections.emptyList;
import static java.util.Map.entry;
import static java.util.stream.Collectors.joining;
import static java.util.stream.LongStream.range;
import static nl.nfi.djpcfg.common.Timers.time;
import static org.assertj.core.api.Assertions.assertThat;

class DistributedCacheTest {

    private static final int RANDOM_PORT = 63535;

    private static final Path TEST_RESOURCES_PATH = Paths.get("src/test/resources").toAbsolutePath();

    private static final Path DEFAULT_TRUNCATED_RULE_PATH = TEST_RESOURCES_PATH.resolve("rules/Default_truncated.pbm");
    private static final Path DEFAULT_RULE_PATH = TEST_RESOURCES_PATH.resolve("rules/Default.pbm");
    private static final Path LARGE_CHECKPOINT_PATH = TEST_RESOURCES_PATH.resolve("checkpoints/384adebf-ba38-4307-891d-5b4a0a02178b_9999990930");

    @TempDir
    Path tempWorkDir;

    @BeforeAll
    static void cacheLargeCheckPoint() throws IOException {
        // System.setProperty("LOG_DIRECTORY_PATH", "/tmp/test-logs");

        if (!isRegularFile(LARGE_CHECKPOINT_PATH)) {
            System.out.println("==================================================================================");
            System.out.println("|       Generating large test checkpoint, can take up to 15 minutes!             |");
            System.out.println("|       One time only, successive test runs will reuse the generated file!       |");
            System.out.println("==================================================================================");
            final Pcfg pcfg = Pcfg.loadFrom(DEFAULT_RULE_PATH);
            final Checkpoint checkpoint = generateCheckpoint(pcfg, 10000000000L, 1);
            try (final CheckpointCodec.Encoder encoder = CheckpointCodec.forOutput(LARGE_CHECKPOINT_PATH)) {
                encoder.writeCheckpointUsingBaseRefs(pcfg, checkpoint);
            }
        }
    }

    @Test
    void loadCheckpoint() throws IOException {
        final DirectoryCheckpointCache cache = DirectoryCheckpointCache.createOrLoadFrom(tempWorkDir);

        final Pcfg pcfg = PcfgCodec.forInput(DEFAULT_TRUNCATED_RULE_PATH).read();
        final PcfgGuesser guesser = PcfgGuesser.forRule(DEFAULT_TRUNCATED_RULE_PATH).cache(cache);

        guesser.generateGuesses(7, 1, new PrintStream(nullOutputStream()));
        guesser.generateGuesses(1023, 1, new PrintStream(nullOutputStream()));

        try (final CheckpointCacheServer server = CheckpointCacheServer.start(RANDOM_PORT, tempWorkDir);
             final CheckpointCacheClient client = CheckpointCacheClient.connect("localhost", RANDOM_PORT)) {

            assertThat(client.getFurthestBefore(pcfg, pcfg.ruleInfo().uuid(), 6)).isNotPresent();

            assertThat(client.getFurthestBefore(pcfg, pcfg.ruleInfo().uuid(), 42)).hasValueSatisfying(checkpoint -> {
                assertThat(checkpoint.keyspacePosition()).isEqualTo(7);
            });

            assertThat(client.getFurthestBefore(pcfg, pcfg.ruleInfo().uuid(), 999999)).hasValueSatisfying(checkpoint -> {
                assertThat(checkpoint.keyspacePosition()).isEqualTo(991);
            });

            final Pcfg nonExistingPcfg = Pcfg.create(new RuleInfo(
                UUID.fromString("11111111-1111-1111-1111-111111111111"),
                Paths.get(""),
                "",
                UTF_8
            ), Grammar.empty(), emptyList());
            assertThat(client.getFurthestBefore(nonExistingPcfg, nonExistingPcfg.ruleInfo().uuid(), 999999)).isNotPresent();
        }
    }

    @Test
    void loadLargeCheckpoint() throws IOException {
        final Pcfg pcfg = PcfgCodec.forInput(DEFAULT_RULE_PATH).read();

        addToCache(tempWorkDir, UUID.fromString("384adebf-ba38-4307-891d-5b4a0a02178b"), 9999990930L, LARGE_CHECKPOINT_PATH);

        try (final CheckpointCacheServer server = CheckpointCacheServer.start(RANDOM_PORT, tempWorkDir);
             final CheckpointCacheClient client = CheckpointCacheClient.connect("localhost", RANDOM_PORT)) {

            assertThat(client.getFurthestBefore(pcfg, pcfg.ruleInfo().uuid(), 10000000000L)).hasValueSatisfying(checkpoint -> {
                assertThat(checkpoint.keyspacePosition()).isEqualTo(9999990930L);
            });
        }
    }

    @Test
    void storeCheckpoint() throws IOException {
        final Pcfg pcfg = PcfgCodec.forInput(DEFAULT_RULE_PATH).read();

        try (final CheckpointCacheServer server = CheckpointCacheServer.start(RANDOM_PORT, tempWorkDir);
             final CheckpointCacheClient client = CheckpointCacheClient.connect("localhost", RANDOM_PORT)) {

            final Checkpoint checkpoint = generateCheckpoint(pcfg, 12345, 1);
            // should not exist yet
            {
                final boolean didStore = client.store(pcfg, pcfg.ruleInfo().uuid(), checkpoint.keyspacePosition(), checkpoint);
                assertThat(didStore).isTrue();
            }
            // exists, so did not store anything
            {
                final boolean didStore = client.store(pcfg, pcfg.ruleInfo().uuid(), checkpoint.keyspacePosition(), checkpoint);
                assertThat(didStore).isFalse();
            }
        }
    }

    @Test
    void storeLargeCheckpoint() throws IOException {
        final Pcfg pcfg = PcfgCodec.forInput(DEFAULT_RULE_PATH).read();

        try (final CheckpointCacheServer server = CheckpointCacheServer.start(RANDOM_PORT, tempWorkDir);
             final CheckpointCacheClient client = CheckpointCacheClient.connect("localhost", RANDOM_PORT)) {

            final Checkpoint checkpoint = CheckpointCodec.forInput(LARGE_CHECKPOINT_PATH).readCheckpointUsingBaseRefs(pcfg);
            // should not exist yet, so should have stored
            {
                final boolean didStore = client.store(pcfg, pcfg.ruleInfo().uuid(), checkpoint.keyspacePosition(), checkpoint);
                assertThat(didStore).isTrue();
            }
            // exists, so did not store anything
            {
                final boolean didStore = client.store(pcfg, pcfg.ruleInfo().uuid(), checkpoint.keyspacePosition(), checkpoint);
                assertThat(didStore).isFalse();
            }
        }
    }

    @Test
    void loadAndStoreCheckpoint() throws IOException {
        final Pcfg pcfg = PcfgCodec.forInput(DEFAULT_TRUNCATED_RULE_PATH).read();

        try (final CheckpointCacheServer server = CheckpointCacheServer.start(RANDOM_PORT, tempWorkDir);
             final CheckpointCacheClient client = CheckpointCacheClient.connect("localhost", RANDOM_PORT)) {

            assertThat(client.getFurthestBefore(pcfg, pcfg.ruleInfo().uuid(), 42)).isEmpty();

            {
                final Checkpoint checkpoint = generateCheckpoint(pcfg, 7, 1);
                client.store(pcfg, pcfg.ruleInfo().uuid(), checkpoint.keyspacePosition(), checkpoint);
            }

            assertThat(client.getFurthestBefore(pcfg, pcfg.ruleInfo().uuid(), 42)).hasValueSatisfying(checkpoint -> {
                assertThat(checkpoint.keyspacePosition()).isEqualTo(7);
            });

            assertThat(client.getFurthestBefore(pcfg, pcfg.ruleInfo().uuid(), 999999)).hasValueSatisfying(checkpoint -> {
                assertThat(checkpoint.keyspacePosition()).isEqualTo(7);
            });

            {
                final Checkpoint checkpoint = generateCheckpoint(pcfg, 1023, 1);
                client.store(pcfg, pcfg.ruleInfo().uuid(), checkpoint.keyspacePosition(), checkpoint);
            }

            assertThat(client.getFurthestBefore(pcfg, pcfg.ruleInfo().uuid(), 999999)).hasValueSatisfying(checkpoint -> {
                assertThat(checkpoint.keyspacePosition()).isEqualTo(991);
            });
        }
    }

    @Test
    void multipleReads() throws IOException {
        final Pcfg pcfg = PcfgCodec.forInput(DEFAULT_RULE_PATH).read();

        addToCache(tempWorkDir, UUID.fromString("384adebf-ba38-4307-891d-5b4a0a02178b"), 9999990930L, LARGE_CHECKPOINT_PATH);

        try (final CheckpointCacheServer server = CheckpointCacheServer.start(RANDOM_PORT, tempWorkDir)) {
            try (final CheckpointCacheClient client = CheckpointCacheClient.connect("localhost", RANDOM_PORT)) {
                {
                    final TimedResult<?> result = time(1,
                        () -> {
                            System.out.println("Requesting checkpoint from thread: " + Thread.currentThread().getName());
                            assertThat(client.getFurthestBefore(pcfg, pcfg.ruleInfo().uuid(), 10000000000L)).hasValueSatisfying(checkpoint -> assertThat(checkpoint.keyspacePosition()).isEqualTo(9999990930L));
                        }
                    );
                    printTimings(result, 1);
                }
            }

            final TimedResult<?> result = time(1, () -> range(0, 8).unordered().parallel().forEach(_ -> {
                try (final CheckpointCacheClient client = CheckpointCacheClient.connect("localhost", RANDOM_PORT)) {
                    System.out.println("Requesting checkpoint from thread: " + Thread.currentThread().getName());
                    assertThat(client.getFurthestBefore(pcfg, pcfg.ruleInfo().uuid(), 10000000000L)).hasValueSatisfying(checkpoint -> assertThat(checkpoint.keyspacePosition()).isEqualTo(9999990930L));
                }
                catch (final IOException e) {
                    throw new UncheckedIOException(e);
                }
            }));

            printTimings(result, 1);
        }
    }

    @Test
    void mixSingleClient() throws IOException {
        final Pcfg pcfg = PcfgCodec.forInput(DEFAULT_RULE_PATH).read();

        final long[] offsets = {
            10, 99999, 78484, 34343, 22222, 12345, 46, 5744, 58555, 334, 1, 999444, 123, 123, 123, 123,
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24
        };

        try (final CheckpointCacheServer server = CheckpointCacheServer.start(RANDOM_PORT, tempWorkDir);
             final CheckpointCacheClient client = CheckpointCacheClient.connect("localhost", RANDOM_PORT)) {

            for (final long offset : offsets) {
                {
                    final Checkpoint checkpoint = generateCheckpoint(pcfg, offset, 1);
                    client.store(pcfg, pcfg.ruleInfo().uuid(), checkpoint.keyspacePosition(), checkpoint);
                }
            }

            final CacheIndex index = CacheIndexCodec.forInput(tempWorkDir.resolve("cache.idx")).read();
            assertThat(index.getKeyspaceOffsets(pcfg.ruleInfo().uuid())).isEqualTo(Set.of(
                1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L, 20L, 21L, 22L, 23L, 24L, 46L, 123L, 334L, 5697L, 12105L, 21630L, 34342L, 58554L, 78025L, 99154L, 962364L
            ));

            final String output = list(tempWorkDir)
                    .filter(Files::isRegularFile)
                    .filter(file -> file.toString().contains("_"))
                    .sorted()
                    .map(file -> {
                        try {
                            final Checkpoint checkpoint = CheckpointCodec.forInput(file).readCheckpointUsingBaseRefs(pcfg);
                            return "  entry(\"" + file.getFileName() + "\", new Info(%d, %d, %s))".formatted(checkpoint.keyspacePosition(), checkpoint.queue().size(), Double.toString(checkpoint.next().probability()));
                        } catch (final IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    })
                    .collect(joining(",\n", "Map.ofEntries(\n", "\n)"));

            // System.out.println(output);

            // TODO: fragile test using hash, also not checking if there are extraneous files
            final Map<String, Info> entries = Map.ofEntries(
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_1", new Info(1, 11318, 0.0024244029452405967)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_10", new Info(10, 11320, 5.755422235042858E-4)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_11", new Info(11, 11320, 5.326712943791993E-4)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_12", new Info(12, 11321, 5.208610916092177E-4)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_12105", new Info(12105, 11463, 4.4766345659649674E-6)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_123", new Info(123, 11324, 1.7630669602691808E-4)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_13", new Info(13, 11321, 4.7908263297284117E-4)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_14", new Info(14, 11321, 4.5731376888565305E-4)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_15", new Info(15, 11321, 4.3835525030400905E-4)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_16", new Info(16, 11321, 4.306465888141571E-4)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_17", new Info(17, 11321, 4.174556723555294E-4)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_18", new Info(18, 11321, 4.1121086084506777E-4)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_19", new Info(19, 11321, 4.060947070553935E-4)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_2", new Info(2, 11319, 0.001888500267613507)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_20", new Info(20, 11321, 3.9816375424924053E-4)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_21", new Info(21, 11321, 3.8384543629717727E-4)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_21630", new Info(21630, 11538, 2.6794330703179038E-6)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_22", new Info(22, 11322, 3.8160445216704254E-4)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_23", new Info(23, 11322, 3.810153825992059E-4)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_24", new Info(24, 11322, 3.7557290534249673E-4)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_3", new Info(3, 11319, 0.0015437994060342043)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_334", new Info(334, 11329, 9.21724976189359E-5)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_34342", new Info(34342, 11624, 1.7836439345140781E-6)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_4", new Info(4, 11319, 0.0015408471922418515)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_46", new Info(46, 11323, 2.9560852802658214E-4)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_5", new Info(5, 11319, 0.0012074122943631226)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_5697", new Info(5697, 11407, 8.574185825017293E-6)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_58554", new Info(58554, 11700, 1.2543396119357143E-6)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_6", new Info(6, 11319, 6.574257619731409E-4)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_7", new Info(7, 11319, 6.42261801054931E-4)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_78025", new Info(78025, 11737, 9.463345603793074E-7)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_8", new Info(8, 11319, 6.364303675070386E-4)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_9", new Info(9, 11320, 6.119825132606092E-4)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_962364", new Info(962364, 14326, 5.6498296750519124E-8)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_99154", new Info(99154, 11739, 9.272821862404561E-7))
            );

            entries.forEach((file, info) -> {
                try {
                    final Checkpoint checkpoint = CheckpointCodec.forInput(tempWorkDir.resolve(file)).readCheckpointUsingBaseRefs(pcfg);
                    assertThat(checkpoint.keyspacePosition()).isEqualTo(info.keyspacePosition());
                    assertThat(checkpoint.queue().size()).isEqualTo(info.queueSize());
                    assertThat(checkpoint.next().probability()).isEqualTo(info.currentProbability());
                }
                catch (final IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }
    }

    @Test
    void mixMultiClient() throws IOException, NoSuchAlgorithmException {
        final Pcfg pcfg = PcfgCodec.forInput(DEFAULT_RULE_PATH).read();

        final long[] offsets = {
            10, 99999, 78484, 34343, 22222, 12345, 46, 5744, 58555, 334, 1, 999444, 123, 123, 123, 123,
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24
        };

        try (final CheckpointCacheServer server = CheckpointCacheServer.start(RANDOM_PORT, tempWorkDir)) {
            LongStream.of(offsets)
                .parallel()
                .forEach(offset -> {
                    try (final CheckpointCacheClient client = CheckpointCacheClient.connect("localhost", RANDOM_PORT)) {
                        final Checkpoint checkpoint = generateCheckpoint(pcfg, offset, 1);
                        client.store(pcfg, pcfg.ruleInfo().uuid(), checkpoint.keyspacePosition(), checkpoint);
                    }
                    catch (final IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });

            final CacheIndex index = CacheIndexCodec.forInput(tempWorkDir.resolve("cache.idx")).read();
            assertThat(index.getKeyspaceOffsets(pcfg.ruleInfo().uuid())).isEqualTo(Set.of(
                1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L, 20L, 21L, 22L, 23L, 24L, 46L, 123L, 334L, 5697L, 12105L, 21630L, 34342L, 58554L, 78025L, 99154L, 962364L
            ));

            final MessageDigest sha256 = MessageDigest.getInstance("SHA-256");

            // TODO: fragile test using hash, also not checking if there are extraneous files
            final Map<String, Info> entries = Map.ofEntries(
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_1", new Info(1, 11318, 0.0024244029452405967)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_10", new Info(10, 11320, 5.755422235042858E-4)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_11", new Info(11, 11320, 5.326712943791993E-4)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_12", new Info(12, 11321, 5.208610916092177E-4)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_12105", new Info(12105, 11463, 4.4766345659649674E-6)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_123", new Info(123, 11324, 1.7630669602691808E-4)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_13", new Info(13, 11321, 4.7908263297284117E-4)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_14", new Info(14, 11321, 4.5731376888565305E-4)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_15", new Info(15, 11321, 4.3835525030400905E-4)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_16", new Info(16, 11321, 4.306465888141571E-4)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_17", new Info(17, 11321, 4.174556723555294E-4)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_18", new Info(18, 11321, 4.1121086084506777E-4)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_19", new Info(19, 11321, 4.060947070553935E-4)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_2", new Info(2, 11319, 0.001888500267613507)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_20", new Info(20, 11321, 3.9816375424924053E-4)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_21", new Info(21, 11321, 3.8384543629717727E-4)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_21630", new Info(21630, 11538, 2.6794330703179038E-6)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_22", new Info(22, 11322, 3.8160445216704254E-4)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_23", new Info(23, 11322, 3.810153825992059E-4)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_24", new Info(24, 11322, 3.7557290534249673E-4)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_3", new Info(3, 11319, 0.0015437994060342043)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_334", new Info(334, 11329, 9.21724976189359E-5)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_34342", new Info(34342, 11624, 1.7836439345140781E-6)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_4", new Info(4, 11319, 0.0015408471922418515)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_46", new Info(46, 11323, 2.9560852802658214E-4)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_5", new Info(5, 11319, 0.0012074122943631226)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_5697", new Info(5697, 11407, 8.574185825017293E-6)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_58554", new Info(58554, 11700, 1.2543396119357143E-6)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_6", new Info(6, 11319, 6.574257619731409E-4)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_7", new Info(7, 11319, 6.42261801054931E-4)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_78025", new Info(78025, 11737, 9.463345603793074E-7)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_8", new Info(8, 11319, 6.364303675070386E-4)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_9", new Info(9, 11320, 6.119825132606092E-4)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_962364", new Info(962364, 14326, 5.6498296750519124E-8)),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_99154", new Info(99154, 11739, 9.272821862404561E-7))
            );

            entries.forEach((file, info) -> {
                try {
                    final Checkpoint checkpoint = CheckpointCodec.forInput(tempWorkDir.resolve(file)).readCheckpointUsingBaseRefs(pcfg);
                    assertThat(checkpoint.keyspacePosition()).isEqualTo(info.keyspacePosition());
                    assertThat(checkpoint.queue().size()).isEqualTo(info.queueSize());
                    assertThat(checkpoint.next().probability()).isEqualTo(info.currentProbability());
                }
                catch (final IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }
    }


    @Disabled
    @Test
    void performanceTest() throws IOException {
        final Pcfg pcfg = PcfgCodec.forInput(DEFAULT_RULE_PATH).read();

        try (final CheckpointCacheServer server = CheckpointCacheServer.start(RANDOM_PORT, tempWorkDir);
             final CheckpointCacheClient client = CheckpointCacheClient.connect("localhost", RANDOM_PORT)) {

            {
                final Checkpoint checkpoint = CheckpointCodec.forInput(LARGE_CHECKPOINT_PATH).readCheckpointUsingBaseRefs(pcfg);
                final boolean didStore = client.store(pcfg, pcfg.ruleInfo().uuid(), checkpoint.keyspacePosition(), checkpoint);
                assertThat(didStore).isTrue();
            }

            final int iterationCount = 8;
            final TimedResult<?> result = time(iterationCount,
                () -> assertThat(client.getFurthestBefore(pcfg, pcfg.ruleInfo().uuid(), 10000000000L)).hasValueSatisfying(checkpoint -> assertThat(checkpoint.keyspacePosition()).isEqualTo(9999990930L))
            );

            printTimings(result, iterationCount);
        }
    }

    private static void printTimings(final TimedResult<?> result, final int iterationCount) {
        final long timeInNanoseconds = result.duration().toNanos();
        System.out.println("// %d iterations".formatted(iterationCount));
        System.out.println("// ns: %d".formatted(timeInNanoseconds));
        System.out.println("// ms: %d".formatted(timeInNanoseconds / 1000000));
        System.out.println("//  s: %d".formatted(timeInNanoseconds / 1000000000));
    }

    private static Checkpoint generateCheckpoint(final Pcfg pcfg, final long skip, final long limit) {
        final PcfgQueue queue = PcfgQueue.fromStart(pcfg);
        final Checkpoint checkpoint = new Checkpoint(queue.queue(), queue.next(), 0);

        return TrueProbOrder.init(pcfg, checkpoint).writeGuesses(skip, limit, new PrintStream(nullOutputStream())).orElseThrow();
    }

    private static void addToCache(final Path cacheDirectory, final UUID uuid, final long offset, final Path checkpoint) throws IOException {
        final CheckpointFileCache cache = CheckpointFileCache.createOrLoadFrom(cacheDirectory);
        cache.store(uuid, offset, copy(checkpoint, checkpoint.resolveSibling(UUID.randomUUID().toString())));
    }

    record Info(long keyspacePosition, int queueSize, double currentProbability) {

    }
}
