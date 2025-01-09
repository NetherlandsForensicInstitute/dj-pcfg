package nl.nfi.djpcfg.guess.cache.distributed;

import static java.io.OutputStream.nullOutputStream;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.copy;
import static java.nio.file.Files.isRegularFile;
import static java.nio.file.Files.readAllBytes;
import static java.util.Collections.emptyList;
import static java.util.Map.entry;
import static java.util.stream.LongStream.of;
import static java.util.stream.LongStream.range;

import static org.assertj.core.api.Assertions.assertThat;

import static nl.nfi.djpcfg.Utils.time;
import static nl.nfi.djpcfg.serialize.CacheIndexCodec.forInput;

import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import nl.nfi.djpcfg.Utils.TimedResult;
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
            System.out.println("Generating large rule file...");
            final Pcfg pcfg = Pcfg.loadFrom(DEFAULT_RULE_PATH);
            final Checkpoint checkpoint = generateCheckpoint(pcfg, 10000000000L, 1);
            try (final CheckpointCodec.Encoder encoder = CheckpointCodec.forOutput(LARGE_CHECKPOINT_PATH)) {
                encoder.writeCacheUsingBaseRefs(pcfg, checkpoint);
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

            final Checkpoint checkpoint = CheckpointCodec.forInput(LARGE_CHECKPOINT_PATH).readCacheUsingBaseRefs(pcfg);
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
    void mixSingleClient() throws IOException, NoSuchAlgorithmException {
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

            final MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
//            final String output = list(tempWorkDir)
//                    .filter(Files::isRegularFile)
//                    .filter(file -> file.toString().contains("-"))
//                    .sorted()
//                    .map(file -> {
//                        try {
//                            return "  entry(\"" + file.getFileName() + "\", \"" + HexFormat.of().formatHex(sha256.digest(readAllBytes(file))) + "\")";
//                        } catch (final IOException e) {
//                            throw new UncheckedIOException(e);
//                        }
//                    })
//                    .collect(joining(",\n", "Map.ofEntries(\n", "\n)"));

            // TODO: fragile test using hash, also not checking if there are extraneous files
            final Map<String, String> entries = Map.ofEntries(
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_1", "e46c6145fdd82673bc8615987d7430a3f54b75cdca31a5dfacb641d2e081fbb3"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_10", "b522bc8181b56fd67c5bd77514cc1178820ab182f4cd65ef45a03b48b97722ed"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_11", "9aa7df0678fe0b9ecf4ae026853ef80ecff8ef94a92acbc4381ae6a68712ca66"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_12", "098e4be27b139def78334ed82bcd5744fda7cec0ab4e0f4af51eeadd2627acfa"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_12105", "92189c66199d87e054db86190ca3224674714b8a0f321a979979d30cf3152ef1"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_123", "96150c997f08beb58d734df4f8d47472d46ad1d4b6b7f4aa0400dcbe94df367f"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_13", "ee0f5dd8020bf61cd695b52c046ee47916766052ce817984ea4844706fe56548"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_14", "f929501cf4f8197ba1593e08ed9ccff77e40ddae7db396e00028456df4752241"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_15", "dbe1b2761b75163974a2adabf9d6fcf6d763650e52eeb3987e20de4db678b844"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_16", "a1a9eb5717c672b318e57a21265e7b011d9ab3e772631c217133395b67e2922c"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_17", "a07307066d1eed05d7577dea47611ba95fb833f5b1950cd61c48c3c60f18d1bf"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_18", "bd607859d817817f210c6236787e0dde0adc489306284564babaa57f3be4ab3d"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_19", "0a263d9512e2c5534fd546476d6c3456055e8956625e850b16afb297247001c8"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_2", "e779c2b7b2b12a70304e64c37a06db8fe9e1be75d2ec1db134cccabee48f7393"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_20", "54415ebd2a772330606cdc8940f0d6da92ca1bea2a880cee8baedfe54e1abce5"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_21", "22c29621d67bedc6eefa84ffa058942f1b6f5d0c03f4100713f71cef7c82b485"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_21630", "d786867fe52437086c15a1a0ae1c5e53e6e1b83391cfbc5037052bbcfd2fa906"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_22", "450ccf35199bea2a8c9932b2df91a7048eb4a49e97f58b309afc07f2bed6714b"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_23", "2c40f2436e789df85f68ce1cb562191ffad3f0fee94900fcef1524586acd1ce6"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_24", "c37bc0b9648d30395518e7397ad2c5910fdf1ecacf16515c0b30e4f534b3cf8c"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_3", "89dd64dd2cc865aee9e602d8ac43e9b0a70401549c0db10804f09b952d1b1cf9"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_334", "5f9674b0574cb8e11f2117dd3c048064a8cdb7fbc31bc7fcbc40fb91522a22b0"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_34342", "0425dbe7c44a29da4d0e90057b4fe9160402fe0b991a11688de2b852e3082f1c"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_4", "ab0c84b46b4e3f3a1579f0b7b290385b0bccce42b5b80536e35a55d4893e4797"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_46", "d9fae60610c303cab993f86629b17cd092612f2e58a24a099f37603493f90884"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_5", "4af02fa16ecfb2881b2caff7d782dc781b557b29cf5444475c0cceddf82ccf97"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_5697", "10163f96e9776abdde0a0c4d19345e968976424e2990e6b997668064b463b4a2"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_58554", "35709891531338e9e756c13beb019e6636f2ed366475bcef57b3e6e2c3f9497d"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_6", "584889b1d4eb012c9aa72dfb286ffb54b3698acede695468c26a2861280e960e"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_7", "dfc4d37e7bd6043261c6598e972ef8cecc6baf0fbea62977f172598323d1ba64"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_78025", "724e16167bed8b6c0e85a548fbf8a7d3ce0330d425183bd6855da39594e88f02"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_8", "6dfedbdef526581764ca7bca1d6d8bcde9ed812ec77e0f74b0ce799855821f73"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_9", "d35b84140a69d13acfb02cdb0202b216b6cc860657c5e0f853c8592b55e0420b"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_962364", "1fff7651a5602ffa8cff75e6142c637bb99f59365ea0bb459f2c166c7807e857"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_99154", "1bb2ceacce5879314bbf4a57f3ca0132e4b15c33c551d8fe34e221306ed9e592")
            );

            entries.forEach((file, expectedHash) -> {
                try {
                    final String actualHash = HexFormat.of().formatHex(sha256.digest(readAllBytes(tempWorkDir.resolve(file))));
                    assertThat(actualHash).isEqualTo(expectedHash);
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
            of(offsets)
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
            final Map<String, String> entries = Map.ofEntries(
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_1", "e46c6145fdd82673bc8615987d7430a3f54b75cdca31a5dfacb641d2e081fbb3"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_10", "b522bc8181b56fd67c5bd77514cc1178820ab182f4cd65ef45a03b48b97722ed"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_11", "9aa7df0678fe0b9ecf4ae026853ef80ecff8ef94a92acbc4381ae6a68712ca66"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_12", "098e4be27b139def78334ed82bcd5744fda7cec0ab4e0f4af51eeadd2627acfa"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_12105", "92189c66199d87e054db86190ca3224674714b8a0f321a979979d30cf3152ef1"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_123", "96150c997f08beb58d734df4f8d47472d46ad1d4b6b7f4aa0400dcbe94df367f"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_13", "ee0f5dd8020bf61cd695b52c046ee47916766052ce817984ea4844706fe56548"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_14", "f929501cf4f8197ba1593e08ed9ccff77e40ddae7db396e00028456df4752241"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_15", "dbe1b2761b75163974a2adabf9d6fcf6d763650e52eeb3987e20de4db678b844"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_16", "a1a9eb5717c672b318e57a21265e7b011d9ab3e772631c217133395b67e2922c"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_17", "a07307066d1eed05d7577dea47611ba95fb833f5b1950cd61c48c3c60f18d1bf"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_18", "bd607859d817817f210c6236787e0dde0adc489306284564babaa57f3be4ab3d"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_19", "0a263d9512e2c5534fd546476d6c3456055e8956625e850b16afb297247001c8"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_2", "e779c2b7b2b12a70304e64c37a06db8fe9e1be75d2ec1db134cccabee48f7393"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_20", "54415ebd2a772330606cdc8940f0d6da92ca1bea2a880cee8baedfe54e1abce5"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_21", "22c29621d67bedc6eefa84ffa058942f1b6f5d0c03f4100713f71cef7c82b485"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_21630", "d786867fe52437086c15a1a0ae1c5e53e6e1b83391cfbc5037052bbcfd2fa906"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_22", "450ccf35199bea2a8c9932b2df91a7048eb4a49e97f58b309afc07f2bed6714b"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_23", "2c40f2436e789df85f68ce1cb562191ffad3f0fee94900fcef1524586acd1ce6"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_24", "c37bc0b9648d30395518e7397ad2c5910fdf1ecacf16515c0b30e4f534b3cf8c"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_3", "89dd64dd2cc865aee9e602d8ac43e9b0a70401549c0db10804f09b952d1b1cf9"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_334", "5f9674b0574cb8e11f2117dd3c048064a8cdb7fbc31bc7fcbc40fb91522a22b0"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_34342", "0425dbe7c44a29da4d0e90057b4fe9160402fe0b991a11688de2b852e3082f1c"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_4", "ab0c84b46b4e3f3a1579f0b7b290385b0bccce42b5b80536e35a55d4893e4797"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_46", "d9fae60610c303cab993f86629b17cd092612f2e58a24a099f37603493f90884"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_5", "4af02fa16ecfb2881b2caff7d782dc781b557b29cf5444475c0cceddf82ccf97"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_5697", "10163f96e9776abdde0a0c4d19345e968976424e2990e6b997668064b463b4a2"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_58554", "35709891531338e9e756c13beb019e6636f2ed366475bcef57b3e6e2c3f9497d"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_6", "584889b1d4eb012c9aa72dfb286ffb54b3698acede695468c26a2861280e960e"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_7", "dfc4d37e7bd6043261c6598e972ef8cecc6baf0fbea62977f172598323d1ba64"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_78025", "724e16167bed8b6c0e85a548fbf8a7d3ce0330d425183bd6855da39594e88f02"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_8", "6dfedbdef526581764ca7bca1d6d8bcde9ed812ec77e0f74b0ce799855821f73"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_9", "d35b84140a69d13acfb02cdb0202b216b6cc860657c5e0f853c8592b55e0420b"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_962364", "1fff7651a5602ffa8cff75e6142c637bb99f59365ea0bb459f2c166c7807e857"),
                entry("384adebf-ba38-4307-891d-5b4a0a02178b_99154", "1bb2ceacce5879314bbf4a57f3ca0132e4b15c33c551d8fe34e221306ed9e592")
            );

            entries.forEach((file, expectedHash) -> {
                try {
                    final String actualHash = HexFormat.of().formatHex(sha256.digest(readAllBytes(tempWorkDir.resolve(file))));
                    assertThat(actualHash).isEqualTo(expectedHash);
                }
                catch (final IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }
    }


    @Test
    void performanceTest() throws IOException {
        final Pcfg pcfg = PcfgCodec.forInput(DEFAULT_RULE_PATH).read();

        try (final CheckpointCacheServer server = CheckpointCacheServer.start(RANDOM_PORT, tempWorkDir);
             final CheckpointCacheClient client = CheckpointCacheClient.connect("localhost", RANDOM_PORT)) {

            {
                final Checkpoint checkpoint = CheckpointCodec.forInput(LARGE_CHECKPOINT_PATH).readCacheUsingBaseRefs(pcfg);
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
}