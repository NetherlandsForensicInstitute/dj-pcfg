package nl.nfi.djpcfg.pcfg;

import nl.nfi.djpcfg.guess.cache.CheckpointCache;
import nl.nfi.djpcfg.guess.cache.directory.DirectoryCheckpointCache;
import nl.nfi.djpcfg.guess.pcfg.PcfgGuesser;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static nl.nfi.djpcfg.Utils.*;
import static org.assertj.core.api.Assertions.assertThat;

class PcfgGuesserTest {

    private static final Path TEST_RESOURCES_PATH = Paths.get("src/test/resources").toAbsolutePath();

    @Nested
    class TestWithOriginalRule {

        @TempDir
        Path tempWorkDir;

        @Test
        void generateLargeAmountOfPasswords() throws IOException, InterruptedException {
            final String rulePath = "rules/Default";
            final List<String> expected = generateWithPython(rulePath, 10000);
            final List<String> actual = generate(rulePath, 0, 10000);

            assertThat(actual).satisfiesAnyOf(
                    passwords -> assertThat(passwords).isEqualTo(expected),
                    passwords -> {
                        final double[] expectedProbabilities = scoreWithPython(rulePath, expected, tempWorkDir).stream()
                                .mapToDouble(ScoredEntry::score)
                                .toArray();
                        final double[] actualProbabilities = scoreWithPython(rulePath, passwords, tempWorkDir).stream()
                                .mapToDouble(ScoredEntry::score)
                                .toArray();

                        assertThat(actualProbabilities).containsExactly(expectedProbabilities);
                    }
            );
        }

        @ParameterizedTest(name = "Keyspace of {0} should be {1}")
        @CsvSource({
                "rules/test_1,67920",
                "rules/test_2,214851",
                "rules/test_3,57553836",
        })
        void keyspace(final String rulePath, final long expected) throws IOException {
            assertThat(calculateKeyspace(rulePath, 0))
                    .isEqualTo(expected);
        }

        @ParameterizedTest(name = "Keyspace of {0} should be {2}")
        @CsvSource({
                "rules/test_1,1,1",
                "rules/test_2,45454,45454",
                "rules/test_3,9999399939,57553836",
        })
        void maxKeyspace(final String rulePath, final long maxKeyspace, final long expected) throws IOException {
            assertThat(calculateKeyspace(rulePath, maxKeyspace))
                    .isEqualTo(expected);
        }

        // TODO: create cartesian product test of the following 3 tests
        @ParameterizedTest
        @CsvSource({
                "rules/Default,0",
                "rules/Default,1",
                "rules/Default,2",
                "rules/Default,3",
                "rules/Default,12345",
                "rules/Default,98773",
        })
        void skipWithLimit1(final String rulePath, final int skip) throws IOException, InterruptedException {
            final List<String> expected = generateWithPython(rulePath, skip + 1);
            final List<String> actual = generate(rulePath, skip, 1);

            assertThat(actual).satisfiesAnyOf(
                    passwords -> assertThat(passwords).isEqualTo(expected.subList(skip, expected.size())),
                    passwords -> {
                        final double[] expectedProbabilities = scoreWithPython(rulePath, expected.subList(skip, expected.size()), tempWorkDir).stream()
                                .mapToDouble(ScoredEntry::score)
                                .toArray();
                        final double[] actualProbabilities = scoreWithPython(rulePath, passwords, tempWorkDir).stream()
                                .mapToDouble(ScoredEntry::score)
                                .toArray();
                        assertThat(actualProbabilities).containsExactly(expectedProbabilities);
                    }
            );
        }

        @ParameterizedTest
        @CsvSource({
                "rules/Default,0",
                "rules/Default,1",
                "rules/Default,2",
                "rules/Default,3",
                "rules/Default,700",
                "rules/Default,98773",
        })
        void skipWithLimit13(final String rulePath, final int skip) throws IOException, InterruptedException {
            final List<String> expected = generateWithPython(rulePath, skip + 13);
            final List<String> actual = generate(rulePath, skip, 13);

            assertThat(actual).satisfiesAnyOf(
                    passwords -> assertThat(passwords).isEqualTo(expected.subList(skip, expected.size())),
                    passwords -> {
                        final double[] expectedProbabilities = scoreWithPython(rulePath, expected.subList(skip, expected.size()), tempWorkDir).stream()
                                .mapToDouble(ScoredEntry::score)
                                .toArray();
                        final double[] actualProbabilities = scoreWithPython(rulePath, passwords, tempWorkDir).stream()
                                .mapToDouble(ScoredEntry::score)
                                .toArray();
                        assertThat(actualProbabilities).containsExactly(expectedProbabilities);
                    }
            );
        }

        @ParameterizedTest
        @CsvSource({
                "rules/Default,0",
                "rules/Default,1",
                "rules/Default,2",
                "rules/Default,3",
                "rules/Default,12345",
                // "rules/Default,98773", // TODO: fix scorer should mirror PCFG
        })
        void skipWithLimit137(final String rulePath, final int skip) throws IOException, InterruptedException {
            final List<String> expected = generateWithPython(rulePath, skip + 137);
            final List<String> actual = generate(rulePath, skip, 137);

            assertThat(actual).satisfiesAnyOf(
                    passwords -> assertThat(passwords).isEqualTo(expected.subList(skip, expected.size())),
                    passwords -> {
                        final double[] expectedProbabilities = scoreWithPython(rulePath, expected.subList(skip, expected.size()), tempWorkDir).stream()
                                .mapToDouble(ScoredEntry::score)
                                .toArray();
                        final double[] actualProbabilities = scoreWithPython(rulePath, passwords, tempWorkDir).stream()
                                .mapToDouble(ScoredEntry::score)
                                .toArray();
                        assertThat(actualProbabilities).containsExactly(expectedProbabilities);
                    }
            );
        }
    }

    @Nested
    class TestWithSerializedRule {

        @TempDir
        Path tempWorkDir;

        @Test
        void generateLargeAmountOfPasswords() throws Exception {
            final String rulePath = "rules/Default";
            final List<String> expected = generateWithPython(rulePath, 10000);
            final List<String> actual = generate(rulePath + ".pbm", 0, 10000);

            assertThat(actual).satisfiesAnyOf(
                    passwords -> assertThat(passwords).isEqualTo(expected),
                    passwords -> {
                        final double[] expectedProbabilities = scoreWithPython(rulePath, expected, tempWorkDir).stream()
                                .mapToDouble(ScoredEntry::score)
                                .toArray();
                        final double[] actualProbabilities = scoreWithPython(rulePath, passwords, tempWorkDir).stream()
                                .mapToDouble(ScoredEntry::score)
                                .toArray();
                        assertThat(actualProbabilities).containsExactly(expectedProbabilities);
                    }
            );
        }

        // TODO: create cartesian product test of the following 3 tests
        @ParameterizedTest
        @CsvSource({
                "rules/Default.pbm,0",
                "rules/Default.pbm,1",
                "rules/Default.pbm,2",
                "rules/Default.pbm,3",
                "rules/Default.pbm,12345",
                "rules/Default.pbm,98773",
        })
        void skipWithLimit1(final String rulePath, final int skip) throws IOException {
            assertThat(generate(rulePath, skip, 1))
                    .isEqualTo(generate("rules/Default", skip, 1));
        }

        @ParameterizedTest
        @CsvSource({
                "rules/Default.pbm,0",
                "rules/Default.pbm,1",
                "rules/Default.pbm,2",
                "rules/Default.pbm,3",
                "rules/Default.pbm,700",
                "rules/Default.pbm,98773",
        })
        void skipWithLimit13(final String rulePath, final int skip) throws IOException {
            assertThat(generate(rulePath, skip, 13))
                    .isEqualTo(generate("rules/Default", skip, 13));
        }

        @ParameterizedTest
        @CsvSource({
                "rules/Default.pbm,0",
                "rules/Default.pbm,1",
                "rules/Default.pbm,2",
                "rules/Default.pbm,3",
                "rules/Default.pbm,12345",
                "rules/Default.pbm,98773",
        })
        void skipWithLimit137(final String rulePath, final int skip) throws IOException {
            assertThat(generate(rulePath, skip, 137))
                    .isEqualTo(generate("rules/Default", skip, 137));
        }
    }

    @Nested
    class CacheTest {

        static final long FIXED_LIMIT = 64;

        @TempDir
        Path tempCacheDir;

        @ParameterizedTest
        @CsvSource({
                "rules/Default, 0, 1",
                "rules/Default, 1, 1",
                "rules/Default, 1, 2",
                "rules/Default, 123, 789",
                "rules/Default, 7172, 8888",
                "rules/Default, 1111, 987654",
        })
        void skipPairsEmptySizeCache(final String rulePath, final int skipBefore, final int skipAfter) throws IOException {
            DirectoryCheckpointCache.MAX_CACHE_SIZE = 0;

            final CheckpointCache cache = DirectoryCheckpointCache.createOrLoadFrom(tempCacheDir);
            final PcfgGuesser guesser = PcfgGuesser.forRule(TEST_RESOURCES_PATH.resolve(rulePath)).cache(cache);

            List<String> expected;
            expected = generate(guesser, 0, skipAfter + FIXED_LIMIT);
            expected = expected.subList(skipAfter, expected.size());
            // sanity check
            assertThat(expected.size())
                    .isEqualTo(FIXED_LIMIT);

            List<String> actual;
            actual = generate(guesser.cache(cache), skipBefore, FIXED_LIMIT);
            actual = generate(guesser.cache(cache), skipAfter, FIXED_LIMIT);

            assertThat(actual).isEqualTo(expected);
        }

        @ParameterizedTest
        @CsvSource({
                "rules/Default, 0, 1",
                "rules/Default, 1, 1",
                "rules/Default, 1, 2",
                "rules/Default, 123, 789",
                "rules/Default, 7172, 8888",
                "rules/Default, 1111, 987654",
        })
        void skipPairsSmallSizeCache(final String rulePath, final int skipBefore, final int skipAfter) throws IOException {
            DirectoryCheckpointCache.MAX_CACHE_SIZE = 1L * 1024 * 1024;

            final CheckpointCache cache = DirectoryCheckpointCache.createOrLoadFrom(tempCacheDir);
            final PcfgGuesser guesser = PcfgGuesser.forRule(TEST_RESOURCES_PATH.resolve(rulePath)).cache(cache);

            List<String> expected;
            expected = generate(guesser, 0, skipAfter + FIXED_LIMIT);
            expected = expected.subList(skipAfter, expected.size());
            // sanity check
            assertThat(expected.size())
                    .isEqualTo(FIXED_LIMIT);

            List<String> actual;
            actual = generate(guesser.cache(cache), skipBefore, FIXED_LIMIT);
            actual = generate(guesser.cache(cache), skipAfter, FIXED_LIMIT);

            assertThat(actual).isEqualTo(expected);
        }

        @ParameterizedTest
        @CsvSource({
                "rules/Default, 0, 1",
                "rules/Default, 1, 1",
                "rules/Default, 1, 2",
                "rules/Default, 123, 789",
                "rules/Default, 7172, 8888",
                "rules/Default, 1111, 987654",
        })
        void skipPairsMediumSizeCache(final String rulePath, final int skipBefore, final int skipAfter) throws IOException {
            DirectoryCheckpointCache.MAX_CACHE_SIZE = 2 * 1024 * 1024;

            final CheckpointCache cache = DirectoryCheckpointCache.createOrLoadFrom(tempCacheDir);
            final PcfgGuesser guesser = PcfgGuesser.forRule(TEST_RESOURCES_PATH.resolve(rulePath)).cache(cache);

            List<String> expected;
            expected = generate(guesser, 0, skipAfter + FIXED_LIMIT);
            expected = expected.subList(skipAfter, expected.size());
            // sanity check
            assertThat(expected.size())
                    .isEqualTo(FIXED_LIMIT);

            List<String> actual;
            actual = generate(guesser.cache(cache), skipBefore, FIXED_LIMIT);
            actual = generate(guesser.cache(cache), skipAfter, FIXED_LIMIT);

            assertThat(actual).isEqualTo(expected);
        }

        @ParameterizedTest
        @CsvSource({
                "rules/Default, 0, 1",
                "rules/Default, 1, 1",
                "rules/Default, 1, 2",
                "rules/Default, 123, 789",
                "rules/Default, 7172, 8888",
                "rules/Default, 1111, 987654",
        })
        void skipPairsLargeSizeCache(final String rulePath, final int skipBefore, final int skipAfter) throws IOException {
            DirectoryCheckpointCache.MAX_CACHE_SIZE = 1L * 1024 * 1024 * 1024;

            final CheckpointCache cache = DirectoryCheckpointCache.createOrLoadFrom(tempCacheDir);
            final PcfgGuesser guesser = PcfgGuesser.forRule(TEST_RESOURCES_PATH.resolve(rulePath)).cache(cache);

            List<String> expected;
            expected = generate(guesser, 0, skipAfter + FIXED_LIMIT);
            expected = expected.subList(skipAfter, expected.size());
            // sanity check
            assertThat(expected.size())
                    .isEqualTo(FIXED_LIMIT);

            List<String> actual;
            actual = generate(guesser.cache(cache), skipBefore, FIXED_LIMIT);
            actual = generate(guesser.cache(cache), skipAfter, FIXED_LIMIT);

            assertThat(actual).isEqualTo(expected);
        }
    }
}