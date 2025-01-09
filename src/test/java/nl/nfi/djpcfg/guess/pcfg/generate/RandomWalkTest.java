package nl.nfi.djpcfg.guess.pcfg.generate;

import nl.nfi.djpcfg.guess.pcfg.Pcfg;
import nl.nfi.djpcfg.serialize.PcfgCodec;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static java.util.stream.Collectors.toCollection;
import static nl.nfi.djpcfg.Utils.ScoredEntry;
import static nl.nfi.djpcfg.Utils.scoreWithPython;

class RandomWalkTest {

    private static final Path TEST_RESOURCES_PATH = Paths.get("src/test/resources").toAbsolutePath();

    @TempDir
    Path tempWorkDir;

    @Disabled
    @Test
    void showScoreDistribution() throws Exception {
        final Pcfg pcfg = PcfgCodec.forInput(TEST_RESOURCES_PATH.resolve("rules/Default.pbm")).read();

        final long outputCount = 100_000;
        final List<Function<Random, PasswordGenerator>> generators = List.of(
                random -> RandomWalk.init(pcfg).random(random),
                random -> ThreadedRandomWalk.init(pcfg).random(random)
        );

        generators.forEach(creator -> {
            try {
                for (int i = 0; i < 4; i++) {
                    final ByteArrayOutputStream output = new ByteArrayOutputStream();
                    final PasswordGenerator generator = creator.apply(new Random(i));
                    generator.writeGuesses(0, outputCount, new PrintStream(output));
                    final List<String> passwords = output.toString().lines().collect(toCollection(ArrayList::new));

                    final List<ScoredEntry> scored = scoreWithPython("rules/Default", passwords, tempWorkDir);

                    final Map<Double, Integer> counts = new TreeMap<>();
                    scored.forEach(s -> {
                        counts.merge(s.score(), 1, Integer::sum);
                    });

                    System.out.println("// %s (iteration %d)".formatted(generator.getClass().getName(), i));
                    final AtomicInteger counter = new AtomicInteger(counts.size());
                    counts.forEach((k, v) -> {
                        if (counter.addAndGet(-1) < 16) {
                            System.out.println("//   %d: %s (%2.2f%%)".formatted(v, String.valueOf(k), v * 100.0 / counts.size()));
                        }
                    });
                }
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
}