package nl.nfi.djpcfg.pcfg;

import nl.nfi.djpcfg.common.Timers;
import nl.nfi.djpcfg.common.Timers.TimedResult;
import nl.nfi.djpcfg.guess.pcfg.Pcfg;
import nl.nfi.djpcfg.guess.pcfg.PcfgGuesser;
import nl.nfi.djpcfg.guess.pcfg.generate.PasswordGenerator;
import nl.nfi.djpcfg.guess.pcfg.generate.ThreadedRandomWalk;
import nl.nfi.djpcfg.serialize.PcfgCodec;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static nl.nfi.djpcfg.common.Timers.time;
import static org.assertj.core.api.Assertions.assertThat;

class PerformanceTest {

    private static final Path TEST_RESOURCES_PATH = Paths.get("src/test/resources").toAbsolutePath();

    @Disabled
    @Test
    void skipPerformance() throws IOException {
        final CountingPrintStream output = new CountingPrintStream();
        final PcfgGuesser guesser = PcfgGuesser.forRule(TEST_RESOURCES_PATH.resolve(Paths.get("rules/Default.pbm")));

        final long outputCount = 256;
        final long iterationCount = 4;

        final TimedResult<?> result = time(iterationCount,
                () -> guesser.generateGuesses(100_000_000, outputCount, output)
        );

        assertThat(output.count()).isEqualTo(outputCount * iterationCount);

        final long timeInNanoseconds = result.duration().toNanos();
        System.out.println("// %d iterations skip".formatted(iterationCount));
        System.out.println("// ns: %d".formatted(timeInNanoseconds));
        System.out.println("// ms: %d".formatted(timeInNanoseconds / 1000000));
        System.out.println("//  s: %d".formatted(timeInNanoseconds / 1000000000));
        // 4 iterations skip
        // ns: 9970377951
        // ms: 9970
        //  s: 9
    }

    @Disabled
    @Test
    void generatePerformanceTrueProb() throws IOException {
        final PcfgGuesser guesser = PcfgGuesser.forRule(TEST_RESOURCES_PATH.resolve(Paths.get("rules/Default.pbm"))).threadCount(4);

        final long outputCount = 100_000_000;
        final long iterationCount = 4;

        final CountingPrintStream output = new CountingPrintStream();
        final TimedResult<?> result = time(iterationCount,
                () -> guesser.generateGuesses(0, outputCount, output)
        );

        assertThat(output.count()).isEqualTo(outputCount * iterationCount);

        final long timeInNanoseconds = result.duration().toNanos();
        System.out.println("// %d iterations generate".formatted(iterationCount));
        System.out.println("// ns: %d".formatted(timeInNanoseconds));
        System.out.println("// ms: %d".formatted(timeInNanoseconds / 1000000));
        System.out.println("//  s: %d".formatted(timeInNanoseconds / 1000000000));
        // 4 iterations generate
        // ns: 14923161364
        // ms: 14923
        //  s: 14
    }

    @Disabled
    @Test
    void generatePerformanceRandomWalk() throws IOException {
        final Pcfg pcfg = PcfgCodec.forInput(TEST_RESOURCES_PATH.resolve(Paths.get("rules/Default.pbm"))).read();

        final long outputCount = 100_000_000;
        final long iterationCount = 4;

        final List<PasswordGenerator> generators = List.of(
//                RandomWalk.init(pcfg),
                ThreadedRandomWalk.init(pcfg)
        );

        generators.forEach(generator -> {
            final CountingPrintStream output = new CountingPrintStream();
            final TimedResult<?> result = time(iterationCount, () -> generator.writeGuesses(0, outputCount, output));

            assertThat(output.count()).isEqualTo(outputCount * iterationCount);

            final long timeInNanoseconds = result.duration().toNanos();
            System.out.println("// %s".formatted(generator.getClass().getName()));
            System.out.println("// %d iterations generate".formatted(iterationCount));
            System.out.println("// ns: %d".formatted(timeInNanoseconds));
            System.out.println("// ms: %d".formatted(timeInNanoseconds / 1000000));
            System.out.println("//  s: %d".formatted(timeInNanoseconds / 1000000000));
            System.out.println();
        });
        // nl.nfi.djpcfg.guess.pcfg.generate.RandomWalk
        // 4 iterations generate
        // ns: 30652037588
        // ms: 30652
        //  s: 30

        // nl.nfi.djpcfg.guess.pcfg.generate.ThreadedRandomWalk
        // 4 iterations generate
        // ns: 4610048757
        // ms: 4610
        //  s: 4
    }

    private static class CountingPrintStream extends PrintStream {

        private int count = 0;

        public CountingPrintStream() {
            super(nullOutputStream());
        }

        public int count() {
            return count;
        }

        @Override
        public void println(final String s) {
            count++;
        }
    }
}