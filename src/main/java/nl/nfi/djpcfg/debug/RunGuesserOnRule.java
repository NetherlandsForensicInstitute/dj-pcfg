package nl.nfi.djpcfg.debug;

import static nl.nfi.djpcfg.common.Timers.time;
import static nl.nfi.djpcfg.guess.pcfg.generate.Mode.TRUE_PROB_ORDER;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.nio.file.Paths;

import nl.nfi.djpcfg.common.Timers.TimedResult;
import nl.nfi.djpcfg.guess.pcfg.Pcfg;
import nl.nfi.djpcfg.guess.pcfg.PcfgGuesser;

public final class RunGuesserOnRule {

    static {
        System.setProperty("LOG_DIRECTORY_PATH", "debug_log");
    }

    public static void main(final String... args) throws IOException {
        // insert path to trained PCFG rule, either the non-serialized rule directory or the serialized .pbm
        final Path rulePath = Paths.get("src/test/resources/rules/Default.pbm");

        final TimedResult<Pcfg> result = time(() -> Pcfg.loadFrom(rulePath));
        System.out.println("Time taken to load PCFG rule: " + result.duration());
        final Pcfg pcfg = result.value();

        final PcfgGuesser guesser = PcfgGuesser.forRule(pcfg)
            .mode(TRUE_PROB_ORDER)
            .threadCount(4);

        final int skip = 128;
        final int limit = 8;
        final PrintStream output = System.out;

        guesser.generateGuesses(skip, limit, output);
    }
}
