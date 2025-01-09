package nl.nfi.djpcfg.guess.pcfg.generate;

import nl.nfi.djpcfg.guess.cache.Checkpoint;
import nl.nfi.djpcfg.guess.pcfg.Pcfg;
import nl.nfi.djpcfg.guess.pcfg.ReplacementSet;

import java.io.PrintStream;
import java.util.Optional;
import java.util.Random;
import java.util.function.Function;

import static nl.nfi.djpcfg.guess.pcfg.generate.GuesserCommon.generateGuess;
import static nl.nfi.djpcfg.guess.pcfg.generate.Samplers.buildReplacementsSampler;

public final class RandomWalk implements PasswordGenerator {

    private final Pcfg pcfg;
    private final Random random;

    private RandomWalk(final Pcfg pcfg, final Random random) {
        this.pcfg = pcfg;
        this.random = random;
    }

    public static RandomWalk init(final Pcfg pcfg) {
        return new RandomWalk(pcfg, null);
    }

    public RandomWalk random(final Random random) {
        return new RandomWalk(pcfg, random);
    }

    @Override
    public Optional<Checkpoint> writeGuesses(final long skip, final long limit, final PrintStream output) {
        final Pcfg pcfg = this.pcfg;
        final Random random = this.random == null ? new Random(skip) : this.random;

        final Function<Random, ReplacementSet> replacementsSampler = buildReplacementsSampler(pcfg);

        long remainingLimit = limit;
        do {
            output.println(generateGuess(pcfg.grammar(), replacementsSampler.apply(random), random));
            remainingLimit--;
        } while (remainingLimit > 0);

        return Optional.empty();
    }
}
