package nl.nfi.djpcfg.guess.pcfg.generate;

import nl.nfi.djpcfg.guess.cache.Checkpoint;
import nl.nfi.djpcfg.guess.pcfg.Pcfg;
import nl.nfi.djpcfg.guess.pcfg.ReplacementSet;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.lang.Long.compareUnsigned;
import static java.lang.Math.min;
import static java.util.stream.LongStream.iterate;
import static nl.nfi.djpcfg.guess.pcfg.generate.GuesserCommon.generateGuess;
import static nl.nfi.djpcfg.guess.pcfg.generate.Samplers.buildReplacementsSampler;

public final class ThreadedRandomWalk implements PasswordGenerator {

    private static final int SPLIT_SIZE = 1 << 16;

    private final Pcfg pcfg;
    private final Random random;

    private ThreadedRandomWalk(final Pcfg pcfg, final Random random) {
        this.pcfg = pcfg;
        this.random = random;
    }

    public static ThreadedRandomWalk init(final Pcfg pcfg) {
        return new ThreadedRandomWalk(pcfg, null);
    }

    public ThreadedRandomWalk random(final Random random) {
        return new ThreadedRandomWalk(pcfg, random);
    }

    @Override
    public Optional<Checkpoint> writeGuesses(final long skip, final long limit, final PrintStream output) {
        final Pcfg pcfg = this.pcfg;
        final Function<Random, ReplacementSet> replacementsSampler = buildReplacementsSampler(pcfg);

        streamChunks(skip, limit)
                .unordered()
                .parallel()
                .map(state -> {
                    final Random random = this.random == null ? new Random(state.remainingSkip) : new Random(this.random.nextLong());
                    final List<String> batch = new ArrayList<>(SPLIT_SIZE);

                    long remainingLimit = state.remainingLimit;
                    do {
                        batch.add(generateGuess(pcfg.grammar(), replacementsSampler.apply(random), random));
                        remainingLimit--;
                    } while (remainingLimit > 0);

                    return batch;
                }).forEach(batch -> {
                    synchronized (output) {
                        batch.forEach(output::println);
                    }
                });

        return Optional.empty();
    }

    private static Stream<ProgressState> streamChunks(final long skip, final long limit) {
        return iterate(skip, toSkip -> compareUnsigned(toSkip, skip + limit) < 0, toSkip -> toSkip + SPLIT_SIZE)
                .mapToObj(toSkip -> {
                    final ProgressState progress = new ProgressState();
                    progress.remainingSkip = toSkip;
                    progress.remainingLimit = min(SPLIT_SIZE, skip + limit - toSkip);
                    return progress;
                });
    }
}
