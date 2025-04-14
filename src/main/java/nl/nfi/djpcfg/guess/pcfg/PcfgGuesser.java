package nl.nfi.djpcfg.guess.pcfg;

import nl.nfi.djpcfg.guess.cache.Checkpoint;
import nl.nfi.djpcfg.guess.cache.CheckpointCache;
import nl.nfi.djpcfg.guess.pcfg.generate.Mode;
import nl.nfi.djpcfg.guess.pcfg.generate.PasswordGenerator;
import nl.nfi.djpcfg.guess.pcfg.generate.RandomWalk;
import nl.nfi.djpcfg.guess.pcfg.generate.ThreadedRandomWalk;
import nl.nfi.djpcfg.guess.pcfg.generate.ThreadedTrueProbOrder;
import nl.nfi.djpcfg.guess.pcfg.generate.TrueProbOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.math.BigInteger;
import java.nio.file.Path;

import static nl.nfi.djpcfg.guess.pcfg.generate.Mode.RANDOM_WALK;
import static nl.nfi.djpcfg.guess.pcfg.generate.Mode.TRUE_PROB_ORDER;

public final class PcfgGuesser {

    private static final Logger LOG = LoggerFactory.getLogger(PcfgGuesser.class);

    private final Pcfg pcfg;
    private final Mode mode;
    private final int threadCount;
    private final CheckpointCache cache;

    private PcfgGuesser(final Pcfg pcfg) {
        this(pcfg, TRUE_PROB_ORDER, 1, CheckpointCache.noop());
    }

    private PcfgGuesser(final Pcfg pcfg, final Mode mode, final int threadCount, final CheckpointCache cache) {
        this.pcfg = pcfg;
        this.mode = mode;
        this.threadCount = threadCount;
        this.cache = cache;
    }

    public static PcfgGuesser forRule(final Path rulePath) throws IOException {
        return forRule(Pcfg.loadFrom(rulePath));
    }

    public static PcfgGuesser forRule(final Pcfg pcfg) {
        return new PcfgGuesser(pcfg);
    }

    public PcfgGuesser mode(final Mode mode) {
        return new PcfgGuesser(pcfg, mode, threadCount, cache);
    }

    public PcfgGuesser cache(final CheckpointCache cache) throws IOException {
        return new PcfgGuesser(pcfg, mode, threadCount, cache);
    }

    public PcfgGuesser threadCount(final int threadCount) {
        return new PcfgGuesser(pcfg, mode, threadCount, cache);
    }

    public void showKeyspace(final long maxKeyspace, final PrintStream output) {
        if (mode == RANDOM_WALK) {
            if (maxKeyspace > 0) {
                output.println(maxKeyspace);
            } else {
                // just some impossibly large number, because there are infinite possibilities
                output.println(new BigInteger("9".repeat(1000)));
            }
            return;
        }

        final BigInteger size = pcfg.keyspaceSize();

        if (maxKeyspace > 0 && BigInteger.valueOf(maxKeyspace).compareTo(size) < 0) {
            output.println(maxKeyspace);
        } else {
            output.println(size);
        }
    }

    public void generateGuesses(final long skip, final long limit, final PrintStream output) throws IOException {
        LOG.info("Generating: skip {}, limit {}", skip, limit);

        final PasswordGenerator generator = switch (mode) {
            case RANDOM_WALK -> initRandomWalkGenerator();
            case TRUE_PROB_ORDER -> initTrueProbGenerator(cache, skip);
        };

        generator.writeGuesses(skip, limit, output).ifPresent(checkpoint -> {
            try {
                cache.store(pcfg, pcfg.ruleInfo().uuid(), checkpoint.keyspacePosition(), checkpoint);
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    private PasswordGenerator initRandomWalkGenerator() {
        if (threadCount == 1) {
            return RandomWalk.init(pcfg);
        }
        return ThreadedRandomWalk.init(pcfg);
    }

    private PasswordGenerator initTrueProbGenerator(final CheckpointCache cache, final long skip) throws IOException {
        final Checkpoint state = cache.getFurthestBefore(pcfg, pcfg.ruleInfo().uuid(), skip).orElseGet(() -> {
            // no state found where keyspace offset was <= current skip
            final PcfgQueue queue = PcfgQueue.fromStart(pcfg);
            return new Checkpoint(queue.queue(), queue.next(), 0);
        });

        if (threadCount == 1) {
            return TrueProbOrder.init(pcfg, state);
        }
        return ThreadedTrueProbOrder.init(pcfg, state).maxProducerThreads(threadCount);
    }
}
