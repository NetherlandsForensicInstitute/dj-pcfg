package nl.nfi.djpcfg.guess.pcfg.generate;

import static java.util.Map.Entry.comparingByValue;
import static java.util.stream.Collectors.joining;

import java.io.PrintStream;
import java.time.Duration;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nl.nfi.djpcfg.guess.cache.Checkpoint;
import nl.nfi.djpcfg.guess.pcfg.ParseTree;
import nl.nfi.djpcfg.guess.pcfg.Pcfg;
import nl.nfi.djpcfg.guess.pcfg.PcfgQueue;

public final class TrueProbOrder implements PasswordGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(TrueProbOrder.class);
    private static final long SKIP_COUNT_BEFORE_LOG = 16777216;

    private final Pcfg pcfg;
    private final Checkpoint state;

    private TrueProbOrder(final Pcfg pcfg, final Checkpoint state) {
        this.pcfg = pcfg;
        this.state = state;
    }

    public static TrueProbOrder init(final Pcfg pcfg, final Checkpoint state) {
        return new TrueProbOrder(pcfg, state);
    }

    @Override
    public Optional<Checkpoint> writeGuesses(final long skip, final long limit, final PrintStream output) {
        LOG.debug("Processing chunk : skip {}, limit {} ", skip, limit);

        long keyspacePosition = state.keyspacePosition();
        if (skip > 0 && skip < keyspacePosition) {
            // TODO: should probably not be invalid? but throw exception for now, in order to detect possible errors
            throw new IllegalStateException("%d < %d".formatted(skip, keyspacePosition));
        }

        long remainingToSkip = skip;
        remainingToSkip -= keyspacePosition;
        long totalToSkip = remainingToSkip;

        final PcfgQueue queue = PcfgQueue.loadFromExistingState(pcfg, state.queue());
        ParseTree next = state.next();

        // region Debugging
        final long dbgStartTimeOfSkip = System.nanoTime();

        long dbgSkipTimeSinceLogCount = dbgStartTimeOfSkip;
        long dbgSkippedSinceLogCount = 0;
        long dbgTerminalBatchOfSize1Count = 0;
        final Map<List<String>, Integer> dbgSingleTerminalParents = new IdentityHashMap<>();
        // endregion

        while (true) {
            final long terminalCount = pcfg.calculateTerminalCount(pcfg.grammar(), next);
            if (terminalCount > remainingToSkip) {
                break;
            }
            remainingToSkip -= terminalCount;

            // region Debugging
            dbgSkippedSinceLogCount += terminalCount;
            if (terminalCount == 1) {
                dbgTerminalBatchOfSize1Count++;
                dbgSingleTerminalParents.merge(next.replacementSet().variables(), 1, Integer::sum);
            }

            if (dbgSkippedSinceLogCount >= SKIP_COUNT_BEFORE_LOG) {
                final long skipped = skip - remainingToSkip;
                final long timeTaken = System.nanoTime() - dbgStartTimeOfSkip;

                LOG.debug(
                    "Skipped {} in: {}, " +
                        "num single terminals: {} ({}%), " +
                        "total skipped: {} ({}%), " +
                        "time taken: {}, " +
                        "estimated total time: {}",
                    dbgSkippedSinceLogCount,
                    Duration.ofNanos(System.nanoTime() - dbgSkipTimeSinceLogCount),
                    dbgTerminalBatchOfSize1Count,
                    String.format("%.2f", (dbgTerminalBatchOfSize1Count * 100.0d / skipped)),
                    skipped,
                    String.format("%.2f", skipped * 100.0d / skip),
                    Duration.ofNanos(timeTaken),
                    Duration.ofNanos((long) ((timeTaken) / (skipped * 1.0d / skip)))
                );
                dbgSkipTimeSinceLogCount = System.nanoTime();
                dbgSkippedSinceLogCount = 0;

                final Function<List<String>, String> formatVariables = variables -> variables.stream()
                    .filter(variable -> !variable.contains("C"))
                    .collect(joining());

                LOG.debug(dbgSingleTerminalParents.entrySet().stream()
                    .sorted(comparingByValue())
                    .map(e -> "%s: %d".formatted(formatVariables.apply(e.getKey()), e.getValue()))
                    .collect(joining("\n", "\n", "\n")));
            }
            // endregion

            if (!queue.hasNext()) {
                return Optional.empty();
            }
            next = queue.next();
        }

        // region Debugging
        final Duration durationOfSkip = Duration.ofNanos(System.nanoTime() - dbgStartTimeOfSkip);
        LOG.debug("Done skipping {}, time taken: {}", skip, durationOfSkip);
        // endregion

        keyspacePosition += totalToSkip - remainingToSkip;

        long remainingLimit = limit;

        while (true) {
            final long generatedGuessesCount = GuesserCommon.writeGuesses(pcfg.grammar(), next, remainingToSkip, remainingLimit, output);

            remainingLimit -= generatedGuessesCount;
            // if (remainingLimit <= 0 || output.checkError()) {
            if (remainingLimit <= 0) {
                return Optional.of(new Checkpoint(queue.queue(), next, keyspacePosition));
            }

            keyspacePosition += remainingToSkip + generatedGuessesCount;
            remainingToSkip = 0;

            if (!queue.hasNext()) {
                return Optional.empty();
            }
            next = queue.next();
        }
    }
}
