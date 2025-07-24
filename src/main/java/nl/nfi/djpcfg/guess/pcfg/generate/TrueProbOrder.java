package nl.nfi.djpcfg.guess.pcfg.generate;

import static java.util.Comparator.reverseOrder;
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

        long dbgSkipTimeSinceLog = dbgStartTimeOfSkip;
        long dbgSkippedSinceLog = 0;
        final Map<List<String>, Integer> dbgSingleTerminalParents = new IdentityHashMap<>();
        // endregion

        while (true) {
            final long terminalCount = pcfg.calculateTerminalCount(pcfg.grammar(), next);
            if (terminalCount > remainingToSkip) {
                break;
            }
            remainingToSkip -= terminalCount;

            // region Debugging
            dbgSkippedSinceLog += terminalCount;
            if (terminalCount == 1) {
                dbgSingleTerminalParents.merge(next.replacementSet().variables(), 1, Integer::sum);
            }

            if (dbgSkippedSinceLog >= SKIP_COUNT_BEFORE_LOG) {
                logStatistics(skip, remainingToSkip, dbgSkippedSinceLog, dbgStartTimeOfSkip, dbgSkipTimeSinceLog, dbgSingleTerminalParents);
                dbgSkipTimeSinceLog = System.nanoTime();
                dbgSkippedSinceLog = 0;
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

    // region Debugging
    private static void logStatistics(
        final long toSkip, final long remainingToSkip, final long skippedSinceLog,
        final long startTimeOfSkip, final long skipTimeSinceLog,
        final Map<List<String>, Integer> singleTerminalParents
    ) {
        final long skipped = toSkip - remainingToSkip;
        final long timeTaken = System.nanoTime() - startTimeOfSkip;
        final long terminalBatchOfSize1Count = singleTerminalParents.values().stream()
            .mapToLong(value -> value)
            .sum();

        LOG.debug(
            "Skipped {} in: {}, " +
                "num single terminals: {} ({}%), " +
                "total skipped: {} ({}%), " +
                "time taken: {}, " +
                "estimated total time: {}",
            skippedSinceLog,
            Duration.ofNanos(System.nanoTime() - skipTimeSinceLog),
            terminalBatchOfSize1Count,
            String.format("%.2f", (terminalBatchOfSize1Count * 100.0d / skipped)),
            skipped,
            String.format("%.2f", skipped * 100.0d / toSkip),
            Duration.ofNanos(timeTaken),
            Duration.ofNanos((long) ((timeTaken) / (skipped * 1.0d / toSkip)))
        );

        final Function<List<String>, String> formatVariables = variables -> variables.stream()
            .filter(variable -> !variable.contains("C"))
            .collect(joining());

        LOG.debug(singleTerminalParents.entrySet().stream()
            .sorted(comparingByValue(reverseOrder()))
            .limit(64)
            .map(e -> "%s: %d".formatted(formatVariables.apply(e.getKey()), e.getValue()))
            .collect(joining("\n", "Statistics:\nBase structures and their number of generated terminal sets of size 1:\n", "\n...\n")));
    }
    // endregion
}
