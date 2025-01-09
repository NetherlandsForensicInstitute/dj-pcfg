package nl.nfi.djpcfg.guess.pcfg.generate;

import nl.nfi.djpcfg.guess.cache.Checkpoint;
import nl.nfi.djpcfg.guess.pcfg.ParseTree;
import nl.nfi.djpcfg.guess.pcfg.Pcfg;
import nl.nfi.djpcfg.guess.pcfg.PcfgQueue;

import java.io.PrintStream;
import java.util.Optional;

public final class TrueProbOrder implements PasswordGenerator {

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

        while (true) {
            final long terminalCount = pcfg.calculateTerminalCount(pcfg.grammar(), next);
            if (terminalCount > remainingToSkip) {
                break;
            }
            remainingToSkip -= terminalCount;

            if (!queue.hasNext()) {
                return Optional.empty();
            }
            next = queue.next();
        }

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
