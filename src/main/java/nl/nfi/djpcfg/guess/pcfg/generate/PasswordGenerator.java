package nl.nfi.djpcfg.guess.pcfg.generate;

import nl.nfi.djpcfg.guess.cache.Checkpoint;

import java.io.PrintStream;
import java.util.Optional;

public interface PasswordGenerator {

    Optional<Checkpoint> writeGuesses(final long skip, final long limit, final PrintStream output);
}
