package nl.nfi.djpcfg.main;

import nl.nfi.djpcfg.guess.PcfgGuesserCli;
import picocli.CommandLine;

public final class GuesserMain {

    public static void main(final String... args) {
        final int exitCode = new CommandLine(new PcfgGuesserCli()).setCaseInsensitiveEnumValuesAllowed(true).execute(args);
        System.exit(exitCode);
    }
}