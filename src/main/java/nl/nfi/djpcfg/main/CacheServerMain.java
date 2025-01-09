package nl.nfi.djpcfg.main;

import nl.nfi.djpcfg.guess.cache.distributed.CacheServerCli;
import picocli.CommandLine;

import java.io.IOException;

public final class CacheServerMain {

    public static void main(final String... args) throws IOException {
        final int exitCode = new CommandLine(new CacheServerCli()).execute(args);
        System.exit(exitCode);
    }
}
