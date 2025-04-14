package nl.nfi.djpcfg.guess;

import nl.nfi.djpcfg.guess.cache.CheckpointCache;
import nl.nfi.djpcfg.guess.cache.directory.DirectoryCheckpointCache;
import nl.nfi.djpcfg.guess.cache.distributed.CheckpointCacheClient;
import nl.nfi.djpcfg.guess.pcfg.PcfgGuesser;
import nl.nfi.djpcfg.guess.pcfg.generate.Mode;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.file.Paths;
import java.util.concurrent.Callable;

import static picocli.CommandLine.Command;
import static picocli.CommandLine.ExitCode;
import static picocli.CommandLine.Option;

@Command(name = "pcfg_guesser")
public class PcfgGuesserCli implements Callable<Integer> {

    @Option(names = {"--rule"}, description = "The PCFG rule to start generating guesses from", required = true)
    private String rulePath;

    @Option(names = {"--output"}, description = "The file to write the guesses to")
    private String outputPath = "-";

    @Option(names = {"--keyspace"}, description = "Return the number of passwords that can be generated from the model")
    private boolean showKeyspace = false;

    @Option(names = {"--max_keyspace"}, description = "Limit the value returned by the keyspace to this number")
    private long maxKeyspace = 0;

    @Option(names = {"--skip"}, description = "Skip the first <skip> passwords before starting to output")
    private long skip = 0;

    @Option(names = {"--limit"}, description = "Output at most <limit> passwords")
    private long limit = Long.MAX_VALUE;

    @Option(names = {"--cache_directory_path"}, description = "Load/store state using given cache directory while generating guesses")
    private String cachePath = null;

    @Option(names = {"--cache_server_address"}, description = "Load/store state using given caching server")
    private String cacheServerAddress = null;

    // 4-5 threads optimal for test case model
    @Option(names = {"--producer_thread_count"}, description = "Use <count> threads for generation (experimental)")
    private int threadCount = 1;

    @Option(names = {"--log_directory_path"}, description = "Directory where to store live and archived log files")
    private String logPath;

    @Option(names = {"--mode"}, description = "Valid values: ${COMPLETION-CANDIDATES} (case insensitive)", defaultValue = "true_prob_order")
    private Mode mode;

    @Override
    public Integer call() throws Exception {
        // TODO: fix hack
        if (logPath != null) {
            System.setProperty("LOG_DIRECTORY_PATH", logPath);
        }

        try {
            final CheckpointCache cache = cachePath != null ? DirectoryCheckpointCache.createOrLoadFrom(Paths.get(cachePath))
                : cacheServerAddress != null ? CheckpointCacheClient.connect(cacheServerAddress)
                : CheckpointCache.noop();

            final PcfgGuesser guesser = PcfgGuesser.forRule(Paths.get(rulePath))
                .mode(mode)
                .threadCount(threadCount)
                .cache(cache);

            if (showKeyspace) {
                guesser.showKeyspace(maxKeyspace, System.out);
            }
            else {
                if (outputPath.equals("-")) {
                    guesser.generateGuesses(skip, limit, System.out);
                    return ExitCode.OK;
                }
                try (final PrintStream output = new PrintStream(new BufferedOutputStream(new FileOutputStream(Paths.get(outputPath).toFile())))) {
                    guesser.generateGuesses(skip, limit, output);
                }
            }
        }
        catch (final Throwable t) {
            LoggerFactory.getLogger(PcfgGuesserCli.class).error("Fatal error", t);
            // TODO: hack to kill piping to Hashcat
            System.err.println("Fatal error: " + t.getMessage());
            return ExitCode.SOFTWARE;
        }

        return ExitCode.OK;
    }
}
