package nl.nfi.djpcfg.guess.cache.distributed;

import static java.lang.Runtime.getRuntime;
import static java.util.concurrent.Executors.newFixedThreadPool;

import static picocli.CommandLine.Command;
import static picocli.CommandLine.ExitCode;
import static picocli.CommandLine.Option;

import java.nio.file.Paths;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;

import org.slf4j.LoggerFactory;

@Command(name = "pcfg_cache_server")
public class CacheServerCli implements Callable<Integer> {

    @Option(names = {"--port"}, description = "Port to start listening on", required = true)
    private int port;

    @Option(names = {"--cache_directory_path"}, description = "Load/store state in given cache directory", required = true)
    private String cachePath = null;

    @Option(names = {"--rpc_thread_count"}, description = "Use <count> threads for processing client requests")
    private int threadCount = getRuntime().availableProcessors();

    @Option(names = {"--log_directory_path"}, description = "Directory where to store live and archived log files")
    private String logPath;

    @Override
    public Integer call() throws Exception {
        // TODO: fix hack
        if (logPath != null) {
            System.setProperty("LOG_DIRECTORY_PATH", logPath);
        }

        try (final CheckpointCacheServer server = CheckpointCacheServer.start(port, Paths.get(cachePath), newFixedThreadPool(threadCount))) {
            System.out.println("Server started...");

            // let's just block the main thread (perhaps it would be nicer to let the main thread
            // finish and instead make the worker threads non-daemon, something to look at...)
            final Semaphore semaphore = new Semaphore(0);
            semaphore.acquire();

            // should never be reached
            return ExitCode.OK;
        }
        catch (final Throwable t) {
            LoggerFactory.getLogger(CacheServerCli.class).error("Fatal error", t);
            return ExitCode.SOFTWARE;
        }
    }
}
