package nl.nfi.djpcfg.guess.pcfg.generate;

import static java.util.concurrent.TimeUnit.SECONDS;

import static org.assertj.core.api.Assertions.assertThat;

import static nl.nfi.djpcfg.Utils.CountingPrintStream;
import static nl.nfi.djpcfg.serialize.PcfgCodec.Decoder;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import nl.nfi.djpcfg.guess.cache.Checkpoint;
import nl.nfi.djpcfg.guess.pcfg.Pcfg;
import nl.nfi.djpcfg.guess.pcfg.PcfgQueue;
import nl.nfi.djpcfg.serialize.PcfgCodec;

class ThreadedTrueProbOrderTest {

    private static final Path TEST_RESOURCES_PATH = Paths.get("src/test/resources").toAbsolutePath();

    @Test
    @Timeout(value = 32, unit = SECONDS)
    void generatingWithHugeLimitDoesNotRunOutOfMemory() throws Exception {
        try (final Decoder decoder = PcfgCodec.forInput(TEST_RESOURCES_PATH.resolve("rules/Default.pbm"))) {
            final Pcfg pcfg = decoder.read();
            final PcfgQueue queue = PcfgQueue.fromStart(pcfg);
            final Checkpoint checkpoint = new Checkpoint(queue.queue(), queue.next(), 0);

            final CountingPrintStream output = new CountingPrintStream();

            // start generating in separate thread
            new Thread(() ->
                ThreadedTrueProbOrder.init(pcfg, checkpoint)
                    .maxProducerThreads(4)
                    .writeGuesses(0, Long.MAX_VALUE, output)
            ).start();

            // 2 seconds should be more than enough to have generated some passwords
            Thread.sleep(2000);

            // not sure about memory visibility however... Thread#sleep has no happens-before relationship...
           assertThat(output.count()).isGreaterThan(0);
        }
    }
}