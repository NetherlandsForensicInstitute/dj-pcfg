package nl.nfi.djpcfg.serialize;

import nl.nfi.djpcfg.common.Timers;
import nl.nfi.djpcfg.common.Timers.TimedResult;
import nl.nfi.djpcfg.guess.cache.Checkpoint;
import nl.nfi.djpcfg.guess.cache.CheckpointCache;
import nl.nfi.djpcfg.guess.cache.directory.DirectoryCheckpointCache;
import nl.nfi.djpcfg.guess.pcfg.Pcfg;
import nl.nfi.djpcfg.guess.pcfg.PcfgGuesser;
import nl.nfi.djpcfg.guess.pcfg.generate.Mode;
import nl.nfi.djpcfg.serialize.common.JreTypeCodec;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;

import static java.io.OutputStream.nullOutputStream;
import static nl.nfi.djpcfg.common.Formatting.toHumanReadableSize;
import static nl.nfi.djpcfg.common.Timers.time;

class CheckpointCodecTest {

    // 100 000 000 000 / 100 000

//    @Disabled
    @Test
    void size() throws IOException {
        final Path rulePath = Paths.get("src/test/resources/rules/Default.pbm");
         final Path inputPath = Paths.get("src/test/resources/checkpoints/384adebf-ba38-4307-891d-5b4a0a02178b_999998816");

        final ByteArrayOutputStream v0_0_7 = new ByteArrayOutputStream();
        final ByteArrayOutputStream v0_4_0 = new ByteArrayOutputStream();

        try (final PcfgCodec.Decoder pcfgDecoder = PcfgCodec.forInput(rulePath)) {
            final Pcfg pcfg = pcfgDecoder.read();

            final Checkpoint checkpoint;

            final JreTypeCodec.Decoder decoderBase = JreTypeCodec.forInput(inputPath);
            decoderBase.readString(); // magic
            decoderBase.readString(); // version

            try (final nl.nfi.djpcfg.serialize.v0_0_7.CheckpointCodec.Decoder decoder = nl.nfi.djpcfg.serialize.v0_0_7.CheckpointCodec.Decoder.decodeUsing(decoderBase)) {
                final TimedResult<Checkpoint> oldRead = time(() -> decoder.readCheckpointUsingBaseRefs(pcfg));
                System.out.println(" Read checkpoint 0.0.7: " + oldRead.duration());
                checkpoint = oldRead.value();
            }
            try (final nl.nfi.djpcfg.serialize.v0_0_7.CheckpointCodec.Encoder encoder = nl.nfi.djpcfg.serialize.v0_0_7.CheckpointCodec.Encoder.encodeUsing(JreTypeCodec.forOutput(v0_0_7))) {
                final Duration oldWrite = time(() -> encoder.writeCheckpointUsingBaseRefs(pcfg, checkpoint));
                System.out.println("Write checkpoint 0.0.7: " + oldWrite);
            }

            try (final nl.nfi.djpcfg.serialize.v0_4_0.CheckpointCodec.Encoder encoder = nl.nfi.djpcfg.serialize.v0_4_0.CheckpointCodec.encodeUsing(JreTypeCodec.forOutput(v0_4_0))) {
                final Duration newWrite = time(() -> encoder.writeCheckpointUsingBaseRefs(pcfg, checkpoint));
                System.out.println("Write checkpoint 0.4.0: " + newWrite);
            }
            try (final nl.nfi.djpcfg.serialize.v0_4_0.CheckpointCodec.Decoder encoder = nl.nfi.djpcfg.serialize.v0_4_0.CheckpointCodec.decodeUsing(JreTypeCodec.forInput(new ByteArrayInputStream(v0_4_0.toByteArray())))) {
                final TimedResult<Checkpoint> newRead = time(() -> encoder.readCheckpointUsingBaseRefs(pcfg));
                final Checkpoint checkpoint_v0_4_0 = newRead.value();
                System.out.println(" Read checkpoint 0.4.0: " + newRead.duration());
                // System.out.println("Checkpoint remains unchanged: " + checkpoint.equals(checkpoint_v0_4_0));
            }
            System.out.println(toHumanReadableSize(v0_0_7.size()));
            System.out.println(toHumanReadableSize(v0_4_0.size()));
            System.out.println("Average of %.2f bytes per entry in the queue".formatted(v0_4_0.size() * 1.0d / checkpoint.queue().size()));

        }

        // Read checkpoint 0.0.7: PT9.76188727S
        //Write checkpoint 0.0.7: PT7.148435045S
        //Compressed 233798061 indices from 891.9MiB to 107.6MiB
        //Write checkpoint 0.4.0: PT28.712268997S
        // Read checkpoint 0.4.0: PT11.188733109S
        //true
        //541.1MiB
        //196.3MiB
        //Average of 6.81 bytes per entry in the queue
    }

    @Disabled
    @Test
    void generateIt() throws IOException {
        final Path rulePath = Paths.get("src/test/resources/rules/Default.pbm");

        System.setProperty("LOG_DIRECTORY_PATH", "logs");

        final CheckpointCache cache = DirectoryCheckpointCache.createOrLoadFrom(Paths.get("directory"));
        final PcfgGuesser guesser = PcfgGuesser.forRule(rulePath)
                .mode(Mode.TRUE_PROB_ORDER)
                .threadCount(4)
                .cache(cache);

        for (int i = 0; i < 32; i++) {
            guesser.generateGuesses(i * 1000000000L, 1, new PrintStream(nullOutputStream()));
            System.out.println(i);
        }
    }
}