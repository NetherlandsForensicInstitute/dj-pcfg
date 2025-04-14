package nl.nfi.djpcfg.debug;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import nl.nfi.djpcfg.guess.cache.Checkpoint;
import nl.nfi.djpcfg.guess.pcfg.ParseTree;
import nl.nfi.djpcfg.guess.pcfg.Pcfg;
import nl.nfi.djpcfg.serialize.CheckpointCodec;
import nl.nfi.djpcfg.serialize.PcfgCodec;

public final class PrintCheckpointMetadata {

    public static void main(final String... args) throws IOException {
        //     offset: 999998816
        // len(queue): 1338345
        //       next:
        //         baseProb: 4.4046248560989037E-5
        //             prob: 1.8971615538769486E-11
        //          replace: A5 C5 K4 [200, 0, 19]
        //
        // Average of 16.68 bytes per entry in the queue
        final Path pcfgPath = Paths.get("src/test/resources/rules/Default.pbm");
        final Path checkpointPath = Paths.get("src/test/resources/checkpoints/384adebf-ba38-4307-891d-5b4a0a02178b_999998816");

        final Pcfg pcfg = PcfgCodec.forInput(pcfgPath).read();
        final Checkpoint checkpoint = CheckpointCodec.forInput(checkpointPath).readCheckpointUsingBaseRefs(pcfg);
        final ParseTree next = checkpoint.next();

        System.out.printf("    offset: %d%n", checkpoint.keyspacePosition());
        System.out.printf("len(queue): %d%n", checkpoint.queue().size());
        System.out.printf("      next: %n");
        System.out.printf("        baseProb: %s%n",  Double.toString(next.baseProbability()));
        System.out.printf("            prob: %s%n",  Double.toString(next.probability()));
        System.out.printf("         replace: %s %s%n",  next.replacementSet().actualStructure(), Arrays.toString(next.replacementSet().indices()));
        System.out.println();
        System.out.println("Average of %.2f bytes per entry in the queue".formatted(Files.size(checkpointPath) * 1.0d / checkpoint.queue().size()));
    }
}
