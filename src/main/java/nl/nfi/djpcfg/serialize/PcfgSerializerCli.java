package nl.nfi.djpcfg.serialize;

import nl.nfi.djpcfg.guess.pcfg.Pcfg;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.concurrent.Callable;

import static picocli.CommandLine.Command;
import static picocli.CommandLine.ExitCode;
import static picocli.CommandLine.Option;

@Command(name = "pcfg_serializer")
public class PcfgSerializerCli implements Callable<Integer> {

    @Option(names = {"--input"}, description = "The directory of the PCFG rule to serialize", required = true)
    private String inputPath;

    @Option(names = {"--output"}, description = "The file to write the serialized binary rule to (defaults to [inputPath].pbm)")
    private String outputPath = null;

    @Override
    public Integer call() throws Exception {
        final String outputPath = this.outputPath == null
                ? inputPath + ".pbm"
                : this.outputPath;

        final Pcfg pcfg = Pcfg.loadFrom(Paths.get(inputPath));
        try (final OutputStream output = new BufferedOutputStream(new FileOutputStream(outputPath))) {
            PcfgCodec.forOutput(output).write(pcfg);
        }

        return ExitCode.OK;
    }
}