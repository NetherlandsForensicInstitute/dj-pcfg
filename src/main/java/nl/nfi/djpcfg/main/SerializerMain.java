package nl.nfi.djpcfg.main;

import nl.nfi.djpcfg.serialize.PcfgSerializerCli;
import picocli.CommandLine;

import java.io.IOException;

public final class SerializerMain {

    public static void main(final String... args) throws IOException {
        final int exitCode = new CommandLine(new PcfgSerializerCli()).execute(args);
        System.exit(exitCode);
    }
}