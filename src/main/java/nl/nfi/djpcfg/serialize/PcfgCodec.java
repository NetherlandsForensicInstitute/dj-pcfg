package nl.nfi.djpcfg.serialize;

import nl.nfi.djpcfg.guess.pcfg.Pcfg;
import nl.nfi.djpcfg.guess.pcfg.RuleInfo;
import nl.nfi.djpcfg.guess.pcfg.grammar.BaseStructure;
import nl.nfi.djpcfg.guess.pcfg.grammar.Grammar;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static nl.nfi.djpcfg.serialize.PcfgCodec.Decoder;
import static nl.nfi.djpcfg.serialize.PcfgCodec.Encoder;

public abstract sealed class PcfgCodec implements Closeable permits Encoder, Decoder {

    private static final String MAGIC = "pbm";

    public static Encoder forOutput(final OutputStream output) {
        return new Encoder(JreTypeCodec.forOutput(output));
    }

    public static Decoder forInput(final Path path) throws IOException {
        return new Decoder(JreTypeCodec.forInput(new BufferedInputStream(new FileInputStream(path.toFile()))));
    }

    public static Decoder forInput(final InputStream input) {
        return new Decoder(JreTypeCodec.forInput(input));
    }

    public static final class Encoder extends PcfgCodec {

        private final JreTypeCodec.Encoder encoder;

        private Encoder(final JreTypeCodec.Encoder encoder) {
            this.encoder = encoder;
        }

        public void write(final Pcfg pcfg) throws IOException {
            encoder.writeString(MAGIC);
            encoder.writeString(Config.SERIALIZED_FORMAT_VERSION);

            final RuleInfo ruleInfo = pcfg.ruleInfo();
            encoder.writeString(ruleInfo.uuid().toString());
            // TODO: change to path of serialized model?
            encoder.writeString(ruleInfo.basePath().toString());
            encoder.writeString(ruleInfo.version());
            encoder.writeString(ruleInfo.encoding().toString());

            final Grammar grammar = pcfg.grammar();
            GrammarCodec.encodeUsing(encoder).write(grammar);

            encoder.writeList(pcfg.baseStructures(), baseStructure -> {
                encoder.writeDouble(baseStructure.probability());
                encoder.writeList(baseStructure.variables(), encoder::writeString);
            });
        }

        @Override
        public void close() throws IOException {
            encoder.close();
        }
    }

    public static final class Decoder extends PcfgCodec {

        private final JreTypeCodec.Decoder decoder;

        private Decoder(final JreTypeCodec.Decoder decoder) {
            this.decoder = decoder;
        }

        public Pcfg read() throws IOException {
            final String magic = decoder.readString();
            if (!magic.equals(MAGIC)) {
                throw new UnsupportedOperationException(magic);
            }

            final String version = decoder.readString();
            if (!version.equals(Config.SERIALIZED_FORMAT_VERSION)) {
                throw new UnsupportedOperationException(version);
            }

            final RuleInfo ruleInfo = new RuleInfo(
                    UUID.fromString(decoder.readString()),
                    Paths.get(decoder.readString()),
                    decoder.readString(),
                    Charset.forName(decoder.readString())
            );
            final Grammar grammar = GrammarCodec.decodeUsing(decoder).read();

            final int baseStructureCount = decoder.readVarInt();
            final List<BaseStructure> baseStructures = new ArrayList<>(baseStructureCount);
            for (long x = 0; x < baseStructureCount; x++) {
                final double probability = decoder.readDouble();
                final int replacementCount = decoder.readVarInt();
                final List<String> replacements = new ArrayList<>(replacementCount);
                for (long y = 0; y < replacementCount; y++) {
                    replacements.add(decoder.readString());
                }
                baseStructures.add(new BaseStructure(probability, replacements));
            }
            return Pcfg.create(ruleInfo, grammar, baseStructures);
        }

        @Override
        public void close() throws IOException {
            decoder.close();
        }
    }
}
