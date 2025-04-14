package nl.nfi.djpcfg.serialize.v0_0_7;

import nl.nfi.djpcfg.guess.pcfg.grammar.Grammar;
import nl.nfi.djpcfg.guess.pcfg.grammar.TerminalGroup;
import nl.nfi.djpcfg.serialize.common.JreTypeCodec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static nl.nfi.djpcfg.serialize.v0_0_7.GrammarCodec.Decoder;
import static nl.nfi.djpcfg.serialize.v0_0_7.GrammarCodec.Encoder;

sealed class GrammarCodec permits Encoder, Decoder {

    static Encoder encodeUsing(final JreTypeCodec.Encoder encoder) {
        return new Encoder(encoder);
    }

    static Decoder decodeUsing(final JreTypeCodec.Decoder decoder) {
        return new Decoder(decoder);
    }

    static final class Encoder extends GrammarCodec {

        private final JreTypeCodec.Encoder encoder;

        private Encoder(final JreTypeCodec.Encoder encoder) {
            this.encoder = encoder;
        }

        public void write(final Grammar grammar) throws IOException {
            encoder.writeCollection(grammar.sections(), section -> {
                encoder.writeString(section);

                encoder.writeList(grammar.getSection(section), terminalGroup -> {
                    encoder.writeDouble(terminalGroup.probability());
                    encoder.writeList(terminalGroup.values(), encoder::writeString);
                });
            });
        }
    }

    static final class Decoder extends GrammarCodec {

        private final JreTypeCodec.Decoder decoder;

        private Decoder(final JreTypeCodec.Decoder decoder) {
            this.decoder = decoder;
        }

        public Grammar read() throws IOException {
            final Grammar grammar = Grammar.empty();

            final long sectionCount = decoder.readVarInt();
            for (long x = 0; x < sectionCount; x++) {
                final String section = decoder.readString();

                final long sectionSize = decoder.readVarInt();
                final List<TerminalGroup> terminalGroups = new ArrayList<>();

                for (long y = 0; y < sectionSize; y++) {
                    final double probability = decoder.readDouble();

                    final long terminalGroupSize = decoder.readVarInt();
                    final List<String> values = new ArrayList<>();

                    for (long z = 0; z < terminalGroupSize; z++) {
                        values.add(decoder.readString());
                    }
                    terminalGroups.add(new TerminalGroup(probability, values));
                }
                grammar.addSection(section, terminalGroups);
            }
            return grammar;
        }
    }
}
