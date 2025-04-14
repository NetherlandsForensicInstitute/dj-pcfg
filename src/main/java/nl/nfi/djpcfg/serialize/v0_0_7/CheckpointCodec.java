package nl.nfi.djpcfg.serialize.v0_0_7;

import nl.nfi.djpcfg.guess.cache.Checkpoint;
import nl.nfi.djpcfg.guess.pcfg.HeapLimitingPTQueue;
import nl.nfi.djpcfg.guess.pcfg.ParseTree;
import nl.nfi.djpcfg.guess.pcfg.Pcfg;
import nl.nfi.djpcfg.guess.pcfg.ReplacementSet;
import nl.nfi.djpcfg.guess.pcfg.grammar.BaseStructure;
import nl.nfi.djpcfg.serialize.common.JreTypeCodec;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import static java.util.Comparator.comparing;
import static nl.nfi.djpcfg.serialize.v0_0_7.CheckpointCodec.Decoder;
import static nl.nfi.djpcfg.serialize.v0_0_7.CheckpointCodec.Encoder;

public abstract sealed class CheckpointCodec implements Closeable permits Encoder, Decoder {

    public static final String VERSION = "0.0.7";

    public static Encoder encodeUsing(final JreTypeCodec.Encoder encoder) {
        return new Encoder(encoder);
    }

    public static Decoder decodeUsing(final JreTypeCodec.Decoder decoder) {
        return new Decoder(decoder);
    }

    public static final class Encoder extends CheckpointCodec {

        private final JreTypeCodec.Encoder encoder;

        private Encoder(final JreTypeCodec.Encoder encoder) {
            this.encoder = encoder;
        }

        public void writeCheckpointUsingBaseRefs(final Pcfg pcfg, final Checkpoint state) throws IOException {
            final Map<List<String>, Integer> lookup = new HashMap<>();
            final List<BaseStructure> baseStructures = pcfg.baseStructures();
            for (int i = 0; i < baseStructures.size(); i++) {
                final BaseStructure baseStructure = baseStructures.get(i);
                lookup.put(baseStructure.variables(), i);
            }

            encoder.writeCollection(state.queue(), parseTree -> writeParseTreeUsingBaseRefs(lookup, parseTree));
            writeParseTreeUsingBaseRefs(lookup, state.next());
            encoder.writeLong(state.keyspacePosition());
        }

        private void writeParseTreeUsingBaseRefs(final Map<List<String>, Integer> lookup, final ParseTree parseTree) throws IOException {
            encoder.writeVarInt(lookup.get(parseTree.replacementSet().variables()));
            encoder.writeDouble(parseTree.probability());
            encoder.writeVarIntArray(parseTree.replacementSet().indices());
        }

        @Override
        public void close() throws IOException {
            encoder.close();
        }
    }

    public static final class Decoder extends CheckpointCodec {

        private final JreTypeCodec.Decoder decoder;

        private Decoder(final JreTypeCodec.Decoder decoder) {
            this.decoder = decoder;
        }

        public Checkpoint readCheckpointUsingBaseRefs(final Pcfg pcfg) throws IOException {
            final Map<Integer, BaseStructure> lookup = new HashMap<>();
            final List<BaseStructure> baseStructures = pcfg.baseStructures();
            for (int i = 0; i < baseStructures.size(); i++) {
                final BaseStructure baseStructure = baseStructures.get(i);
                lookup.put(i, baseStructure);
            }

            final PriorityQueue<ParseTree> queue = decoder.readQueue(() -> readParseTreeUsingBaseRefs(lookup), size ->
                    new HeapLimitingPTQueue(size, comparing(ParseTree::probability).reversed())
            );
            final ParseTree next = readParseTreeUsingBaseRefs(lookup);
            final long pos = decoder.readLong();
            return new Checkpoint(queue, next, pos);
        }

        private ParseTree readParseTreeUsingBaseRefs(final Map<Integer, BaseStructure> lookup) throws IOException {
            final BaseStructure base = lookup.get(decoder.readVarInt());
            final double probability = decoder.readDouble();
            final int[] indices = decoder.readVarIntArray();

            return new ParseTree(
                    base.probability(),
                    probability,
                    new ReplacementSet(base.variables(), indices)
            );
        }

        @Override
        public void close() throws IOException {
            decoder.close();
        }
    }
}