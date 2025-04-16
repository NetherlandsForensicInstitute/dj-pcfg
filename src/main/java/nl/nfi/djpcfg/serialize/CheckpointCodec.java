package nl.nfi.djpcfg.serialize;

import nl.nfi.djpcfg.guess.cache.Checkpoint;
import nl.nfi.djpcfg.guess.pcfg.HeapLimitingPTQueue;
import nl.nfi.djpcfg.guess.pcfg.ParseTree;
import nl.nfi.djpcfg.guess.pcfg.Pcfg;
import nl.nfi.djpcfg.guess.pcfg.ReplacementSet;
import nl.nfi.djpcfg.guess.pcfg.grammar.BaseStructure;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import static java.util.Comparator.comparing;
import static nl.nfi.djpcfg.serialize.CheckpointCodec.Decoder;
import static nl.nfi.djpcfg.serialize.CheckpointCodec.Encoder;

public abstract sealed class CheckpointCodec implements Closeable permits Encoder, Decoder {

    private static final String MAGIC = "pbc";

    public static Encoder forOutput(final Path path) throws IOException {
        return new Encoder(JreTypeCodec.forOutput(new BufferedOutputStream(new FileOutputStream(path.toFile()))));
    }

    public static Encoder forOutput(final OutputStream output) {
        return new Encoder(JreTypeCodec.forOutput(output));
    }

    public static Decoder forInput(final Path path) throws IOException {
        return new Decoder(JreTypeCodec.forInput(new BufferedInputStream(new FileInputStream(path.toFile()))));
    }

    public static Decoder forInput(final InputStream input) {
        return new Decoder(JreTypeCodec.forInput(input));
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

            encoder.writeString(MAGIC);
            encoder.writeString(Config.SERIALIZED_FORMAT_VERSION);

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

            final String magic = decoder.readString();
            if (!magic.equals(MAGIC)) {
                throw new UnsupportedOperationException(magic);
            }

            final String version = decoder.readString();
            if (!version.equals(Config.SERIALIZED_FORMAT_VERSION)) {
                throw new UnsupportedOperationException(version);
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