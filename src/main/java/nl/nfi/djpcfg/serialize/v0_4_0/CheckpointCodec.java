package nl.nfi.djpcfg.serialize.v0_4_0;

import nl.nfi.djpcfg.common.stream.BitInputStream;
import nl.nfi.djpcfg.common.stream.BitOutputStream;
import nl.nfi.djpcfg.guess.cache.Checkpoint;
import nl.nfi.djpcfg.guess.pcfg.HeapLimitingPTQueue;
import nl.nfi.djpcfg.guess.pcfg.ParseTree;
import nl.nfi.djpcfg.guess.pcfg.Pcfg;
import nl.nfi.djpcfg.guess.pcfg.ReplacementSet;
import nl.nfi.djpcfg.guess.pcfg.grammar.BaseStructure;
import nl.nfi.djpcfg.serialize.common.JreTypeCodec;
import nl.nfi.djpcfg.serialize.compression.Code;
import nl.nfi.djpcfg.serialize.compression.CodeTable;
import nl.nfi.djpcfg.serialize.compression.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.BitShuffle;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.SequencedMap;

import static java.util.Comparator.comparing;
import static java.util.Comparator.comparingDouble;
import static java.util.Comparator.comparingLong;
import static nl.nfi.djpcfg.serialize.v0_4_0.CheckpointCodec.Decoder;
import static nl.nfi.djpcfg.serialize.v0_4_0.CheckpointCodec.Encoder;

public abstract sealed class CheckpointCodec implements Closeable permits Encoder, Decoder {

    private static final Logger LOG = LoggerFactory.getLogger(CheckpointCodec.class);

    public static final String VERSION = "0.4.0";

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

            writeQueueState(state, lookup);
            writeParseTreeUsingBaseRefs(lookup, state.next());
            encoder.writeLong(state.keyspacePosition());
        }

        private void writeQueueState(final Checkpoint state, final Map<List<String>, Integer> lookup) throws IOException {
            final List<ParseTree> entries = state.queue()
                    .stream()
                     // .parallelStream() // TODO: calling this inside gRPC observer makes everything slow, thread contention?
                    .sorted(comparingDouble(ParseTree::probability))
                    .toList();

            // write the references to the base structure of each parse tree
            final int[] references = entries.stream()
                    .mapToInt(parseTree -> lookup.get(parseTree.replacementSet().variables()))
                    .toArray();

            encoder.writeBytes(Snappy.compress(BitShuffle.shuffle(references)));

            // write the probability of each parse tree
            final long[] probabilities = entries.stream()
                    .mapToLong(parseTree -> Double.doubleToLongBits(parseTree.probability()))
                    .toArray();

            // before compression, xor neighbours because most bits will be equal, so we get better compression
            for (int i = probabilities.length - 1; i >= 1; i--) {
                probabilities[i] ^= probabilities[i - 1];
            }

            encoder.writeBytes(Snappy.compress(probabilities));

            // create frequency table for each used index referring to a terminal group
            final Map<Integer, Integer> frequencyTable = new HashMap<>();
            for (final ParseTree parseTree : entries) {
                final int[] indices = parseTree.replacementSet().indices();
                // count occurrences of each index
                for (final int index : indices) {
                    frequencyTable.merge(index, 1, Integer::sum);
                }
            }

            // queue ordering nodes from least to most encountered
            final PriorityQueue<Node> queue = new PriorityQueue<>(comparingLong(Node::count));
            frequencyTable.forEach((key, count) -> {
                queue.add(Node.leaf(key, count));
            });

            // keep merging least encountered value nodes until we built the binary tree
            while (queue.size() > 1) {
                final Node l = queue.poll();
                final Node r = queue.poll();
                queue.add(Node.internal(l, r));
            }

            // now get the tree and build the code table
            final Node root = queue.poll();
            // table is mapping from value to prefix code
            // we also canonicalize it, so we only have to send the bit lengths
            final SequencedMap<Integer, Code> codeTable = CodeTable.canonicalize(CodeTable.createCodeTable(root));

            // write the values of the code table
            final int[] values = codeTable.keySet().stream()
                    .mapToInt(l -> l)
                    .toArray();

            encoder.writeBytes(Snappy.compress(BitShuffle.shuffle(values)));

            // write the bit lengths of each code
            final int[] bitcounts = codeTable.values().stream()
                    .mapToInt(Code::bitcount)
                    .toArray();

            encoder.writeBytes(Snappy.compress(BitShuffle.shuffle(bitcounts)));

            // write all the replacement indices data
            final ByteArrayOutputStream encodedBytes = new ByteArrayOutputStream();
            try (final BitOutputStream bitstream = BitOutputStream.forOutput(encodedBytes)) {
                for (final ParseTree parseTree : entries) {
                    final int[] indices = parseTree.replacementSet().indices();
                    for (final int index : indices) {
                        final Code indexCode = codeTable.get(index);
                        bitstream.write(indexCode.bits(), indexCode.bitcount());
                    }
                }
            }

            encoder.writeBytes(encodedBytes.toByteArray());
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

            final PriorityQueue<ParseTree> queue = readQueueState(lookup);
            final ParseTree next = readParseTreeUsingBaseRefs(lookup);
            final long pos = decoder.readLong();
            return new Checkpoint(queue, next, pos);
        }

        private PriorityQueue<ParseTree> readQueueState(final Map<Integer, BaseStructure> lookup) throws IOException {
            // i.e. pointers to the base structures of the grammar
            final int[] references = BitShuffle.unshuffleIntArray(Snappy.uncompress(decoder.readBytes()));
            final int queueSize = references.length;
            System.out.println("Queue size: " + queueSize);

            final long[] probabilities = Snappy.uncompressLongArray(decoder.readBytes());
            for (int i = 1; i < probabilities.length; i++) {
                probabilities[i] ^= probabilities[i - 1];
            }

            final int[] values = BitShuffle.unshuffleIntArray(Snappy.uncompress(decoder.readBytes()));
            final int[] bitcounts = BitShuffle.unshuffleIntArray(Snappy.uncompress(decoder.readBytes()));

            final Map<Code, Integer> codeTable = buildCodeTable(values, bitcounts);

            final byte[] encodedIndices = decoder.readBytes();

            final PriorityQueue<ParseTree> queue = new HeapLimitingPTQueue(queueSize, comparing(ParseTree::probability).reversed());
            try (final BitInputStream bitstream = BitInputStream.forInput(encodedIndices)) {
                for (int i = 0; i < queueSize; i++) {
                    final int reference = references[i];
                    final double probability = Double.longBitsToDouble(probabilities[i]);
                    final BaseStructure base = lookup.get(reference);
                    final int[] indices = new int[base.variables().size()];

                    for (int j = 0; j < indices.length; j++) {
                        indices[j] = findNextIndex(bitstream, codeTable);
                    }

                    queue.add(new ParseTree(
                            base.probability(),
                            probability,
                            new ReplacementSet(base.variables(), indices)
                    ));
                }
            }
            return queue;
        }

        private static Map<Code, Integer> buildCodeTable(final int[] values, final int[] bitcounts) {
            final Map<Code, Integer> codeTable = new HashMap<>();

            int prevBits = 0;
            int prevBitcount = bitcounts[0];

            for (int i = 0; i < bitcounts.length; i++) {
                final int curBitcount = bitcounts[i];
                if (curBitcount > prevBitcount) {
                    prevBits <<= curBitcount - prevBitcount;
                }
                codeTable.put(new Code(prevBits, curBitcount), values[i]);
                prevBits++;
                prevBitcount = curBitcount;
            }

            return codeTable;
        }

        private int findNextIndex(final BitInputStream bitstream, final Map<Code, Integer> codeTable) throws IOException {
            return findNextIndexRecursively(bitstream, codeTable, 0, 0);
        }

        private int findNextIndexRecursively(final BitInputStream bitstream, final Map<Code, Integer> codeTable, final int currentCode, final int bitcount) throws IOException {
            final int nextPossibleCode = currentCode << 1 | bitstream.read();
            final int curBitCount = bitcount + 1;
            final Integer possibleIndex = codeTable.get(new Code(nextPossibleCode, curBitCount));
            if (possibleIndex != null) {
                return possibleIndex;
            }
            return findNextIndexRecursively(bitstream, codeTable, nextPossibleCode, curBitCount);
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