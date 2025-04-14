package nl.nfi.djpcfg.common.stream;

import nl.nfi.djpcfg.Utils.Tuple;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

class BitInputStreamTest {

    @ParameterizedTest
    @MethodSource("generateReadTestCases")
    void read(final byte[] bitstream, final int readCount, final int[] expectedBits) throws IOException {
        final int[] bits = new int[readCount];
        try (final BitInputStream input = BitInputStream.forInput(bitstream)) {
            for (int i = 0; i < readCount; i++) {
                bits[i] = input.read();
            }
        }
        assertThat(bits).isEqualTo(expectedBits);
    }

    static Stream<Arguments> generateReadTestCases() {
        final Map<Tuple<byte[], Integer>, int[]> testCases = new LinkedHashMap<>();
        // 1
        testCases.put(Tuple.of(input(), 0), bits());
        testCases.put(Tuple.of(input(0b10000000), 1), bits(1));
        testCases.put(Tuple.of(input(0b10000000), 2), bits(1, 0));
        testCases.put(Tuple.of(input(0b10000000), 8), bits(1, 0, 0, 0, 0, 0, 0, 0));
        // 5
        testCases.put(Tuple.of(input(0b10010011, 0b00010010), 0), bits());
        testCases.put(Tuple.of(input(0b10010011, 0b00010010), 1), bits(1));
        testCases.put(Tuple.of(input(0b10010011, 0b00010010), 2), bits(1, 0));
        testCases.put(Tuple.of(input(0b10010011, 0b00010010), 3), bits(1, 0, 0));
        testCases.put(Tuple.of(input(0b10010011, 0b00010010), 4), bits(1, 0, 0, 1));
        // 10
        testCases.put(Tuple.of(input(0b10010011, 0b00010010), 5), bits(1, 0, 0, 1, 0));
        testCases.put(Tuple.of(input(0b10010011, 0b00010010), 6), bits(1, 0, 0, 1, 0, 0));
        testCases.put(Tuple.of(input(0b10010011, 0b00010010), 7), bits(1, 0, 0, 1, 0, 0, 1));
        testCases.put(Tuple.of(input(0b10010011, 0b00010010), 8), bits(1, 0, 0, 1, 0, 0, 1, 1));
        testCases.put(Tuple.of(input(0b10010011, 0b00010010), 9), bits(1, 0, 0, 1, 0, 0, 1, 1, 0));
        // 15
        testCases.put(Tuple.of(input(0b10010011, 0b00010010), 10), bits(1, 0, 0, 1, 0, 0, 1, 1, 0, 0));
        testCases.put(Tuple.of(input(0b10010011, 0b00010010), 11), bits(1, 0, 0, 1, 0, 0, 1, 1, 0, 0, 0));
        testCases.put(Tuple.of(input(0b10010011, 0b00010010), 12), bits(1, 0, 0, 1, 0, 0, 1, 1, 0, 0, 0, 1));
        testCases.put(Tuple.of(input(0b10010011, 0b00010010), 13), bits(1, 0, 0, 1, 0, 0, 1, 1, 0, 0, 0, 1, 0));
        testCases.put(Tuple.of(input(0b10010011, 0b00010010), 14), bits(1, 0, 0, 1, 0, 0, 1, 1, 0, 0, 0, 1, 0, 0));
        // 20
        testCases.put(Tuple.of(input(0b10010011, 0b00010010), 15), bits(1, 0, 0, 1, 0, 0, 1, 1, 0, 0, 0, 1, 0, 0, 1));
        testCases.put(Tuple.of(input(0b10010011, 0b00010010), 16), bits(1, 0, 0, 1, 0, 0, 1, 1, 0, 0, 0, 1, 0, 0, 1, 0));
        testCases.put(Tuple.of(input(0b10010011, 0b00010010, 0b10000000), 17), bits(1, 0, 0, 1, 0, 0, 1, 1, 0, 0, 0, 1, 0, 0, 1, 0, 1));
        testCases.put(Tuple.of(input(0b10010011, 0b00010010, 0b10000000), 18), bits(1, 0, 0, 1, 0, 0, 1, 1, 0, 0, 0, 1, 0, 0, 1, 0, 1, 0));

        return testCases.entrySet().stream()
            .map(entry -> {
                final Tuple<byte[], Integer> input = entry.getKey();
                final byte[] bitStream = input.left();
                final int readCount = input.right();
                final int[] expectedBits = entry.getValue();

                final String[] bitStreamDisplay = new String[bitStream.length];
                for (int i = 0; i < bitStream.length; i++) {
                    bitStreamDisplay[i] = byteToBinaryString(bitStream[i]);
                }
                final String[] expectedBitsDisplay = new String[expectedBits.length];
                for (int i = 0; i < expectedBits.length; i++) {
                    expectedBitsDisplay[i] = expectedBits[i] + "";
                }

                return argumentSet(
                    String.join(" ", bitStreamDisplay) + " read(" + readCount + ") => " + String.join(" ", expectedBitsDisplay),
                    bitStream,
                    readCount,
                    expectedBits
                );
            });
    }

    private static byte[] input(final int... bytes) {
        final byte[] converted = new byte[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            converted[i] = (byte) bytes[i];
        }
        return converted;
    }

    private static int[] bits(final int... bits) {
        return bits;
    }

    public static String byteToBinaryString(final int uByte) {
        return String.format("%8s", Integer.toBinaryString(uByte & 0xff)).replaceAll(" ", "0");
    }
}