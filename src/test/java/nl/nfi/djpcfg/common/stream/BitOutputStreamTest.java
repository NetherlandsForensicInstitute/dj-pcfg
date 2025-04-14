package nl.nfi.djpcfg.common.stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

class BitOutputStreamTest {

    @ParameterizedTest
    @MethodSource("generateWriteIntTestCases")
    void writeIntBits(final int[] writes, final byte[] expectedBytes) throws IOException {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (final BitOutputStream output = BitOutputStream.forOutput(bytes)) {
            for (int i = 0; i < writes.length / 2; i++) {
                output.write(writes[i * 2 + 0], writes[i * 2 + 1]);
            }
        }
        assertThat(bytes.toByteArray()).isEqualTo(expectedBytes);
    }

    static Stream<Arguments> generateWriteIntTestCases() {
        final Map<int[], byte[]> testCases = new LinkedHashMap<>();
        // 1
        testCases.put(writes(), expected());
        testCases.put(writes(0b00000000, 0), expected());
        testCases.put(writes(0b11111111, 0), expected());
        testCases.put(writes(0b00000000, 1), expected(0b00000000));
        // 5
        testCases.put(writes(0b00000001, 1), expected(0b10000000));
        testCases.put(writes(0b00000011, 4), expected(0b00110000));
        testCases.put(writes(0b10000011, 8), expected(0b10000011));
        testCases.put(writes(0b00000001, 1, 0b00000010, 3), expected(0b10100000));
        testCases.put(writes(0b00000001, 1, 0b00000010, 3, 0b00000011, 4), expected(0b10100011));
        // 10
        testCases.put(writes(0b10101010, 8, 0b00000001, 1), expected(0b10101010, 0b10000000));
        testCases.put(writes(0b11110000, 8, 0b00001111, 8), expected(0b11110000, 0b00001111));
        testCases.put(writes(0b11110000, 8, 0b00001111, 8, 0b00001111, 4), expected(0b11110000, 0b00001111, 0b11110000));

        testCases.put(writes(0b00000001, 1), expected(0b10000000));
        testCases.put(writes(0b00000001, 1, 0b00000001, 1), expected(0b11000000));
        // 15
        testCases.put(writes(0b00000001, 1, 0b00000001, 1, 0b00000001, 1), expected(0b11100000));
        testCases.put(writes(0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1), expected(0b11110000));
        testCases.put(writes(0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1), expected(0b11111000));
        testCases.put(writes(0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1), expected(0b11111100));
        testCases.put(writes(0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1), expected(0b11111110));
        // 20
        testCases.put(writes(0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1), expected(0b11111111));
        testCases.put(writes(0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1), expected(0b11111111, 0b10000000));
        testCases.put(writes(0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1), expected(0b11111111, 0b11000000));

//        testCases.put(writes(0x12345678, 9), expected(0x04, 0x68, 0xac, 0xf0));
//        testCases.put(writes(0x12345678, 15), expected(0x04, 0x68, 0xac, 0xf0));
//        testCases.put(writes(0x12345678, 16), expected(0x04, 0x68, 0xac, 0xf0));
//        testCases.put(writes(0x12345678, 17), expected(0x04, 0x68, 0xac, 0xf0));
//        testCases.put(writes(0x12345678, 23), expected(0x04, 0x68, 0xac, 0xf0));
//        testCases.put(writes(0x12345678, 24), expected(0x04, 0x68, 0xac, 0xf0));
        testCases.put(writes(0x12345678, 25), expected(0x1a, 0x2b, 0x3c, 0x00));
        testCases.put(writes(0x12345678, 31), expected(0x24, 0x68, 0xac, 0xf0));
        testCases.put(writes(0x12345678, 32), expected(0x12, 0x34, 0x56, 0x78));

        return testCases.entrySet().stream()
            .map(entry -> {
                final int[] writes = entry.getKey();
                final byte[] expected = entry.getValue();

                final String[] writesDisplay = new String[writes.length / 2];
                for (int i = 0; i < writes.length / 2; i++) {
                    writesDisplay[i] = "<" + longToBinaryString(writes[i * 2 + 0] & 0xffffffffL).substring(64 - writes[i * 2 + 1], 64) + ">";
                }
                final String[] expectedDisplay = new String[expected.length];
                for (int i = 0; i < expected.length; i++) {
                    expectedDisplay[i] = byteToBinaryString(expected[i] & 0xff);
                }

                return argumentSet(
                    String.join(" ", writesDisplay) + " => " + String.join(" ", expectedDisplay),
                    writes,
                    expected
                );
            });
    }

    @ParameterizedTest
    @MethodSource("generateWriteLongTestCases")
    void writeLongBits(final long[] writes, final byte[] expectedBytes) throws IOException {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (final BitOutputStream output = BitOutputStream.forOutput(bytes)) {
            for (int i = 0; i < writes.length / 2; i++) {
                output.write(writes[i * 2 + 0], (int) writes[i * 2 + 1]);
            }
        }
        assertThat(bytes.toByteArray()).isEqualTo(expectedBytes);
    }

    static Stream<Arguments> generateWriteLongTestCases() {
        final Map<long[], byte[]> testCases = new LinkedHashMap<>();
        // 1
        testCases.put(writes(new long[0]), expected());
        testCases.put(writes(0b00000000L, 0), expected());
        testCases.put(writes(0b11111111L, 0), expected());
        testCases.put(writes(0b00000000L, 1), expected(0b00000000));
        // 5
        testCases.put(writes(0b00000001L, 1), expected(0b10000000));
        testCases.put(writes(0b00000011L, 4), expected(0b00110000));
        testCases.put(writes(0b10000011L, 8), expected(0b10000011));
        testCases.put(writes(0b00000001L, 1, 0b00000010, 3), expected(0b10100000));
        testCases.put(writes(0b00000001L, 1, 0b00000010, 3, 0b00000011, 4), expected(0b10100011));
        // 10
        testCases.put(writes(0b10101010L, 8, 0b00000001, 1), expected(0b10101010, 0b10000000));
        testCases.put(writes(0b11110000L, 8, 0b00001111, 8), expected(0b11110000, 0b00001111));
        testCases.put(writes(0b11110000L, 8, 0b00001111, 8, 0b00001111, 4), expected(0b11110000, 0b00001111, 0b11110000));

        testCases.put(writes(0b00000001L, 1), expected(0b10000000));
        testCases.put(writes(0b00000001L, 1, 0b00000001, 1), expected(0b11000000));
        // 15
        testCases.put(writes(0b00000001L, 1, 0b00000001, 1, 0b00000001, 1), expected(0b11100000));
        testCases.put(writes(0b00000001L, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1), expected(0b11110000));
        testCases.put(writes(0b00000001L, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1), expected(0b11111000));
        testCases.put(writes(0b00000001L, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1), expected(0b11111100));
        testCases.put(writes(0b00000001L, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1), expected(0b11111110));
        // 20
        testCases.put(writes(0b00000001L, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1), expected(0b11111111));
        testCases.put(writes(0b00000001L, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1), expected(0b11111111, 0b10000000));
        testCases.put(writes(0b00000001L, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1, 0b00000001, 1), expected(0b11111111, 0b11000000));

        testCases.put(writes(0x02345678L, 31), expected(0x04, 0x68, 0xac, 0xf0));
        testCases.put(writes(0x12345678L, 32), expected(0x12, 0x34, 0x56, 0x78));
        // 25
        testCases.put(writes(0x1234567812345678L, 64), expected(0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78));

        return testCases.entrySet().stream()
            .map(entry -> {
                final long[] writes = entry.getKey();
                final byte[] expected = entry.getValue();

                final String[] writesDisplay = new String[writes.length / 2];
                for (int i = 0; i < writes.length / 2; i++) {
                    writesDisplay[i] = "<" + longToBinaryString(writes[i * 2 + 0]).substring((int) (64 - writes[i * 2 + 1]), 64) + ">";
                }
                final String[] expectedDisplay = new String[expected.length];
                for (int i = 0; i < expected.length; i++) {
                    expectedDisplay[i] = byteToBinaryString(expected[i] & 0xff);
                }

                return argumentSet(
                    String.join(" ", writesDisplay) + " => " + String.join(" ", expectedDisplay),
                    writes,
                    expected
                );
            });
    }

    private static int[] writes(final int... writes) {
        return writes;
    }

    private static long[] writes(final long... writes) {
        return writes;
    }

    private static byte[] expected(final int... bytes) {
        final byte[] converted = new byte[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            converted[i] = (byte) bytes[i];
        }
        return converted;
    }

    public static String byteToBinaryString(final int uByte) {
        return String.format("%8s", Integer.toBinaryString(uByte & 0xff)).replaceAll(" ", "0");
    }

    public static String longToBinaryString(final long bits) {
        return String.format("%64s", Long.toBinaryString(bits)).replaceAll(" ", "0");
    }
}