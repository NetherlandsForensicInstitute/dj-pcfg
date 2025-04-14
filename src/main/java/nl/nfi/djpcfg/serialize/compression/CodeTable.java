package nl.nfi.djpcfg.serialize.compression;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SequencedMap;

import static java.util.Comparator.comparing;
import static java.util.Map.Entry.comparingByKey;
import static java.util.Map.Entry.comparingByValue;

public final class CodeTable {

    public static SequencedMap<Integer, Code> createCodeTable(final Node root) {
        final SequencedMap<Integer, Code> codeTable = new LinkedHashMap<>();
        fillCodeTableRecursively(root, 0, 0, codeTable);
        return codeTable;
    }

    private static void fillCodeTableRecursively(final Node node, final int bits, final int count, final Map<Integer, Code> codeTable) {
        if (node.isLeaf()) {
            codeTable.put(node.value(), new Code(bits, count));
        } else {
            fillCodeTableRecursively(node.l(), bits << 1 | 0, count + 1, codeTable);
            fillCodeTableRecursively(node.r(), bits << 1 | 1, count + 1, codeTable);
        }
    }

    public static SequencedMap<Integer, Code> canonicalize(final SequencedMap<Integer, Code> codeTable) {
        final List<Entry<Integer, Code>> reordered = codeTable.entrySet().stream()
                .sorted(comparingByKey())
                .sorted(comparingByValue(comparing(Code::bitcount)))
                .toList();

        final SequencedMap<Integer, Code> canonicalized = new LinkedHashMap<>();

        int prevBits = 0;
        int prevBitcount = 0;

        for (final Entry<Integer, Code> entry : reordered) {
            final int curBitcount = entry.getValue().bitcount();
            if (curBitcount > prevBitcount) {
                prevBits <<= curBitcount - prevBitcount;
            }
            canonicalized.put(entry.getKey(), new Code(prevBits, curBitcount));

            prevBits++;
            prevBitcount = curBitcount;
        }

        return canonicalized;
    }
}
