package nl.nfi.djpcfg.common;

import java.text.CharacterIterator;
import java.text.StringCharacterIterator;

import static java.lang.Long.signum;
import static java.lang.Math.abs;

public final class Formatting {

    public static String toHumanReadableSize(final long size) {
        final long absB = size == Long.MIN_VALUE ? Long.MAX_VALUE : abs(size);
        if (absB < 1024) {
            return size + " B";
        }
        long value = absB;
        final CharacterIterator ci = new StringCharacterIterator("KMGTPE");
        for (int i = 40; i >= 0 && absB > 0xfffccccccccccccL >> i; i -= 10) {
            value >>= 10;
            ci.next();
        }
        value *= signum(size);
        return String.format("%.1f%ciB", value / 1024.0, ci.current());
    }
}
