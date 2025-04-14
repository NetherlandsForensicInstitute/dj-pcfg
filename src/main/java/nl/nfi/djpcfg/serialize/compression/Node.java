package nl.nfi.djpcfg.serialize.compression;

import static java.lang.Long.max;

public record Node(int value, long count, Node l, Node r) {

    public static Node internal(final Node l, final Node r) {
        return new Node(-1, l.count + r.count, l, r);
    }

    public static Node leaf(final int value, final long count) {
        if (value < 0) {
            throw new IllegalArgumentException("value must be >= 0: " + value);
        }
        return new Node(value, count, null, null);
    }

    public boolean isInternal() {
        return value < 0;
    }

    public boolean isLeaf() {
        return value >= 0;
    }

    public long size() {
        // implies r is null
        if (l == null) {
            return 1;
        }
        return 1 + l.size() + r.size();
    }

    public long depth() {
        // implies r is null
        if (l == null) {
            return 1;
        }
        return 1 + max(l.depth(), r.depth());
    }
}
