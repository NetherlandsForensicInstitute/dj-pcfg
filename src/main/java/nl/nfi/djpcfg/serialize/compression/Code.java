package nl.nfi.djpcfg.serialize.compression;

public record Code(int bits, int bitcount) {

    public Code {
        if (bitcount > 32) {
            throw new IllegalArgumentException("code with more than 32 bits not supported: " + bitcount);
        }
    }

    @Override
    public String toString() {
        return String.format("%32s", Integer.toBinaryString(bits)).replaceAll(" ", "0").substring(32 - bitcount, 32) + "(" + bits + ")";
    }
}
