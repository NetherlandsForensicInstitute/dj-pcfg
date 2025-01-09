package nl.nfi.djpcfg.guess.cache.distributed;

public final class Config {

    public static final int CHUNK_SIZE = 64 * 1024 * 1024;
    public static final int MAX_MESSAGE_SIZE = CHUNK_SIZE + 64 * 1024;
}
