package nl.nfi.djpcfg.serialize.common;

import java.io.IOException;

public interface DelegateReader<T> {

    T read() throws IOException;
}
