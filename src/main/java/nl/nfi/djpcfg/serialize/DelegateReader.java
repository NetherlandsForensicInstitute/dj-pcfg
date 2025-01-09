package nl.nfi.djpcfg.serialize;

import java.io.IOException;

public interface DelegateReader<T> {

    T read() throws IOException;
}
