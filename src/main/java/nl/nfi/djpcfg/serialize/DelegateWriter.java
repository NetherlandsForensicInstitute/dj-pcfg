package nl.nfi.djpcfg.serialize;

import java.io.IOException;

public interface DelegateWriter<T> {

    void write(final T value) throws IOException;
}
