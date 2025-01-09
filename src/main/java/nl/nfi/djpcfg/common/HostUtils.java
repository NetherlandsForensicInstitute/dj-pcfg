package nl.nfi.djpcfg.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public final class HostUtils {

    public static long pid() {
        return ProcessHandle.current().pid();
    }

    public static String hostname() {
        try {
            final Process hostname = Runtime.getRuntime().exec(new String[]{"hostname"});
            return new BufferedReader(new InputStreamReader(hostname.getInputStream())).readLine();
        } catch (final IOException e) {
            throw new UnsupportedOperationException("Could not determine hostname", e);
        }
    }
}
