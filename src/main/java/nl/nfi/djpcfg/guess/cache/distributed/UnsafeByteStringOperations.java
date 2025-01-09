package nl.nfi.djpcfg.guess.cache.distributed;

import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;

public final class UnsafeByteStringOperations {

    private static final Logger LOG = LoggerFactory.getLogger(UnsafeByteStringOperations.class);

    private static final MethodHandle GET_BYTE_STRING_BYTES;

    static {
        try {
            final Class<?> literalByteString = Class.forName("com.google.protobuf.ByteString$LiteralByteString");
            final Field field = literalByteString.getDeclaredField("bytes");
            field.setAccessible(true);
            GET_BYTE_STRING_BYTES = MethodHandles.lookup().unreflectGetter(field);
        } catch (final ClassNotFoundException | NoSuchFieldException | IllegalAccessException e) {
            LOG.error("Can't load method handle", e);
            throw new RuntimeException(e);
        }
    }

    // TODO: not profiled in depth, some superficial testing seems to point to a small perf increase vs. toByteArray,
    //       but might be coincidence
    public static byte[] getBytes(final ByteString byteString) {
        try {
            return (byte[]) GET_BYTE_STRING_BYTES.invokeWithArguments(byteString);
        } catch (final Throwable t) {
            LOG.error("Failed to execute method handle", t);
            throw new RuntimeException(t);
        }
    }
}
