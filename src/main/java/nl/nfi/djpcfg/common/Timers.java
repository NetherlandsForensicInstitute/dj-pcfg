package nl.nfi.djpcfg.common;

import java.time.Duration;

public final class Timers {

    public static <X extends Exception> Duration time(final Statement<X> executable) throws X {
        final long start = System.nanoTime();
        executable.execute();
        return Duration.ofNanos(System.nanoTime() - start);
    }

    public static <T, X extends Exception> TimedResult<T> time(final Expression<T, X> executable) throws X {
        final long start = System.nanoTime();
        final T result = executable.execute();
        return new TimedResult<>(result, Duration.ofNanos(System.nanoTime() - start));
    }

    public static <X extends Exception> TimedResult<?> time(final long iterationCount, final Statement<X> executable) throws X {
        final long start = System.nanoTime();
        for (long i = 0; i < iterationCount; i++) {
            executable.execute();
        }
        return new TimedResult<>(null, Duration.ofNanos(System.nanoTime() - start).dividedBy(iterationCount));
    }

    public static <T, X extends Exception> TimedResult<T> time(final long iterationCount, final Expression<T, X> executable) throws X {
        final long start = System.nanoTime();
        T latest = executable.execute();
        for (long i = 0; i < iterationCount - 1; i++) {
            latest = executable.execute();
        }
        return new TimedResult<>(latest, Duration.ofNanos(System.nanoTime() - start).dividedBy(iterationCount));
    }

    @FunctionalInterface
    public interface Statement<X extends Exception> {
        void execute() throws X;
    }

    @FunctionalInterface
    public interface Expression<T, X extends Exception> {
        T execute() throws X;
    }

    public record TimedResult<T>(T value, Duration duration) {
    }
}
