package nl.nfi.djpcfg;

import nl.nfi.djpcfg.guess.pcfg.PcfgGuesser;
import nl.nfi.djpcfg.serialize.JreTypeCodec;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static java.lang.Long.parseLong;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.write;
import static java.util.stream.Collectors.toCollection;

public final class Utils {

    private static final Path TEST_RESOURCES_PATH = Paths.get("src/test/resources").toAbsolutePath();
    private static final Path GUESSER_PYTHON_PATH = TEST_RESOURCES_PATH.resolve("pcfg_python/pcfg_guesser/pcfg_guesser");
    private static final Path SCORER_PYTHON_PATH = TEST_RESOURCES_PATH.resolve("pcfg_python/password_scorer/password_scorer");

    public static long calculateKeyspace(final String rulePath, final long maxKeyspace) throws IOException {
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        PcfgGuesser.forRule(TEST_RESOURCES_PATH.resolve(rulePath)).showKeyspace(maxKeyspace, new PrintStream(output));
        return parseLong(output.toString().trim());
    }

    public static List<String> generate(final String rulePath, final long skip, final long limit) throws IOException {
        return generate(PcfgGuesser.forRule(TEST_RESOURCES_PATH.resolve(rulePath)), skip, limit);
    }

    public static List<String> generate(final PcfgGuesser guesser, final long skip, final long limit) throws IOException {
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        guesser.generateGuesses(skip, limit, new PrintStream(output));
        return output.toString().lines().collect(toCollection(ArrayList::new));
    }

    public static List<String> generateWithPython(final String rulePath, final long limit) throws IOException, InterruptedException {
        final String resultFileKey = "pw_guesses_%d_%d".formatted(rulePath.hashCode() & 0xffffffffL, limit);
        final Path resultFile = TEST_RESOURCES_PATH.resolve("pcfg_python_outputs/%s".formatted(resultFileKey));

        if (Files.isRegularFile(resultFile)) {
            try (final InputStream input = new BufferedInputStream(new FileInputStream(resultFile.toFile()))) {
                final JreTypeCodec.Decoder decoder = JreTypeCodec.forInput(input);
                return decoder.readList(decoder::readString);
            }
        }

        if (true) {
            throw new IllegalStateException();
        }

        final Process process = new ProcessBuilder(
                GUESSER_PYTHON_PATH.toString(),
                "--rule", TEST_RESOURCES_PATH.resolve(rulePath).toString(),
                "--limit", Long.toString(limit),
                "--skip_brute"
        ).start();

        final List<String> results = readOutput(process.getInputStream());
        process.waitFor();

        final List<String> passwords = new ArrayList<>(results.subList(1, results.size()));

        try (final OutputStream output = new BufferedOutputStream(new FileOutputStream(resultFile.toFile()))) {
            final JreTypeCodec.Encoder encoder = JreTypeCodec.forOutput(output);
            encoder.writeList(passwords, encoder::writeString);
        }

        return passwords;
    }

    private static List<String> readOutput(final InputStream inputStream) throws IOException {
        try (final BufferedReader output = new BufferedReader(new InputStreamReader(inputStream))) {
            return output.lines().collect(toCollection(ArrayList::new));
        }
    }

    // TODO: apparently the generated passwords by python are not in non-increasing order when using the scorer...
    public static List<ScoredEntry> scoreWithPython(final String rulePath, final List<? extends String> passwords, final Path tempWorkDir) throws IOException, InterruptedException {
        final String resultFileKey = "pw_scores_%d".formatted(passwords.hashCode() & 0xffffffffL);
        final Path resultFile = TEST_RESOURCES_PATH.resolve("pcfg_python_outputs/%s".formatted(resultFileKey));

        if (Files.isRegularFile(resultFile)) {
            try (final InputStream input = new BufferedInputStream(new FileInputStream(resultFile.toFile()))) {
                final JreTypeCodec.Decoder decoder = JreTypeCodec.forInput(input);
                return decoder.readList(() -> new ScoredEntry(decoder.readString(), decoder.readDouble()));
            }
        }

        final Path passwordsFile = tempWorkDir.resolve(UUID.randomUUID().toString());
        write(passwordsFile, passwords, UTF_8);

        final Process process = new ProcessBuilder(
                SCORER_PYTHON_PATH.toString(),
                "--rule", TEST_RESOURCES_PATH.resolve(rulePath).toString(),
                "--input", passwordsFile.toString(),
                "--max_omen", "0"
        ).start();

        List<String> results = readOutput(process.getInputStream());
        process.waitFor();

        // remove printed header
        results = results.subList(15, results.size());
        // remove false negatives print
        results = results.subList(0, results.size() - 1);

        final List<ScoredEntry> scores = results.stream()
                .map(line -> line.split("\t"))
                .map(parts -> new ScoredEntry(parts[0], Double.parseDouble(parts[2])))
                .collect(toCollection(ArrayList::new));

        try (final OutputStream output = new BufferedOutputStream(new FileOutputStream(resultFile.toFile()))) {
            final JreTypeCodec.Encoder encoder = JreTypeCodec.forOutput(output);
            encoder.writeList(scores, entry -> {
                encoder.writeString(entry.password());
                encoder.writeDouble(entry.score());
            });
        }

        return scores;
    }

    public static <X extends Exception> TimedResult<?> time(final Statement<X> executable) throws X {
        final long start = System.nanoTime();
        executable.execute();
        return new TimedResult<>(null, Duration.ofNanos(System.nanoTime() - start));
    }

    public static <X extends Exception> TimedResult<?> time(final long iterationCount, final Statement<X> executable) throws X {
        final long start = System.nanoTime();
        for (long i = 0; i < iterationCount; i++) {
            executable.execute();
        }
        return new TimedResult<>(null, Duration.ofNanos(System.nanoTime() - start).dividedBy(iterationCount));
    }

    public static <T, X extends Exception> TimedResult<T> time(final Expression<T, X> executable) throws X {
        final long start = System.nanoTime();
        final T result = executable.execute();
        return new TimedResult<>(result, Duration.ofNanos(System.nanoTime() - start));
    }

    public static <T, X extends Exception> TimedResult<T> time(final long iterationCount, final Expression<T, X> executable) throws X {
        final long start = System.nanoTime();
        T latest = executable.execute();
        for (long i = 0; i < iterationCount - 1; i++) {
            latest = executable.execute();
        }
        return new TimedResult<>(latest, Duration.ofNanos(System.nanoTime() - start).dividedBy(iterationCount));
    }

    public record ScoredEntry(String password, double score) {
    }

    @FunctionalInterface
    public interface Statement<X extends Exception> {
        void execute() throws X;
    }

    @FunctionalInterface
    public interface Expression<T, X extends Exception> {
        T execute() throws X;
    }

    public record TimedResult<T>(T result, Duration duration) {
    }
}
