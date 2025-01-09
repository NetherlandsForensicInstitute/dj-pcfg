package nl.nfi.djpcfg.common.lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.channels.FileLock;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;

import static nl.nfi.djpcfg.common.HostUtils.hostname;
import static nl.nfi.djpcfg.common.HostUtils.pid;

public final class DirectoryLock implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(DirectoryLock.class);

    private final Path path;
    private final Closeable owner;
    private final FileLock lock;

    public DirectoryLock(final Path path, final Closeable owner, final FileLock lock) {
        this.path = path;
        this.owner = owner;
        this.lock = lock;
    }

    public static DirectoryLock lock(final Path directory) throws IOException {
        return lockFile(directory.resolve(".lock"));
    }

    private static DirectoryLock lockFile(final Path path) throws IOException {
        if (!Files.exists(path)) {
            // TODO: single createDirectory is atomic, but createDirectories?...
            Files.createDirectories(path.getParent());
            try {
                // can fail, but then somebody else already created it
                Files.createFile(path);
            } catch (final FileAlreadyExistsException e) {
                // TODO: pass attribute to ignore this, too lazy for now
            }
        }

        final FileOutputStream output = new FileOutputStream(path.toFile());
        // TODO: add some kind if timeout, loop with tryLock?
        LOG.debug("Acquiring lock: {}", path);
        final FileLock lock = output.getChannel().lock();

        try (final PrintWriter writer = new PrintWriter(new FileOutputStream(path.resolveSibling(".lock.info").toFile()))) {
            writer.println("host:  %s\npid:   %d\nstate: LOCKED".formatted(hostname(), pid()));
        }

        return new DirectoryLock(path, output, lock);
    }

    @Override
    public void close() throws IOException {
        try (final PrintWriter writer = new PrintWriter(new FileOutputStream(path.resolveSibling(".lock.info").toFile()))) {
            writer.println("host:  %s\npid:   %d\nstate: RELEASED".formatted(hostname(), pid()));
        }
        // TODO: not sure if there is any value in releasing lock, or worse...
        LOG.debug("Releasing lock: {} (chan {})", path, lock.acquiredBy());
        // lock.release();
        owner.close();
    }
}
