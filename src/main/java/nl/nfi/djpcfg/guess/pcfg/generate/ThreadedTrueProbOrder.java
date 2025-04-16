package nl.nfi.djpcfg.guess.pcfg.generate;

import nl.nfi.djpcfg.guess.cache.Checkpoint;
import nl.nfi.djpcfg.guess.pcfg.HeapLimitingPTQueue;
import nl.nfi.djpcfg.guess.pcfg.Pcfg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Collections.emptyList;

// experimental
public final class ThreadedTrueProbOrder implements PasswordGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(ThreadedTrueProbOrder.class);

    private static final int SPLIT_SIZE = 10_000_000;

    private final Pcfg pcfg;
    private final int maxProducerThreads;

    // not final, we want it garbage collected as soon as we start generating
    // to reduce memory pressure
    private Checkpoint state;

    private ThreadedTrueProbOrder(final Pcfg pcfg, final Checkpoint state, final int maxProducerThreads) {
        this.pcfg = pcfg;
        this.state = state;
        this.maxProducerThreads = maxProducerThreads;
    }

    public static ThreadedTrueProbOrder init(final Pcfg pcfg, final Checkpoint state) {
        return new ThreadedTrueProbOrder(pcfg, state, 1);
    }

    public ThreadedTrueProbOrder maxProducerThreads(final int maxProducerThreads) {
        return new ThreadedTrueProbOrder(pcfg, state, maxProducerThreads);
    }

    // TODO: ugly parallel part, improve
    @Override
    public Optional<Checkpoint> writeGuesses(final long skip, final long limit, final PrintStream output) {
        final int numProcessors = Runtime.getRuntime().availableProcessors();

        final int numPossibleThreads = max(1, min(numProcessors - 1, maxProducerThreads));
        final int numProducerThreads = numPossibleThreads > limit ? (int) limit : numPossibleThreads;

        final List<ProgressState> states = generateChunks(skip, limit);

        try (final ExecutorService executorService = Executors.newFixedThreadPool(numProducerThreads)) {
            final BlockingQueue<ProgressState> workItems = new ArrayBlockingQueue<>(states.size() + numProducerThreads, false, states);
            for (int i = 0; i < numProducerThreads; i++) {
                workItems.add(new ProgressState());
            }

            final BlockingQueue<List<String>> passwordBatches = new ArrayBlockingQueue<>(numProducerThreads * 4);
            final AtomicReference<Checkpoint> checkpoint = new AtomicReference<>(state);
            // make free for garbage collection when the atomic reference above is changed
            state = null;

            // used to make each thread wait until they have retrieved the starting checkpoint
            final CountDownLatch latch = new CountDownLatch(numProducerThreads);

            for (int i = 0; i < numProducerThreads; i++) {
                final int threadId = i;
                executorService.submit(() -> {
                    try {
                        Checkpoint localCheckpoint = checkpoint.get();
                        // wait until each thread has retrieved the initial checkpoint
                        latch.countDown();
                        latch.await();

                        LOG.debug("Starting producer thread with id [{}]", threadId);

                        while (true) {
                            final ProgressState workItem = workItems.poll();
                            if (workItem.limit == 0) {
                                passwordBatches.put(emptyList());
                                return;
                            }

                            try (final PrintStream out = new CapturingOutputStream(batch -> {
                                try {
                                    passwordBatches.put(batch);
                                } catch (final InterruptedException e) {
                                    LOG.error("Exception in worker thread [{}]", threadId, e);
                                    Thread.currentThread().interrupt();
                                    throw new RuntimeException(e);
                                }
                            })) {
                                final Checkpoint globalCheckpoint = checkpoint.get();
                                // if the shared checkpoint can be used to start from, and it progressed further than
                                // what we have locally, use that checkpoint
                                // else, use our local checkpoint to start from
                                if (globalCheckpoint.keyspacePosition() <= workItem.skip && globalCheckpoint.keyspacePosition() >= localCheckpoint.keyspacePosition()) {
                                    localCheckpoint = new Checkpoint(new HeapLimitingPTQueue(globalCheckpoint.queue()), globalCheckpoint.next(), globalCheckpoint.keyspacePosition());
                                } else {
                                    // TODO: I don't think this can actually hit, but keep for now to be sure
                                    localCheckpoint = new Checkpoint(new HeapLimitingPTQueue(localCheckpoint.queue()), localCheckpoint.next(), localCheckpoint.keyspacePosition());
                                }

                                TrueProbOrder.init(pcfg, localCheckpoint)
                                        .writeGuesses(workItem.skip, workItem.limit, out)
                                        .ifPresent(nextCheckpoint -> {
                                            checkpoint.getAndUpdate(previousCheckpoint ->
                                                    nextCheckpoint.keyspacePosition() > previousCheckpoint.keyspacePosition()
                                                            ? nextCheckpoint
                                                            : previousCheckpoint
                                            );
                                        });
                            }
                        }
                    } catch (final Throwable t) {
                        LOG.error("Exception in worker thread [{}]", threadId, t);
                        throw new RuntimeException(t);
                    }
                });
            }

            int remainingThreadCount = numProducerThreads;
            while (remainingThreadCount > 0) {
                final List<String> batch = passwordBatches.take();
                if (batch.isEmpty()) {
                    remainingThreadCount--;
                } else {
                    batch.forEach(output::println);
                }
            }
            return Optional.ofNullable(checkpoint.get());
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    // TODO: can cause heap space error, e.g. skip 0, limit = Long.MAX_VALUE, take a look at ThreadedRandomWalk
    private static List<ProgressState> generateChunks(final long skip, final long limit) {
        final List<ProgressState> states = new ArrayList<>();
        long remaining = limit;
        while (remaining > 0) {
            final ProgressState progress = new ProgressState();
            progress.skip = skip + limit - remaining;
            progress.limit = min(SPLIT_SIZE, remaining);
            remaining -= progress.limit;
            states.add(progress);
        }
        return states;
    }
}
