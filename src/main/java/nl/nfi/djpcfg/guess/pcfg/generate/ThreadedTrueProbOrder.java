package nl.nfi.djpcfg.guess.pcfg.generate;

import nl.nfi.djpcfg.guess.cache.Checkpoint;
import nl.nfi.djpcfg.guess.pcfg.Pcfg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Collections.emptyList;

// experimental
public final class ThreadedTrueProbOrder implements PasswordGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(ThreadedTrueProbOrder.class);

    private static final int SPLIT_SIZE = 1000000;

    private final Pcfg pcfg;
    private final Checkpoint state;
    private final int maxProducerThreads;

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
            final AtomicReference<Checkpoint> checkpoint = new AtomicReference<>();

            for (int i = 0; i < numProducerThreads; i++) {
                final int threadId = i;
                executorService.submit(() -> {
                    try {
                        while (true) {
                            final ProgressState workItem = workItems.poll();
                            if (workItem.remainingLimit == 0) {
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
                                Checkpoint localState = checkpoint.get();
                                if (localState == null) {
                                    localState = new Checkpoint(new PriorityQueue<>(state.queue()), state.next(), state.keyspacePosition());
                                } else {
                                    localState = new Checkpoint(new PriorityQueue<>(localState.queue()), localState.next(), localState.keyspacePosition());
                                }

                                TrueProbOrder.init(pcfg, localState)
                                        .writeGuesses(workItem.remainingSkip, workItem.remainingLimit, out)
                                        .ifPresent(newCheckpoint -> {
                                            checkpoint.getAndUpdate(value -> value == null
                                                    ? newCheckpoint
                                                    : value.keyspacePosition() < newCheckpoint.keyspacePosition()
                                                    ? newCheckpoint
                                                    : value
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
            progress.remainingSkip = skip + limit - remaining;
            progress.remainingLimit = min(SPLIT_SIZE, remaining);
            remaining -= progress.remainingLimit;
            states.add(progress);
        }
        return states;
    }
}
