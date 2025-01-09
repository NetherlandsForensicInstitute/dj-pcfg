package nl.nfi.djpcfg.guess.pcfg.generate;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public final class CapturingOutputStream extends PrintStream {

    private static final int DEFAULT_BATCH_SIZE = 1 << 16;

    private final int batchSize;
    private final Consumer<List<String>> collectBatch;
    private List<String> batch = new ArrayList<>();

    public CapturingOutputStream(final Consumer<List<String>> collectBatch) {
        this(DEFAULT_BATCH_SIZE, collectBatch);
    }

    public CapturingOutputStream(final int batchSize, final Consumer<List<String>> collectBatch) {
        super(nullOutputStream());
        this.batchSize = batchSize;
        this.collectBatch = collectBatch;
    }

    @Override
    public void println(final String string) {
        batch.add(string);
        if (batch.size() > batchSize) {
            transferBatch();
        }
    }

    @Override
    public void close() {
        if (!batch.isEmpty()) {
            transferBatch();
        }
    }

    private void transferBatch() {
        collectBatch.accept(batch);
        batch = new ArrayList<>();
    }
}
