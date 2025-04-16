package nl.nfi.djpcfg.guess.pcfg;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.NotificationEmitter;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryNotificationInfo;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.WeakHashMap;

import static java.util.Collections.newSetFromMap;
import static java.util.Collections.synchronizedSet;
import static nl.nfi.djpcfg.common.Formatting.toHumanReadableSize;

// TODO: it's a hack, create something better
// heap pressure limiting parse tree priority queue
public final class HeapLimitingPTQueue extends PriorityQueue<ParseTree> {

    private static final Logger LOG = LoggerFactory.getLogger(HeapLimitingPTQueue.class);

    private static final int THRESHOLD_PERCENTAGE = 90;
    private static final int SIZE_PERCENTAGE_TO_REDUCE_TO = 30;

    private static final Set<HeapLimitingPTQueue> LIVE_QUEUES = synchronizedSet(newSetFromMap(new WeakHashMap<>()));

    private volatile boolean thresholdReached;
    private volatile boolean shrinking;

    static {
        final MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
        if (!(memoryBean instanceof final NotificationEmitter notifier)) {
            throw new UnsupportedOperationException();
        }
        notifier.addNotificationListener(
                (_, bean) -> {
                    final MemoryUsage memoryUsage = ((MemoryMXBean) bean).getHeapMemoryUsage();
                    if (memoryUsage.getUsed() * 100 / memoryUsage.getMax() > THRESHOLD_PERCENTAGE) {
                        LOG.info("Reaching heap limit, using more than {}% of max memory usage", THRESHOLD_PERCENTAGE);
                        LIVE_QUEUES.forEach(HeapLimitingPTQueue::markThresholdReached);
                    }
                },
                notification -> notification.getType().equals(MemoryNotificationInfo.MEMORY_COLLECTION_THRESHOLD_EXCEEDED),
                memoryBean);

        for (final MemoryPoolMXBean memoryPoolBean : ManagementFactory.getMemoryPoolMXBeans()) {
            if (memoryPoolBean.getType() == MemoryType.HEAP && memoryPoolBean.isCollectionUsageThresholdSupported()) {
                final long max = memoryPoolBean.getUsage().getMax();
                if (max >= 0) {
                    final long size = max * THRESHOLD_PERCENTAGE / 100;
                    LOG.info("Setting heap limit threshold at {} on pool: {} ({})",
                            toHumanReadableSize(size),
                            memoryPoolBean.getName(),
                            memoryPoolBean.getType()
                    );
                    memoryPoolBean.setCollectionUsageThreshold(size);
                }
            }
        }
    }

    public HeapLimitingPTQueue(final Comparator<? super ParseTree> comparator) {
        super(comparator);
        setupMemoryManagement();
    }

    public HeapLimitingPTQueue(final int initialCapacity, final Comparator<? super ParseTree> comparator) {
        super(initialCapacity, comparator);
        setupMemoryManagement();
    }

    public HeapLimitingPTQueue(final PriorityQueue<ParseTree> queue) {
        super(queue.size(), queue.comparator());
        setupMemoryManagement();
        // slower, but now we also use the heap management
        addAll(queue);
    }

    private void setupMemoryManagement() {
        LIVE_QUEUES.add(this);
    }

    private void markThresholdReached() {
        if (!shrinking) {
            thresholdReached = true;
        }
    }

    @Override
    public boolean add(final ParseTree element) {
        manageMemoryUsage();
        return super.add(element);
    }

    @Override
    public boolean offer(final ParseTree element) {
        manageMemoryUsage();
        return super.offer(element);
    }

    private void manageMemoryUsage() {
        if (!thresholdReached) {
            return;
        }
        shrinking = true;
        thresholdReached = false;

        final int startSize = size();
        final int numberOfItemsToKeep = startSize / 100 * SIZE_PERCENTAGE_TO_REDUCE_TO;

        LOG.info("Reaching heap limit, reducing queue size from {} to about {}", startSize, numberOfItemsToKeep);

        // use Object identity set, in order to check which base structures we already keep in the reduced queue
        final Set<Object> seen = newSetFromMap(new IdentityHashMap<>());
        final List<ParseTree> itemsToKeep = new ArrayList<>(numberOfItemsToKeep);
        for (int i = 0; i < numberOfItemsToKeep; i++) {
            final ParseTree item = poll();
            itemsToKeep.add(item);
            seen.add(item.replacementSet().variables());
        }

        // ensure 1 of each base structure remains in the reduced queue
        int extraCount = 0;
        while (!isEmpty()) {
            final ParseTree item = poll();
            // if not yet seen these set of variables (i.e. the base structure),
            // add it to the items to keep
            if (seen.add(item.replacementSet().variables())) {
                itemsToKeep.add(item);
                extraCount++;
            }
        }

        // now add all we want to keep, back
        addAll(itemsToKeep);

        LOG.info("Reduced queue size from {} to {}, kept {} extra items in order to keep at least 1 of each base structure", startSize, itemsToKeep.size(), extraCount);

        shrinking = false;
    }
}
