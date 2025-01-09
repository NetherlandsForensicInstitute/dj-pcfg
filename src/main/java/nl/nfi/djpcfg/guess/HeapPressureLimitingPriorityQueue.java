package nl.nfi.djpcfg.guess;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.NotificationEmitter;
import java.lang.management.*;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import static java.lang.Long.signum;
import static java.lang.Math.abs;

// TODO: hack
public final class HeapPressureLimitingPriorityQueue<T> extends PriorityQueue<T> {

    private static final Logger LOG = LoggerFactory.getLogger(HeapPressureLimitingPriorityQueue.class);

    private static final int THRESHOLD_PERCENTAGE = 90;
    private static final int QUEUE_REDUCTION_PERCENTAGE = 30;

    private static volatile boolean thresholdReached = false;

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
                        thresholdReached = true;
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

    public HeapPressureLimitingPriorityQueue(final Comparator<? super T> comparator) {
        super(comparator);
    }

    public HeapPressureLimitingPriorityQueue(final int initialCapacity, final Comparator<? super T> comparator) {
        super(initialCapacity, comparator);
    }

    @Override
    public boolean offer(final T element) {
        if (thresholdReached) {
            // TODO: lock out other queues?
            // TODO: recheck threshold? maybe collection/cleanup has already happened?
            final int numberOfItemsToKeep = size() / 100 * QUEUE_REDUCTION_PERCENTAGE;
            LOG.info("Reaching heap limit, reducing queue size from {} to {}", size(), numberOfItemsToKeep);
            final List<T> itemsToKeep = new ArrayList<>(numberOfItemsToKeep);
            for (int i = 0; i < numberOfItemsToKeep; i++) {
                itemsToKeep.add(poll());
            }
            clear();
            System.gc();
            itemsToKeep.forEach(super::offer);
            thresholdReached = false;
        }
        return super.offer(element);
    }

    private static String toHumanReadableSize(final long size) {
        final long absB = size == Long.MIN_VALUE ? Long.MAX_VALUE : abs(size);
        if (absB < 1024) {
            return size + " B";
        }
        long value = absB;
        final CharacterIterator ci = new StringCharacterIterator("KMGTPE");
        for (int i = 40; i >= 0 && absB > 0xfffccccccccccccL >> i; i -= 10) {
            value >>= 10;
            ci.next();
        }
        value *= signum(size);
        return String.format("%.1f%ciB", value / 1024.0, ci.current());
    }
}
