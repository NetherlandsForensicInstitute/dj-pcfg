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
import java.util.List;
import java.util.PriorityQueue;

import static nl.nfi.djpcfg.common.Formatting.toHumanReadableSize;

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
}
