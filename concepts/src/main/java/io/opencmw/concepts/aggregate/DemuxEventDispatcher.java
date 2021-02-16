package io.opencmw.concepts.aggregate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.LockSupport;

import io.opencmw.utils.Cache;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceReportingEventHandler;
import com.lmax.disruptor.TimeoutHandler;

/**
 * Dispatches aggregation workers upon seeing new values for a specified event field.
 * Each aggregation worker then assembles all events for this value and optionally publishes back an aggregated events.
 * If the aggregation is not completed within a configurable timeout, a partial AggregationEvent is published.
 *
 * For now events are aggregated into a list of Objects until a certain number of events is reached.
 * The final api should allow to specify different Objects to be placed into a result domain object.
 *
 * @author Alexander Krimm
 */
public class DemuxEventDispatcher implements SequenceReportingEventHandler<TestEventSource.IngestedEvent> {
    private static final int N_WORKERS = 4; // number of workers defines the maximum number of aggregate events groups which can be overlapping
    private static final long TIMEOUT = 400;
    private static final int RETENTION_SIZE = 10;
    private static final int N_AGG_ELEMENTS = 3;
    private final AggregationHandler[] aggregationHandler;
    private final List<AggregationHandler> freeWorkers = Collections.synchronizedList(new ArrayList<>(N_WORKERS));
    private final RingBuffer<TestEventSource.IngestedEvent> rb;
    // private Map<Long, Object> aggregatedBpcts = new SoftHashMap<>(RETENTION_SIZE);
    private final Cache<Long, Object> aggregatedBpcts = new Cache<>(RETENTION_SIZE);
    private Sequence seq;

    public DemuxEventDispatcher(final RingBuffer<TestEventSource.IngestedEvent> ringBuffer) {
        rb = ringBuffer;
        aggregationHandler = new AggregationHandler[N_WORKERS];
        for (int i = 0; i < N_WORKERS; i++) {
            aggregationHandler[i] = new AggregationHandler();
            freeWorkers.add(aggregationHandler[i]);
        }
    }

    public AggregationHandler[] getAggregationHander() {
        return aggregationHandler;
    }

    @Override
    public void onEvent(final TestEventSource.IngestedEvent event, final long nextSequence, final boolean b) {
        if (!(event.payload instanceof TestEventSource.Event)) {
            return;
        }
        final long eventBpcts = ((TestEventSource.Event) event.payload).bpcts;
        // final boolean alreadyScheduled = Arrays.stream(workers).filter(w -> w.bpcts == eventBpcts).findFirst().isPresent();
        final boolean alreadyScheduled = aggregatedBpcts.containsKey(eventBpcts);
        if (alreadyScheduled) {
            return;
        }
        while (true) {
            if (!freeWorkers.isEmpty()) {
                final AggregationHandler freeWorker = freeWorkers.remove(0);
                freeWorker.bpcts = eventBpcts;
                freeWorker.aggStart = event.ingestionTime;
                aggregatedBpcts.put(eventBpcts, new Object()); // NOPMD - necessary to allocate inside loop
                seq.set(nextSequence); // advance sequence to let workers process events up to here
                return;
            }
            // no free worker available
            long waitTime = Long.MAX_VALUE;
            for (AggregationHandler w : aggregationHandler) {
                final long currentTime = System.currentTimeMillis();
                final long diff = currentTime - w.aggStart;
                waitTime = Math.min(waitTime, diff * 1_000_000);
                if (w.bpcts != -1 && diff < TIMEOUT) {
                    w.publishAndFreeWorker(true); // timeout reached, publish partial result and free worker
                    break;
                }
            }
            LockSupport.parkNanos(waitTime);
        }
    }

    @Override
    public void setSequenceCallback(final Sequence sequence) {
        this.seq = sequence;
    }

    @SuppressWarnings("PMD.AvoidUsingVolatile") // necessary for desired CPU caching behaviour
    public class AggregationHandler implements EventHandler<TestEventSource.IngestedEvent>, TimeoutHandler {
        protected volatile long bpcts = -1; // [ms]
        protected volatile long aggStart = -1; // [ns]
        private List<TestEventSource.IngestedEvent> payloads = new ArrayList<>();

        @Override
        public void onEvent(final TestEventSource.IngestedEvent event, final long sequence, final boolean endOfBatch) {
            if (bpcts != -1 && event.ingestionTime > aggStart + TIMEOUT) {
                publishAndFreeWorker(true);
                return;
            }
            if (bpcts == -1 || !(event.payload instanceof TestEventSource.Event) || ((TestEventSource.Event) event.payload).bpcts != bpcts) {
                return; // skip irrelevant events
            }
            this.payloads.add(event);
            if (payloads.size() == N_AGG_ELEMENTS) {
                publishAndFreeWorker(false);
            }
        }

        protected void publishAndFreeWorker(final boolean partial) {
            rb.publishEvent(((event1, sequence1, arg0) -> {
                event1.ingestionTime = System.currentTimeMillis();
                event1.payload = partial ? ("aggregation timed out for bpcts: " + bpcts + " -> ") + payloads : payloads;
            }),
                    payloads);
            bpcts = -1;
            payloads = new ArrayList<>();
            freeWorkers.add(this);
        }

        @Override
        public void onTimeout(final long sequence) {
            if (bpcts != -1 && System.currentTimeMillis() > aggStart + TIMEOUT) {
                publishAndFreeWorker(true);
            }
        }
    }
}
