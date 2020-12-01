package de.gsi.microservice;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceReportingEventHandler;
import com.lmax.disruptor.TimeoutHandler;

import de.gsi.dataset.utils.Cache;
import de.gsi.dataset.utils.NoDuplicatesList;
import de.gsi.microservice.filter.EvtTypeFilter;
import de.gsi.microservice.filter.TimingCtx;
import de.gsi.microservice.utils.SharedPointer;

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
public class AggregateEventHandler implements SequenceReportingEventHandler<RingBufferEvent> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AggregateEventHandler.class);
    // private Map<Long, Object> aggregatedBpcts = new SoftHashMap<>(RETENTION_SIZE);
    protected final Map<Long, Object> aggregatedBpcts;
    private final RingBuffer<RingBufferEvent> ringBuffer;
    private final long timeOut;
    private final TimeUnit timeOutUnit;
    private final int numberOfEventsToAggregate;
    private final List<String> deviceList;
    private final List<Predicate<RingBufferEvent>> evtTypeFilter;
    private final InternalAggregationHandler[] aggregationHandler;
    private final List<InternalAggregationHandler> freeWorkers;
    private final String aggregateName;
    private Sequence seq;

    private AggregateEventHandler(final RingBuffer<RingBufferEvent> ringBuffer, final String aggregateName, final long timeOut, final TimeUnit timeOutUnit, final int nWorkers, final int retentionSize, final List<String> deviceList, List<Predicate<RingBufferEvent>> evtTypeFilter) { // NOPMD NOSONAR -- number of arguments acceptable/ complexity handled by factory
        this.ringBuffer = ringBuffer;
        this.aggregateName = aggregateName;
        this.timeOut = timeOut;
        this.timeOutUnit = timeOutUnit;

        freeWorkers = Collections.synchronizedList(new ArrayList<>(nWorkers));
        aggregationHandler = new InternalAggregationHandler[nWorkers];
        for (int i = 0; i < nWorkers; i++) {
            aggregationHandler[i] = new InternalAggregationHandler();
            freeWorkers.add(aggregationHandler[i]);
        }
        aggregatedBpcts = new Cache<>(retentionSize);
        this.deviceList = deviceList;
        this.evtTypeFilter = evtTypeFilter;
        numberOfEventsToAggregate = deviceList.size() + evtTypeFilter.size();
    }

    public InternalAggregationHandler[] getAggregationHander() {
        return aggregationHandler;
    }

    @Override
    public void onEvent(final RingBufferEvent event, final long nextSequence, final boolean b) {
        final TimingCtx ctx = event.getFilter(TimingCtx.class);
        if (ctx == null) {
            return;
        }

        // final boolean alreadyScheduled = Arrays.stream(workers).filter(w -> w.bpcts == eventBpcts).findFirst().isPresent();
        final boolean alreadyScheduled = aggregatedBpcts.containsKey(ctx.bpcts);
        if (alreadyScheduled) {
            return;
        }
        while (true) {
            if (!freeWorkers.isEmpty()) {
                final InternalAggregationHandler freeWorker = freeWorkers.remove(0);
                freeWorker.bpcts = ctx.bpcts;
                freeWorker.aggStart = event.arrivalTimeStamp;
                aggregatedBpcts.put(ctx.bpcts, new Object());
                seq.set(nextSequence); // advance sequence to let workers process events up to here
                return;
            }
            // no free worker available
            long waitTimeNanos = Long.MAX_VALUE;
            for (InternalAggregationHandler w : aggregationHandler) {
                final long currentTime = System.currentTimeMillis();
                final long diffMillis = currentTime - w.aggStart;
                waitTimeNanos = Math.min(waitTimeNanos, TimeUnit.MILLISECONDS.toNanos(diffMillis));
                if (w.bpcts != -1 && diffMillis < timeOutUnit.toMillis(timeOut)) {
                    w.publishAndFreeWorker(EvtTypeFilter.UpdateType.PARTIAL); // timeout reached, publish partial result and free worker
                    break;
                }
            }
            LockSupport.parkNanos(waitTimeNanos);
        }
    }

    @Override
    public void setSequenceCallback(final Sequence sequence) {
        this.seq = sequence;
    }

    public static AggregateEventHandlerFactory getFactory() {
        return new AggregateEventHandlerFactory();
    }

    public static class AggregateEventHandlerFactory {
        private final List<String> deviceList = new NoDuplicatesList<>();
        private final List<Predicate<RingBufferEvent>> evtTypeFilter = new NoDuplicatesList<>();
        private RingBuffer<RingBufferEvent> ringBuffer;
        private int numberWorkers = 4; // number of workers defines the maximum number of aggregate events groups which can be overlapping
        private long timeOut = 400;
        private TimeUnit timeOutUnit = TimeUnit.MILLISECONDS;
        private int retentionSize = 12;
        private String aggregateName;

        public AggregateEventHandler build() {
            if (aggregateName == null || aggregateName.isBlank()) {
                throw new IllegalArgumentException("aggregateName must not be null or blank");
            }
            if (ringBuffer == null) {
                throw new IllegalArgumentException("ringBuffer must not be null");
            }
            final int actualRetentionSize = Math.min(retentionSize, 3 * numberWorkers);
            return new AggregateEventHandler(ringBuffer, aggregateName, timeOut, timeOutUnit, numberWorkers, actualRetentionSize, deviceList, evtTypeFilter);
        }

        public String getAggregateName() {
            return aggregateName;
        }

        public AggregateEventHandlerFactory setAggregateName(final String aggregateName) {
            this.aggregateName = aggregateName;
            return this;
        }

        public List<String> getDeviceList() {
            return deviceList;
        }

        /**
         *
         * @param deviceList lists of devices, event names, etc. that shall be aggregated
         * @return itself (fluent design)
         */
        public AggregateEventHandlerFactory setDeviceList(final List<String> deviceList) {
            this.deviceList.addAll(deviceList);
            return this;
        }

        /**
         *
         * @param devices single or lists of devices, event names, etc. that shall be aggregated
         * @return itself (fluent design)
         */
        public AggregateEventHandlerFactory setDeviceList(final String... devices) {
            this.deviceList.addAll(Arrays.asList(devices));
            return this;
        }

        public List<Predicate<RingBufferEvent>> getEvtTypeFilter() {
            return evtTypeFilter;
        }

        /**
         *
         * @param evtTypeFilter single or lists of predicate filters of events that shall be aggregated
         * @return itself (fluent design)
         */
        @SafeVarargs
        public final AggregateEventHandlerFactory setEvtTypeFilter(final Predicate<RingBufferEvent>... evtTypeFilter) {
            this.evtTypeFilter.addAll(Arrays.asList(evtTypeFilter));
            return this;
        }

        /**
         *
         * @param evtTypeFilter single or lists of predicate filters of events that shall be aggregated
         * @return itself (fluent design)
         */
        public AggregateEventHandlerFactory setEvtTypeFilter(final List<Predicate<RingBufferEvent>> evtTypeFilter) {
            this.evtTypeFilter.addAll(evtTypeFilter);
            return this;
        }

        /**
         * @return number of workers defines the maximum number of aggregate events groups which can be overlapping
         */
        public int getNumberWorkers() {
            return numberWorkers;
        }

        /**
         *
         * @param numberWorkers number of workers defines the maximum number of aggregate events groups which can be overlapping
         * @return itself (fluent design)
         */
        public AggregateEventHandlerFactory setNumberWorkers(final int numberWorkers) {
            if (numberWorkers < 1) {
                throw new IllegalArgumentException("numberWorkers must not be < 1: " + numberWorkers);
            }
            this.numberWorkers = numberWorkers;
            return this;
        }

        public int getRetentionSize() {
            return retentionSize;
        }

        public AggregateEventHandlerFactory setRetentionSize(final int retentionSize) {
            if (retentionSize < 1) {
                throw new IllegalArgumentException("timeOut must not be < 1: " + retentionSize);
            }
            this.retentionSize = retentionSize;
            return this;
        }

        public RingBuffer<RingBufferEvent> getRingBuffer() {
            return ringBuffer;
        }

        public AggregateEventHandlerFactory setRingBuffer(final RingBuffer<RingBufferEvent> ringBuffer) {
            if (ringBuffer == null) {
                throw new IllegalArgumentException("ringBuffer must not null");
            }
            this.ringBuffer = ringBuffer;
            return this;
        }

        public long getTimeOut() {
            return timeOut;
        }

        public TimeUnit getTimeOutUnit() {
            return timeOutUnit;
        }

        public AggregateEventHandlerFactory setTimeOut(final long timeOut, final TimeUnit timeOutUnit) {
            if (timeOut <= 0) {
                throw new IllegalArgumentException("timeOut must not be <=0: " + timeOut);
            }
            if (timeOutUnit == null) {
                throw new IllegalArgumentException("timeOutUnit must not null");
            }
            this.timeOut = timeOut;
            this.timeOutUnit = timeOutUnit;
            return this;
        }
    }

    protected class InternalAggregationHandler implements EventHandler<RingBufferEvent>, TimeoutHandler {
        protected volatile long bpcts = -1; // [ms]
        protected volatile long aggStart = -1; // [ns]
        protected List<RingBufferEvent> aggregatedEventsStash = new ArrayList<>();

        @Override
        public void onEvent(final RingBufferEvent event, final long sequence, final boolean endOfBatch) {
            if (bpcts != -1 && event.arrivalTimeStamp > aggStart + timeOutUnit.toMillis(timeOut)) {
                publishAndFreeWorker(EvtTypeFilter.UpdateType.PARTIAL);
                return;
            }
            final TimingCtx ctx = event.getFilter(TimingCtx.class);
            if (bpcts == -1 || ctx == null || ctx.bpcts != bpcts) {
                return; // skip irrelevant events
            }
            final EvtTypeFilter evtType = event.getFilter(EvtTypeFilter.class);
            if (evtType == null) {
                throw new IllegalArgumentException("cannot aggregate events without ring buffer containing EvtTypeFilter");
            }
            if ((!deviceList.isEmpty() && !deviceList.contains(evtType.typeName)) || (!evtTypeFilter.isEmpty() && evtTypeFilter.stream().noneMatch(filter -> filter.test(event)))) {
                return;
            }

            aggregatedEventsStash.add(event);
            if (aggregatedEventsStash.size() == numberOfEventsToAggregate) {
                publishAndFreeWorker(EvtTypeFilter.UpdateType.COMPLETE);
            }
        }

        @Override
        public void onTimeout(final long sequence) {
            if (bpcts != -1 && System.currentTimeMillis() > aggStart + timeOut) {
                publishAndFreeWorker(EvtTypeFilter.UpdateType.PARTIAL);
            }
        }

        protected void publishAndFreeWorker(final EvtTypeFilter.UpdateType updateType) {
            final long now = System.currentTimeMillis();
            ringBuffer.publishEvent(((event, sequence, arg0) -> {
                final TimingCtx ctx = event.getFilter(TimingCtx.class);
                if (ctx == null) {
                    throw new IllegalStateException("RingBufferEvent has not TimingCtx definition");
                }
                final EvtTypeFilter evtType = event.getFilter(EvtTypeFilter.class);
                if (evtType == null) {
                    throw new IllegalArgumentException("cannot aggregate events without ring buffer containing EvtTypeFilter");
                }

                // write header/meta-type data
                event.arrivalTimeStamp = now;
                event.payload = new SharedPointer<>();
                final Map<String, SharedPointer<?>> aggregatedItems = new HashMap<>(); // <String:deviceName,SharedPointer:dataObject>
                event.payload.set(aggregatedItems);
                if (updateType == EvtTypeFilter.UpdateType.PARTIAL) {
                    LOGGER.atInfo().log("aggregation timed out for bpcts: " + bpcts);
                }

                if (aggregatedEventsStash.isEmpty()) {
                    // notify empty aggregate
                    event.parentSequenceNumber = sequence;
                    evtType.typeName = aggregateName;
                    evtType.updateType = EvtTypeFilter.UpdateType.EMPTY;
                    evtType.evtType = EvtTypeFilter.DataType.AGGREGATE_DATA;
                    return;
                }

                // handle non-empty aggregate
                final RingBufferEvent firstItem = aggregatedEventsStash.get(0);
                for (int i = 0; i < firstItem.filters.length; i++) {
                    firstItem.filters[i].copyTo(event.filters[i]);
                }
                if (updateType == EvtTypeFilter.UpdateType.PARTIAL) {
                    LOGGER.atInfo().log("aggregation timed out for 2:bpcts: " + event.getFilter(TimingCtx.class).bpcts);
                }
                evtType.typeName = aggregateName;
                evtType.updateType = updateType;
                evtType.evtType = EvtTypeFilter.DataType.AGGREGATE_DATA;
                event.parentSequenceNumber = sequence;

                // add new events to payload
                for (RingBufferEvent rbEvent : aggregatedEventsStash) {
                    final EvtTypeFilter type = rbEvent.getFilter(EvtTypeFilter.class);
                    aggregatedItems.put(type.typeName, rbEvent.payload.getCopy());
                }
            }),
                    aggregatedEventsStash);

            // init worker for next aggregation iteration
            bpcts = -1;
            aggregatedEventsStash = new ArrayList<>();
            freeWorkers.add(this);
        }
    }
}
