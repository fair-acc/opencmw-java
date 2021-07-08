package io.opencmw;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opencmw.utils.Cache;
import io.opencmw.utils.LimitedArrayList;
import io.opencmw.utils.NoDuplicatesList;
import io.opencmw.utils.WorkerThreadFactory;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.TimeoutBlockingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.EventHandlerGroup;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.Util;

/**
 * Initial event-source concept with one primary event-stream and arbitrary number of secondary context-multiplexed event-streams.
 *
 * Each event-stream is implemented using LMAX's disruptor ring-buffer using the default {@link RingBufferEvent}.
 *
 * The multiplexing-context for the secondary ring buffers is controlled via the 'Function&lt;RingBufferEvent, String&gt; muxCtxFunction'
 * function that produces a unique string hash for a given ring buffer event, e.g.:
 * {@code Function<RingBufferEvent, String> muxCtx = evt -> "cid=" + evt.getFilter(TimingCtx.class).cid;}
 *
 * See EventStoreTest in the teest directory for usage and API examples.
 *
 * @author rstein
 */
public class EventStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventStore.class);
    private static final String NOT_FOUND_FOR_MULTIPLEXING_CONTEXT = "disruptor not found for multiplexing context = ";
    protected final WorkerThreadFactory threadFactory;
    protected final List<LocalEventHandlerGroup> listener = new NoDuplicatesList<>();
    protected final List<EventHandler<RingBufferEvent>> allEventHandlers = new NoDuplicatesList<>();
    protected final List<Function<RingBufferEvent, String>> muxCtxFunctions = new NoDuplicatesList<>();
    protected final Cache<String, Disruptor<RingBufferEvent>> eventStreams;
    protected final Disruptor<RingBufferEvent> disruptor;
    protected final int lengthHistoryBuffer;
    protected Function<String, Disruptor<RingBufferEvent>> ctxMappingFunction;

    /**
     *
     * @param filterConfig static filter configuration
     */
    @SafeVarargs
    protected EventStore(final Cache.CacheBuilder<String, Disruptor<RingBufferEvent>> muxBuilder, final Function<RingBufferEvent, String> muxCtxFunction, final int ringBufferSize, final int lengthHistoryBuffer, final int maxThreadNumber, final boolean isSingleProducer, final WaitStrategy waitStrategy, final Class<? extends Filter>... filterConfig) { // NOPMD NOSONAR - handled by factory
        assert filterConfig != null;
        if (muxCtxFunction != null) {
            this.muxCtxFunctions.add(muxCtxFunction);
        }
        this.lengthHistoryBuffer = lengthHistoryBuffer;
        this.threadFactory = new WorkerThreadFactory(EventStore.class.getSimpleName() + "Worker", maxThreadNumber);
        this.disruptor = new Disruptor<>(() -> new RingBufferEvent(filterConfig), ringBufferSize, threadFactory, isSingleProducer ? ProducerType.SINGLE : ProducerType.MULTI, waitStrategy);
        final BiConsumer<String, Disruptor<RingBufferEvent>> clearCacheElement = (muxCtx, d) -> {
            d.shutdown();
            final RingBuffer<RingBufferEvent> rb = d.getRingBuffer();
            for (long i = rb.getMinimumGatingSequence(); i < rb.getCursor(); i++) {
                rb.get(i).clear();
            }
        };
        this.eventStreams = muxBuilder == null ? Cache.<String, Disruptor<RingBufferEvent>>builder().withPostListener(clearCacheElement).build() : muxBuilder.build();

        this.ctxMappingFunction = ctx -> {
            // mux contexts -> create copy into separate disruptor/ringbuffer if necessary
            // N.B. only single writer ... no further post-processors (all done in main eventStream)
            final Disruptor<RingBufferEvent> ld = new Disruptor<>(() -> new RingBufferEvent(filterConfig), 8, threadFactory, ProducerType.SINGLE, new BlockingWaitStrategy());
            ld.start();
            return ld;
        };
    }

    public Disruptor<RingBufferEvent> getDisruptor() {
        return disruptor;
    }

    public List<RingBufferEvent> getHistory(final String muxCtx, final Predicate<RingBufferEvent> predicate, final int nHistory) {
        return getHistory(muxCtx, predicate, Long.MAX_VALUE, nHistory);
    }

    public List<RingBufferEvent> getHistory(final String muxCtx, final Predicate<RingBufferEvent> predicate, final long sequence, final int nHistory) {
        assert muxCtx != null && !muxCtx.isBlank();
        assert sequence >= 0 : "sequence = " + sequence;
        assert nHistory > 0 : "nHistory = " + nHistory;
        final Disruptor<RingBufferEvent> localDisruptor = eventStreams.computeIfAbsent(muxCtx, ctxMappingFunction);
        assert localDisruptor != null : NOT_FOUND_FOR_MULTIPLEXING_CONTEXT + muxCtx;
        final RingBuffer<RingBufferEvent> ringBuffer = localDisruptor.getRingBuffer();

        // simple consistency checks
        final long cursor = ringBuffer.getCursor();
        assert cursor >= 0 : "uninitialised cursor: " + cursor;
        assert nHistory < ringBuffer.getBufferSize()
            : (" nHistory == " + nHistory + " <! " + ringBuffer.getBufferSize());

        // search the last nHistory matching elements that match the provided predicate
        List<RingBufferEvent> history = new ArrayList<>(nHistory);
        long seqStart = Math.max(cursor - ringBuffer.getBufferSize() - 1, 0);
        for (long seq = cursor; history.size() < nHistory && seqStart <= seq; seq--) {
            final RingBufferEvent evt = ringBuffer.get(seq);
            if (evt.parentSequenceNumber <= sequence && predicate.test(evt)) {
                history.add(evt);
            }
        }
        return history;
    }

    public Optional<RingBufferEvent> getLast(final String muxCtx, final Predicate<RingBufferEvent> predicate) {
        return getLast(muxCtx, predicate, Long.MAX_VALUE);
    }

    public Optional<RingBufferEvent> getLast(final String muxCtx, final Predicate<RingBufferEvent> predicate, final long sequence) {
        assert muxCtx != null && !muxCtx.isBlank();
        final Disruptor<RingBufferEvent> localDisruptor = eventStreams.computeIfAbsent(muxCtx, ctxMappingFunction);
        assert localDisruptor != null : NOT_FOUND_FOR_MULTIPLEXING_CONTEXT + muxCtx;
        final RingBuffer<RingBufferEvent> ringBuffer = localDisruptor.getRingBuffer();
        assert ringBuffer.getCursor() > 0 : "uninitialised cursor: " + ringBuffer.getCursor();

        // search for the most recent element that matches the provided predicate
        long seqStart = Math.max(ringBuffer.getCursor() - ringBuffer.getBufferSize() - 1, 0);
        for (long seq = ringBuffer.getCursor(); seqStart <= seq; seq--) {
            final RingBufferEvent evt = ringBuffer.get(seq);
            if (evt.parentSequenceNumber <= sequence && predicate.test(evt)) {
                return Optional.of(evt.clone());
            }
        }
        return Optional.empty();
    }

    public RingBuffer<RingBufferEvent> getRingBuffer() {
        return disruptor.getRingBuffer();
    }

    @SafeVarargs
    public final LocalEventHandlerGroup register(final EventHandler<RingBufferEvent>... eventHandler) {
        final LocalEventHandlerGroup group = new LocalEventHandlerGroup(lengthHistoryBuffer, eventHandler);
        listener.add(group);
        return group;
    }

    public final LocalEventHandlerGroup register(final Predicate<RingBufferEvent> filter, Function<RingBufferEvent, String> muxCtxFunction, final HistoryEventHandler... eventHandler) {
        final LocalEventHandlerGroup group = new LocalEventHandlerGroup(lengthHistoryBuffer, filter, muxCtxFunction, eventHandler);
        listener.add(group);
        return group;
    }

    public void start(final boolean startReaper) {
        // create single writer that is always executed first
        EventHandler<RingBufferEvent> muxCtxWriter = (evt, seq, batch) -> {
            for (Function<RingBufferEvent, String> muxCtxFunc : muxCtxFunctions) {
                final String muxCtx = muxCtxFunc.apply(evt);
                // only single writer ... no further post-processors (all done in main eventStream)
                final Disruptor<RingBufferEvent> localDisruptor = eventStreams.computeIfAbsent(muxCtx, ctxMappingFunction);
                assert localDisruptor != null : NOT_FOUND_FOR_MULTIPLEXING_CONTEXT + muxCtx;

                if (!localDisruptor.getRingBuffer().tryPublishEvent((event, sequence) -> {
                        if (event.payload != null && event.payload.getReferenceCount() > 0) {
                            event.payload.release();
                        }
                        evt.copyTo(event);
                    })) {
                    throw new IllegalStateException("could not write event, sequence = " + seq + " muxCtx = " + muxCtx);
                }
            }
        };
        allEventHandlers.add(muxCtxWriter);
        EventHandlerGroup<RingBufferEvent> handlerGroup = disruptor.handleEventsWith(muxCtxWriter);

        // add other handler
        for (LocalEventHandlerGroup localHandlerGroup : listener) {
            attachHandler(disruptor, handlerGroup, localHandlerGroup);
        }

        assert handlerGroup != null;
        @SuppressWarnings("unchecked")
        EventHandler<RingBufferEvent>[] eventHandlers = allEventHandlers.toArray(new EventHandler[0]);
        if (startReaper) {
            // start the reaper thread for this given ring buffer
            disruptor.after(eventHandlers).then(new RingBufferEvent.ClearEventHandler());
        }

        // register this event store to all DefaultHistoryEventHandler
        for (EventHandler<?> handler : allEventHandlers) {
            if (handler instanceof DefaultHistoryEventHandler) {
                ((DefaultHistoryEventHandler) handler).setEventStore(this);
            }
        }

        disruptor.start();
    }

    public void start() {
        this.start(true);
    }

    public void stop() {
        disruptor.shutdown();
    }

    protected EventHandlerGroup<RingBufferEvent> attachHandler(final Disruptor<RingBufferEvent> disruptor, final EventHandlerGroup<RingBufferEvent> parentGroup, final LocalEventHandlerGroup localHandlerGroup) {
        EventHandlerGroup<RingBufferEvent> handlerGroup;
        @SuppressWarnings("unchecked")
        EventHandler<RingBufferEvent>[] eventHanders = localHandlerGroup.handler.toArray(new EventHandler[0]);
        allEventHandlers.addAll(localHandlerGroup.handler);
        if (parentGroup == null) {
            handlerGroup = disruptor.handleEventsWith(eventHanders);
        } else {
            handlerGroup = parentGroup.then(eventHanders);
        }

        if (localHandlerGroup.dependent != null && !localHandlerGroup.handler.isEmpty()) {
            handlerGroup = attachHandler(disruptor, handlerGroup, localHandlerGroup.dependent);
        }

        return handlerGroup;
    }

    public static EventStoreFactory getFactory() {
        return new EventStoreFactory();
    }

    public static class LocalEventHandlerGroup {
        protected final List<EventHandler<RingBufferEvent>> handler = new NoDuplicatesList<>();
        protected final int lengthHistoryBuffer;
        protected LocalEventHandlerGroup dependent;

        @SafeVarargs
        private LocalEventHandlerGroup(final int lengthHistoryBuffer, final EventHandler<RingBufferEvent>... eventHandler) {
            assert eventHandler != null;
            this.lengthHistoryBuffer = lengthHistoryBuffer;
            handler.addAll(Arrays.asList(eventHandler));
        }

        private LocalEventHandlerGroup(final int lengthHistoryBuffer, final Predicate<RingBufferEvent> filter, Function<RingBufferEvent, String> muxCtxFunction, final HistoryEventHandler... eventHandlerCallbacks) {
            assert eventHandlerCallbacks != null;
            this.lengthHistoryBuffer = lengthHistoryBuffer;
            for (final HistoryEventHandler callback : eventHandlerCallbacks) {
                handler.add(new DefaultHistoryEventHandler(null, filter, muxCtxFunction, lengthHistoryBuffer, callback)); // NOPMD - necessary to allocate inside loop
            }
        }

        @SafeVarargs
        public final LocalEventHandlerGroup and(final EventHandler<RingBufferEvent>... eventHandler) {
            assert eventHandler != null;
            handler.addAll(Arrays.asList(eventHandler));
            return this;
        }

        public final LocalEventHandlerGroup and(final Predicate<RingBufferEvent> filter, Function<RingBufferEvent, String> muxCtxFunction, final HistoryEventHandler... eventHandlerCallbacks) {
            assert eventHandlerCallbacks != null;
            for (final HistoryEventHandler callback : eventHandlerCallbacks) {
                handler.add(new DefaultHistoryEventHandler(null, filter, muxCtxFunction, lengthHistoryBuffer, callback)); // NOPMD - necessary to allocate inside loop
            }
            return this;
        }

        @SafeVarargs
        public final LocalEventHandlerGroup then(final EventHandler<RingBufferEvent>... eventHandler) {
            return (dependent = new LocalEventHandlerGroup(lengthHistoryBuffer, eventHandler)); // NOPMD NOSONAR
        }

        public final LocalEventHandlerGroup then(final Predicate<RingBufferEvent> filter, Function<RingBufferEvent, String> muxCtxFunction, final HistoryEventHandler... eventHandlerCallbacks) {
            return (dependent = new LocalEventHandlerGroup(lengthHistoryBuffer, filter, muxCtxFunction, eventHandlerCallbacks)); // NOPMD NOSONAR
        }
    }

    public static class EventStoreFactory {
        private boolean singleProducer;
        private int maxThreadNumber = 4;
        private int ringbufferSize = 64;
        private int lengthHistoryBuffer = 10;
        private Cache.CacheBuilder<String, Disruptor<RingBufferEvent>> muxBuilder;
        private Function<RingBufferEvent, String> muxCtxFunction;
        private WaitStrategy waitStrategy = new TimeoutBlockingWaitStrategy(100, TimeUnit.MILLISECONDS);
        @SuppressWarnings("unchecked")
        private Class<? extends Filter>[] filterConfig = new Class[0];

        public EventStore build() {
            if (muxBuilder == null) {
                muxBuilder = Cache.<String, Disruptor<RingBufferEvent>>builder().withLimit(lengthHistoryBuffer);
            }
            return new EventStore(muxBuilder, muxCtxFunction, ringbufferSize, lengthHistoryBuffer, maxThreadNumber, singleProducer, waitStrategy, filterConfig);
        }

        public Class<? extends Filter>[] getFilterConfig() {
            return filterConfig;
        }

        @SafeVarargs
        @SuppressWarnings("PMD.ArrayIsStoredDirectly")
        public final EventStoreFactory setFilterConfig(final Class<? extends Filter>... filterConfig) {
            if (filterConfig == null) {
                throw new IllegalArgumentException("filterConfig is null");
            }
            this.filterConfig = filterConfig;
            return this;
        }

        public int getLengthHistoryBuffer() {
            return lengthHistoryBuffer;
        }

        public EventStoreFactory setLengthHistoryBuffer(final int lengthHistoryBuffer) {
            if (lengthHistoryBuffer < 0) {
                throw new IllegalArgumentException("lengthHistoryBuffer < 0: " + lengthHistoryBuffer);
            }

            this.lengthHistoryBuffer = lengthHistoryBuffer;
            return this;
        }

        public int getMaxThreadNumber() {
            return maxThreadNumber;
        }

        public EventStoreFactory setMaxThreadNumber(final int maxThreadNumber) {
            this.maxThreadNumber = maxThreadNumber;
            return this;
        }

        public Cache.CacheBuilder<String, Disruptor<RingBufferEvent>> getMuxBuilder() {
            return muxBuilder;
        }

        public EventStoreFactory setMuxBuilder(final Cache.CacheBuilder<String, Disruptor<RingBufferEvent>> muxBuilder) {
            this.muxBuilder = muxBuilder;
            return this;
        }

        public Function<RingBufferEvent, String> getMuxCtxFunction() {
            return muxCtxFunction;
        }

        public EventStoreFactory setMuxCtxFunction(final Function<RingBufferEvent, String> muxCtxFunction) {
            this.muxCtxFunction = muxCtxFunction;
            return this;
        }

        public int getRingbufferSize() {
            return ringbufferSize;
        }

        public EventStoreFactory setRingbufferSize(final int ringbufferSize) {
            if (ringbufferSize < 0) {
                throw new IllegalArgumentException("lengthHistoryBuffer < 0: " + ringbufferSize);
            }
            final int rounded = Util.ceilingNextPowerOfTwo(ringbufferSize - 1);
            if (ringbufferSize != rounded) {
                LOGGER.atWarn().addArgument(ringbufferSize).addArgument(rounded).log("setRingbufferSize({}) is not a power of two setting to next power of two: {}");
                this.ringbufferSize = rounded;
                return this;
            }

            this.ringbufferSize = ringbufferSize;
            return this;
        }

        public WaitStrategy getWaitStrategy() {
            return waitStrategy;
        }

        public EventStoreFactory setWaitStrategy(final WaitStrategy waitStrategy) {
            this.waitStrategy = waitStrategy;
            return this;
        }

        public boolean isSingleProducer() {
            return singleProducer;
        }

        public EventStoreFactory setSingleProducer(final boolean singleProducer) {
            this.singleProducer = singleProducer;
            return this;
        }
    }

    protected static class DefaultHistoryEventHandler implements EventHandler<RingBufferEvent> {
        private final Predicate<RingBufferEvent> filter;
        private final Function<RingBufferEvent, String> muxCtxFunction;
        private final HistoryEventHandler callback;
        private final int lengthHistoryBuffer;
        private EventStore eventStore;
        private Cache<String, LimitedArrayList<RingBufferEvent>> historyCache;

        protected DefaultHistoryEventHandler(final EventStore eventStore, final Predicate<RingBufferEvent> filter, Function<RingBufferEvent, String> muxCtxFunction, final int lengthHistoryBuffer, final HistoryEventHandler callback) {
            assert filter != null : "filter predicate is null";
            assert muxCtxFunction != null : "muxCtxFunction hash function is null";
            assert callback != null : "callback function must not be null";

            this.eventStore = eventStore;
            this.filter = filter;
            this.muxCtxFunction = muxCtxFunction;
            this.lengthHistoryBuffer = lengthHistoryBuffer;
            this.callback = callback;
        }

        @Override
        public void onEvent(final RingBufferEvent event, final long sequence, final boolean endOfBatch) {
            if (!filter.test(event)) {
                return;
            }
            final String muxCtx = muxCtxFunction.apply(event);
            final LimitedArrayList<RingBufferEvent> history = historyCache.computeIfAbsent(muxCtx, ctx -> new LimitedArrayList<>(lengthHistoryBuffer));

            final RingBufferEvent eventCopy = event.clone();
            if (history.size() == history.getLimit()) {
                final RingBufferEvent removedEvent = history.remove(history.size() - 1);
                removedEvent.clear();
            }
            history.add(0, eventCopy);

            final RingBufferEvent result;
            try {
                result = callback.onEvent(history, eventStore, sequence, endOfBatch);
            } catch (Exception e) { // NOPMD - part of exception handling/forwarding scheme
                LOGGER.atError().setCause(e).addArgument(history.size()).addArgument(sequence).addArgument(endOfBatch) //
                        .log("caught error for arguments (history={}, eventStore, sequence={}, endOfBatch={})");
                event.throwables.add(e);
                return;
            }
            if (result == null) {
                return;
            }
            eventStore.getRingBuffer().publishEvent((newEvent, newSequence) -> {
                result.copyTo(newEvent);
                newEvent.parentSequenceNumber = newSequence;
            });
        }

        private void setEventStore(final EventStore eventStore) {
            this.eventStore = eventStore;
            // allocate cache
            final BiConsumer<String, LimitedArrayList<RingBufferEvent>> clearCacheElement = (muxCtx, history) -> history.forEach(RingBufferEvent::clear);
            final Cache<?, ?> c = eventStore.eventStreams; // re-use existing config limits
            historyCache = Cache.<String, LimitedArrayList<RingBufferEvent>>builder().withLimit((int) c.getLimit()).withTimeout(c.getTimeout(), c.getTimeUnit()).withPostListener(clearCacheElement).build();
        }
    }
}
