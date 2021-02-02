package io.opencmw;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.spi.LoggingEventBuilder;

import io.opencmw.filter.EvtTypeFilter;
import io.opencmw.filter.TimingCtx;
import io.opencmw.utils.Cache;
import io.opencmw.utils.SharedPointer;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.TimeoutHandler;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;

class EventStoreTest {
    private final static Logger LOGGER = LoggerFactory.getLogger(EventStoreTest.class);
    private final static boolean IN_ORDER = true;

    @Test
    void testFactory() {
        assertDoesNotThrow(EventStore::getFactory);

        final EventStore.EventStoreFactory factory = EventStore.getFactory();
        assertDoesNotThrow(factory::build);

        assertEquals(0, factory.getFilterConfig().length);
        factory.setFilterConfig(TimingCtx.class, EvtTypeFilter.class);
        assertArrayEquals(new Class[] { TimingCtx.class, EvtTypeFilter.class }, factory.getFilterConfig());

        factory.setLengthHistoryBuffer(42);
        assertEquals(42, factory.getLengthHistoryBuffer());

        factory.setMaxThreadNumber(7);
        assertEquals(7, factory.getMaxThreadNumber());

        factory.setRingbufferSize(128);
        assertEquals(128, factory.getRingbufferSize());
        factory.setRingbufferSize(42);
        assertEquals(64, factory.getRingbufferSize());

        factory.setSingleProducer(true);
        assertTrue(factory.isSingleProducer());
        factory.setSingleProducer(false);
        assertFalse(factory.isSingleProducer());

        final Function<RingBufferEvent, String> muxCtx = evt -> "cid=" + evt.getFilter(TimingCtx.class).cid;
        assertNotEquals(muxCtx, factory.getMuxCtxFunction());
        factory.setMuxCtxFunction(muxCtx);
        assertEquals(muxCtx, factory.getMuxCtxFunction());

        final Cache.CacheBuilder<String, Disruptor<RingBufferEvent>> ctxCacheBuilder = Cache.<String, Disruptor<RingBufferEvent>>builder().withLimit(100);
        final WaitStrategy customWaitStrategy = new YieldingWaitStrategy();
        assertNotEquals(ctxCacheBuilder, factory.getMuxBuilder());
        assertNotEquals(customWaitStrategy, factory.getWaitStrategy());
        factory.setWaitStrategy(customWaitStrategy).setMuxBuilder(ctxCacheBuilder);
        assertEquals(customWaitStrategy, factory.getWaitStrategy());
        assertEquals(ctxCacheBuilder, factory.getMuxBuilder());
    }

    @Test
    void basicTest() {
        assertDoesNotThrow(() -> EventStore.getFactory().setFilterConfig(TimingCtx.class, EvtTypeFilter.class).build());

        // global multiplexing context function -> generate new EventStream per detected context, here: multiplexed on {@see TimingCtx#cid}
        final Function<RingBufferEvent, String> muxCtx = evt -> "cid=" + evt.getFilter(TimingCtx.class).cid;
        final Cache.CacheBuilder<String, Disruptor<RingBufferEvent>> ctxCacheBuilder = Cache.<String, Disruptor<RingBufferEvent>>builder().withLimit(100);
        final EventStore es = EventStore.getFactory().setMuxCtxFunction(muxCtx).setMuxBuilder(ctxCacheBuilder).setFilterConfig(TimingCtx.class, EvtTypeFilter.class).build();

        Predicate<RingBufferEvent> filterBp1 = evt -> evt.test(TimingCtx.class, TimingCtx.matches(-1, -1, 1));

        es.register(filterBp1, muxCtx, (evts, evtStore, seq, eob) -> {
              LOGGER.atTrace().addArgument(evts.get(0).payload.get()).log("SequencedFilteredTask 1.0: received cid == 1 : payload = {}");
              return null;
          }).and(filterBp1, muxCtx, (evts, evtStore, seq, eob) -> {
                LOGGER.atTrace().addArgument(evts.get(0).payload.get()).log("SequencedFilteredTask 1.1: received cid == 1 : payload = {}");
                return null;
            }).and(filterBp1, muxCtx, (evts, evtStore, seq, eob) -> {
                  LOGGER.atTrace().addArgument(evts.get(0).payload.get()).log("SequencedFilteredTask 1.2: received cid == 1 : payload = {}");
                  return null;
              }).then(filterBp1, muxCtx, (evts, evtStore, seq, eob) -> {
                    LOGGER.atTrace().addArgument(evts.get(0).payload.get()).log("SequencedFilteredTask 2.0: received cid == 1 : payload = {}");
                    return null;
                }).then(filterBp1, muxCtx, (evts, evtStore, seq, eob) -> {
                      LOGGER.atTrace().addArgument(evts.get(0).payload.get()).log("SequencedFilteredTask 3.0: received cid == 1 : payload = {}");
                      return null;
                  }).and(filterBp1, muxCtx, (evts, evtStore, seq, eob) -> {
                        LOGGER.atTrace().addArgument(evts.get(0).payload.get()).log("SequencedFilteredTask 3.1: received cid == 1 : payload = {}");
                        return null;
                    }).and(filterBp1, muxCtx, (evts, evtStore, seq, eob) -> {
            LOGGER.atTrace().addArgument(evts.get(0).payload.get()).log("SequencedFilteredTask 3.2: received cid == 1 : payload = {}");
            return null;
        });

        Predicate<RingBufferEvent> filterBp0 = evt -> evt.test(TimingCtx.class, TimingCtx.matches(-1, -1, 0));
        es.register(filterBp0, muxCtx, (evts, evtStore, seq, eob) -> {
            final String history = evts.stream().map(b -> (String) b.payload.get()).collect(Collectors.joining(", ", "(", ")"));
            final String historyAlt = es.getHistory(muxCtx.apply(evts.get(0)), filterBp0, seq, 30).stream().map(b -> (String) b.payload.get()).collect(Collectors.joining(", ", "(", ")"));
            LOGGER.atTrace().addArgument(history).log("@@@EventHandler with history: {}");

            // check identity between the two reference implementations
            assertEquals(evts.size(), es.getHistory(muxCtx.apply(evts.get(0)), filterBp0, seq, 30).size());
            assertEquals(history, historyAlt);
            return null;
        });

        assertNotNull(es.getRingBuffer());

        es.start();
        testPublish(es, LOGGER.atTrace(), "message A", 0);
        testPublish(es, LOGGER.atTrace(), "message B", 0);
        testPublish(es, LOGGER.atTrace(), "message C", 0);
        testPublish(es, LOGGER.atTrace(), "message A", 1);
        testPublish(es, LOGGER.atTrace(), "message D", 0);
        testPublish(es, LOGGER.atTrace(), "message E", 0);

        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100)); // give a bit of time until all workers are finished
        es.stop();
    }

    @Test
    void attachEventHandlerTest() {
        final Cache.CacheBuilder<String, Disruptor<RingBufferEvent>> ctxCacheBuilder = Cache.<String, Disruptor<RingBufferEvent>>builder().withLimit(100);
        final Function<RingBufferEvent, String> muxCtx = evt -> "cid=" + evt.getFilter(TimingCtx.class).cid;
        final EventStore es = EventStore.getFactory().setMuxCtxFunction(muxCtx).setMuxBuilder(ctxCacheBuilder).setFilterConfig(TimingCtx.class, EvtTypeFilter.class).build();

        final AtomicInteger handlerCount1 = new AtomicInteger();
        final AtomicInteger handlerCount2 = new AtomicInteger();
        final AtomicInteger handlerCount3 = new AtomicInteger();
        es.register((evt, seq, eob) -> handlerCount1.incrementAndGet())
                .and((evt, seq, eob) -> handlerCount2.incrementAndGet())
                .then((evt, seq, eob) -> {
                    assertTrue(handlerCount1.get() >= handlerCount3.get());
                    assertTrue(handlerCount2.get() >= handlerCount3.get());
                    handlerCount3.incrementAndGet();
                });

        es.start();
        testPublish(es, LOGGER.atTrace(), "A", 0);
        testPublish(es, LOGGER.atTrace(), "B", 0);
        testPublish(es, LOGGER.atTrace(), "C", 0);
        testPublish(es, LOGGER.atTrace(), "A", 1);
        testPublish(es, LOGGER.atTrace(), "D", 0);
        testPublish(es, LOGGER.atTrace(), "E", 0);
        Awaitility.await().atMost(200, TimeUnit.MILLISECONDS).until(() -> {
            es.stop();
            return true; });

        assertEquals(6, handlerCount1.get());
        assertEquals(6, handlerCount2.get());
        assertEquals(6, handlerCount3.get());
    }

    @Test
    void attachHistoryEventHandlerTest() {
        final Cache.CacheBuilder<String, Disruptor<RingBufferEvent>> ctxCacheBuilder = Cache.<String, Disruptor<RingBufferEvent>>builder().withLimit(100);
        final Function<RingBufferEvent, String> muxCtx = evt -> "cid=" + evt.getFilter(TimingCtx.class).cid;
        final EventStore es = EventStore.getFactory().setMuxCtxFunction(muxCtx).setMuxBuilder(ctxCacheBuilder).setFilterConfig(TimingCtx.class, EvtTypeFilter.class).build();

        final AtomicInteger handlerCount1 = new AtomicInteger();
        final AtomicInteger handlerCount2 = new AtomicInteger();
        final AtomicInteger handlerCount3 = new AtomicInteger();
        Predicate<RingBufferEvent> filterBp1 = evt -> evt.test(TimingCtx.class, TimingCtx.matches(-1, -1, 0));
        es.register(filterBp1, muxCtx, (evts, evtStore, seq, eob) -> {
              handlerCount1.incrementAndGet();
              return null;
          }).and(filterBp1, muxCtx, (evts, evtStore, seq, eob) -> {
                handlerCount2.incrementAndGet();
                return null;
            }).then(filterBp1, muxCtx, (evts, evtStore, seq, eob) -> {
            assertTrue(handlerCount1.get() >= handlerCount3.get());
            assertTrue(handlerCount2.get() >= handlerCount3.get());
            handlerCount3.incrementAndGet();
            return null;
        });

        es.start();
        testPublish(es, LOGGER.atTrace(), "A", 0);
        testPublish(es, LOGGER.atTrace(), "B", 0);
        testPublish(es, LOGGER.atTrace(), "C", 0);
        testPublish(es, LOGGER.atTrace(), "A", 1);
        testPublish(es, LOGGER.atTrace(), "D", 0);
        testPublish(es, LOGGER.atTrace(), "E", 0);
        Awaitility.await().atMost(200, TimeUnit.MILLISECONDS).until(() -> {
            es.stop();
            return true; });

        assertEquals(5, handlerCount1.get());
        assertEquals(5, handlerCount2.get());
        assertEquals(5, handlerCount3.get());
    }

    @Test
    void historyTest() {
        final Cache.CacheBuilder<String, Disruptor<RingBufferEvent>> ctxCacheBuilder = Cache.<String, Disruptor<RingBufferEvent>>builder().withLimit(100);
        final Function<RingBufferEvent, String> muxCtx = evt -> "cid=" + evt.getFilter(TimingCtx.class).cid;
        final EventStore es = EventStore.getFactory().setMuxCtxFunction(muxCtx).setMuxBuilder(ctxCacheBuilder).setFilterConfig(TimingCtx.class, EvtTypeFilter.class).build();

        es.start();
        testPublish(es, LOGGER.atTrace(), "A", 0);
        testPublish(es, LOGGER.atTrace(), "B", 0);
        testPublish(es, LOGGER.atTrace(), "C", 0);
        testPublish(es, LOGGER.atTrace(), "A", 1);
        testPublish(es, LOGGER.atTrace(), "D", 0);
        testPublish(es, LOGGER.atTrace(), "E", 0);
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100)); // give a bit of time until all workers are finished

        final Optional<RingBufferEvent> lastEvent = es.getLast("cid=0", evt -> true);
        assertTrue(lastEvent.isPresent());
        assertFalse(es.getLast("cid=0", evt -> evt.matches(TimingCtx.class, TimingCtx.matches(-1, -1, 2))).isPresent());
        LOGGER.atTrace().addArgument(lastEvent.get().payload.get(String.class)).log("retrieved last event  = {}");

        final List<RingBufferEvent> eventHistory = es.getHistory("cid=0", evt -> evt.matches(String.class) && evt.matches(TimingCtx.class, TimingCtx.matches(-1, -1, 0)), 4);
        final String history = eventHistory.stream().map(b -> b.payload.get(String.class)).collect(Collectors.joining(", ", "(", ")"));
        LOGGER.atTrace().addArgument(history).log("retrieved last events = {}");

        es.stop();
    }

    public static void main(final String[] args) {
        // global multiplexing context function -> generate new EventStream per detected context, here: multiplexed on {@see TimingCtx#cid}
        final Function<RingBufferEvent, String> muxCtx = evt -> "cid=" + evt.getFilter(TimingCtx.class).cid;
        final Cache.CacheBuilder<String, Disruptor<RingBufferEvent>> ctxCacheBuilder = Cache.<String, Disruptor<RingBufferEvent>>builder().withLimit(100);
        final EventStore es = EventStore.getFactory().setMuxCtxFunction(muxCtx).setMuxBuilder(ctxCacheBuilder).setFilterConfig(TimingCtx.class, EvtTypeFilter.class).build();

        final MyHandler handler1 = new MyHandler("Handler1", es.getRingBuffer());
        MyHandler handler2 = new MyHandler("Handler2", es.getRingBuffer());
        EventHandler<RingBufferEvent> lambdaEventHandler = (evt, seq, buffer) -> //
                LOGGER.atInfo().addArgument(seq).addArgument(evt.payload.get()).addArgument(es.getRingBuffer().getMinimumGatingSequence()).log("Lambda-Handler3 seq:{} - '{}' - gate ='{}'");

        if (IN_ORDER) {
            // execute in order
            es.register(handler1).then(handler2).then(lambdaEventHandler);
        } else {
            //execute out-of-order
            es.register(handler1).and(handler2).and(lambdaEventHandler);
        }

        Predicate<RingBufferEvent> filterBp1 = evt -> evt.test(TimingCtx.class, TimingCtx.matches(-1, -1, 1));
        es.register(filterBp1, muxCtx, (evts, evtStore, seq, eob) -> {
              LOGGER.atInfo().addArgument(evts.get(0).payload.get()).log("SequencedFilteredTask 1.0: received cid == 1 : payload = {}");
              return null;
          }).and(filterBp1, muxCtx, (evts, evtStore, seq, eob) -> {
                LOGGER.atInfo().addArgument(evts.get(0).payload.get()).log("SequencedFilteredTask 1.1: received cid == 1 : payload = {}");
                return null;
            }).and(filterBp1, muxCtx, (evts, evtStore, seq, eob) -> {
                  LOGGER.atInfo().addArgument(evts.get(0).payload.get()).log("SequencedFilteredTask 1.2: received cid == 1 : payload = {}");
                  return null;
              }).then(filterBp1, muxCtx, (evts, evtStore, seq, eob) -> {
                    LOGGER.atInfo().addArgument(evts.get(0).payload.get()).log("SequencedFilteredTask 2.0: received cid == 1 : payload = {}");
                    return null;
                }).then(filterBp1, muxCtx, (evts, evtStore, seq, eob) -> {
                      LOGGER.atInfo().addArgument(evts.get(0).payload.get()).log("SequencedFilteredTask 3.0: received cid == 1 : payload = {}");
                      return null;
                  }).and(filterBp1, muxCtx, (evts, evtStore, seq, eob) -> {
                        LOGGER.atInfo().addArgument(evts.get(0).payload.get()).log("SequencedFilteredTask 3.1: received cid == 1 : payload = {}");
                        return null;
                    }).and(filterBp1, muxCtx, (evts, evtStore, seq, eob) -> {
            LOGGER.atInfo().addArgument(evts.get(0).payload.get()).log("SequencedFilteredTask 3.2: received cid == 1 : payload = {}");
            return null;
        });

        EventHandler<RingBufferEvent> printEndHandler = (evt, seq, buffer) -> //
                LOGGER.atInfo().addArgument(es.getRingBuffer().getMinimumGatingSequence()) //
                        .addArgument(es.disruptor.getSequenceValueFor(handler1)) //
                        .addArgument(es.disruptor.getSequenceValueFor(handler2)) //
                        .addArgument(es.disruptor.getSequenceValueFor(lambdaEventHandler)) //
                        .addArgument(seq) //
                        .log("### gating position = {} sequences for handler 1: {} 2: {} 3:{} ph: {}");

        es.register(printEndHandler);

        Predicate<RingBufferEvent> filterBp0 = evt -> evt.test(TimingCtx.class, TimingCtx.matches(-1, -1, 0));
        es.register(filterBp0, muxCtx, (evts, evtStore, seq, eob) -> {
            final String history = evts.stream().map(b -> (String) b.payload.get()).collect(Collectors.joining(", ", "(", ")"));
            final String historyAlt = es.getHistory(muxCtx.apply(evts.get(0)), filterBp0, seq, 30).stream().map(b -> (String) b.payload.get()).collect(Collectors.joining(", ", "(", ")"));
            LOGGER.atInfo().addArgument(history).log("@@@EventHandlerA with history: {}");
            LOGGER.atInfo().addArgument(historyAlt).log("@@@EventHandlerB with history: {}");
            return null;
        });

        es.start();

        testPublish(es, LOGGER.atInfo(), "message A", 0);
        testPublish(es, LOGGER.atInfo(), "message B", 0);
        testPublish(es, LOGGER.atInfo(), "message C", 0);
        testPublish(es, LOGGER.atInfo(), "message A", 1);
        testPublish(es, LOGGER.atInfo(), "message D", 0);
        testPublish(es, LOGGER.atInfo(), "message E", 0);
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100)); // give a bit of time until all workers are finished
        es.stop();
    }

    protected static void testPublish(EventStore eventStore, final LoggingEventBuilder logger, final String payLoad, final int beamProcess) {
        eventStore.getRingBuffer().publishEvent((event, sequence, buffer) -> {
            event.arrivalTimeStamp = System.currentTimeMillis() * 1000;
            event.parentSequenceNumber = sequence;
            event.getFilter(TimingCtx.class).setSelector("FAIR.SELECTOR.C=0:S=0:P=" + beamProcess, event.arrivalTimeStamp);
            event.payload = new SharedPointer<>();
            event.payload.set("pid=" + beamProcess + ": " + payLoad);
            logger.addArgument(sequence).addArgument(event.payload.get()).addArgument(buffer).log("publish Seq:{} - event:'{}' buffer:'{}'");
        });
    }

    public static class MyHandler implements EventHandler<RingBufferEvent>, TimeoutHandler, LifecycleAware {
        private final RingBuffer<?> ringBuffer;

        private final String handlerName;

        public MyHandler(final String handlerName, final RingBuffer<?> ringBuffer) {
            this.handlerName = handlerName;
            this.ringBuffer = ringBuffer;
        }

        @Override
        public void onEvent(final RingBufferEvent event, final long sequence, final boolean endOfBatch) {
            LOGGER.atInfo().addArgument(handlerName).addArgument(sequence).addArgument(event.payload.get()).log("'{}'- process sequence ID: {} event = {}");
        }

        @Override
        public void onShutdown() {
            LOGGER.atInfo().addArgument(MyHandler.class).addArgument(handlerName).log("stopped '{}'-name:'{}'");
        }

        @Override
        public void onStart() {
            LOGGER.atInfo().addArgument(MyHandler.class).addArgument(handlerName).log("started '{}'-name:'{}'");
        }

        @Override
        public void onTimeout(final long sequence) {
            LOGGER.atInfo().addArgument(handlerName).addArgument(sequence).addArgument(ringBuffer.getMinimumGatingSequence()).log("onTimeout '{}'-sequence:'{}' - gate:'{}'");
        }
    }
}
