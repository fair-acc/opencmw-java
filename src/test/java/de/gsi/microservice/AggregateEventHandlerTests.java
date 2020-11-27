package de.gsi.microservice;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.awaitility.Awaitility;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.TimeoutBlockingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.EventHandlerGroup;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;

import de.gsi.microservice.filter.EvtTypeFilter;
import de.gsi.microservice.filter.TimingCtx;
import de.gsi.microservice.utils.SharedPointer;

/**
 * Unit-Test for {@link de.gsi.microservice.AggregateEventHandler}
 *
 * @author Alexander Krimm
 * @author rstein
 */
class AggregateEventHandlerTests {
    private static final Logger LOGGER = LoggerFactory.getLogger(AggregateEventHandlerTests.class);
    private static final String[] DEVICES = { "a", "b", "c" };
    private static Stream<Arguments> workingEventSamplesProvider() { // NOPMD NOSONAR - false-positive PMD error (unused function), function is used via reflection
        return Stream.of(
                arguments("ordinary", "a1 b1 c1 a2 b2 c2 a3 b3 c3", "a1 b1 c1; a2 b2 c2; a3 b3 c3", "", 1),
                arguments("duplicate events", "a1 b1 c1 b1 a2 b2 c2 a2 a3 b3 c3 c3", "a1 b1 c1; a2 b2 c2; a3 b3 c3", "", 1),
                arguments("reordered", "a1 c1 b1 a2 b2 c2 a3 b3 c3", "a1 b1 c1; a2 b2 c2; a3 b3 c3", "", 1),
                arguments("interleaved", "a1 b1 a2 b2 c1 a3 b3 c2 c3", "a1 b1 c1; a2 b2 c2; a3 b3 c3", "", 1),
                arguments("missing event", "a1 b1 a2 b2 c2 a3 b3 c3", "a1 b1; a2 b2 c2; a3 b3 c3", "1", 1),
                arguments("missing device", "a1 b1 a2 b2 a3 b3", "a1 b1; a2 b2; a3 b3", "1 2 3", 1),
                arguments("late", "a1 b1 a2 b2 c2 a3 b3 c3 c1", "a1 b1 c1; a2 b2 c2; a3 b3 c3", "", 1),
                arguments("timeout without event", "a1 b1 c1 a2 b2", "a1 b1 c1; a2 b2", "2", 1),
                arguments("long queue", "a1 b1 c1 a2 b2", "a1 b1 c1; a2 b2; a1001 b1001 c1001; a1002 b1002; a2001 b2001 c2001; a2002 b2002; a3001 b3001 c3001; a3002 b3002; a4001 b4001 c4001; a4002 b4002", "2 1002 2002 3002 4002", 5),
                arguments("simple broken long queue", "a1 b1", "a1 b1; a1001 b1001; a2001 b2001; a3001 b3001; a4001 b4001", "1 1001 2001 3001 4001", 5),
                arguments("single event timeout", "a1 b1 pause pause c1", "a1 b1", "1", 1));
    }

    @Test
    void testFactoryMethods() {
        assertDoesNotThrow(AggregateEventHandler::getFactory);

        final AggregateEventHandler.AggregateEventHandlerFactory factory = AggregateEventHandler.getFactory();
        assertThrows(IllegalArgumentException.class, factory::build); // missing aggregate name

        factory.setAggregateName("testAggregate");
        assertEquals("testAggregate", factory.getAggregateName());

        assertTrue(factory.getDeviceList().isEmpty());
        factory.setDeviceList("testDevice1", "testDevice2");
        assertEquals(List.of("testDevice1", "testDevice2"), factory.getDeviceList());
        factory.getDeviceList().clear();
        assertTrue(factory.getDeviceList().isEmpty());
        factory.setDeviceList(List.of("testDevice1", "testDevice2"));
        assertEquals(List.of("testDevice1", "testDevice2"), factory.getDeviceList());
        factory.getDeviceList().clear();

        Predicate<RingBufferEvent> filter1 = rbEvt -> true;
        Predicate<RingBufferEvent> filter2 = rbEvt -> false;
        assertTrue(factory.getEvtTypeFilter().isEmpty());
        factory.setEvtTypeFilter(filter1, filter2);
        assertEquals(List.of(filter1, filter2), factory.getEvtTypeFilter());
        factory.getEvtTypeFilter().clear();
        assertTrue(factory.getEvtTypeFilter().isEmpty());
        factory.setEvtTypeFilter(List.of(filter1, filter2));
        assertEquals(List.of(filter1, filter2), factory.getEvtTypeFilter());
        factory.getEvtTypeFilter().clear();

        factory.setNumberWorkers(42);
        assertEquals(42, factory.getNumberWorkers());
        assertThrows(IllegalArgumentException.class, () -> factory.setNumberWorkers(0));

        factory.setRetentionSize(13);
        assertEquals(13, factory.getRetentionSize());
        assertThrows(IllegalArgumentException.class, () -> factory.setRetentionSize(0));

        factory.setTimeOut(2, TimeUnit.SECONDS);
        assertEquals(2, factory.getTimeOut());
        assertEquals(TimeUnit.SECONDS, factory.getTimeOutUnit());
        assertThrows(IllegalArgumentException.class, () -> factory.setTimeOut(0, TimeUnit.SECONDS));
        assertThrows(IllegalArgumentException.class, () -> factory.setTimeOut(1, null));

        assertThrows(IllegalArgumentException.class, factory::build); // missing ring buffer assignment
        RingBuffer<RingBufferEvent> ringBuffer = RingBuffer.createSingleProducer(RingBufferEvent::new, 8, new BlockingWaitStrategy());
        assertThrows(IllegalArgumentException.class, () -> factory.setRingBuffer(null));
        factory.setRingBuffer(ringBuffer);
        assertEquals(ringBuffer, factory.getRingBuffer());

        assertDoesNotThrow(factory::build);
    }

    @ParameterizedTest
    @MethodSource("workingEventSamplesProvider")
    void testSimpleEvents(final String eventSetupName, final String events, final String aggregatesAll, final String timeoutsAll, final int repeat) {
        // handler which collects all aggregate events which are republished to the buffer
        final Set<Set<String>> aggResults = ConcurrentHashMap.newKeySet();
        final Set<Long> aggTimeouts = ConcurrentHashMap.newKeySet();

        EventHandler<RingBufferEvent> testHandler = (evt, seq, eob) -> {
            LOGGER.atDebug().addArgument(evt).log("testHandler: {}");
            if (evt.payload == null) {
                throw new IllegalStateException("RingBufferEvent payload is null.");
            }

            final EvtTypeFilter evtType = evt.getFilter(EvtTypeFilter.class);
            if (evtType == null) {
                throw new IllegalStateException("RingBufferEvent has no EvtTypeFilter definition");
            }
            final TimingCtx timingCtx = evt.getFilter(TimingCtx.class);
            if (timingCtx == null) {
                throw new IllegalStateException("RingBufferEvent has no TimingCtx definition");
            }

            if (evtType.evtType == EvtTypeFilter.DataType.AGGREGATE_DATA) {
                if (!(evt.payload.get() instanceof Map)) {
                    throw new IllegalStateException("RingBufferEvent payload is not a Map: " + evt.payload.get());
                }
                @SuppressWarnings("unchecked")
                final Map<String, SharedPointer<String>> agg = (Map<String, SharedPointer<String>>) evt.payload.get();
                final Set<String> payloads = agg.values().stream().map(SharedPointer::get).collect(Collectors.toSet());
                // add aggregate events to result vector
                aggResults.add(payloads);
            }

            if (evtType.evtType == EvtTypeFilter.DataType.AGGREGATE_DATA && evtType.updateType != EvtTypeFilter.UpdateType.COMPLETE) {
                // time-out occured -- add failed aggregate event BPCTS to result vector
                aggTimeouts.add(timingCtx.bpcts);
            }
        };

        // create event ring buffer and add de-multiplexing processors
        final Disruptor<RingBufferEvent> disruptor = new Disruptor<>(() -> new RingBufferEvent(TimingCtx.class, EvtTypeFilter.class), 256, DaemonThreadFactory.INSTANCE, ProducerType.MULTI, new TimeoutBlockingWaitStrategy(200, TimeUnit.MILLISECONDS));
        final AggregateEventHandler aggProc = AggregateEventHandler.getFactory().setRingBuffer(disruptor.getRingBuffer()).setAggregateName("testAggregate").setDeviceList(DEVICES).build();
        final EventHandlerGroup<RingBufferEvent> endBarrier = disruptor.handleEventsWith(testHandler).handleEventsWith(aggProc).then(aggProc.getAggregationHander());
        RingBuffer<RingBufferEvent> rb = disruptor.start();

        // Use event source to publish demo events to the buffer.
        AggregateEventHandlerTestSource testEventSource = new AggregateEventHandlerTestSource(events, repeat, rb);
        assertDoesNotThrow(testEventSource::run);

        // wait for all events to be played and processed
        Awaitility.await().atMost(Duration.ofSeconds(repeat)).until(() -> endBarrier.asSequenceBarrier().getCursor() == rb.getCursor() && Arrays.stream(aggProc.getAggregationHander()).allMatch(w -> w.bpcts == -1));
        // compare aggregated results and timeouts
        assertFalse(aggResults.isEmpty());
        assertThat(aggResults, containsInAnyOrder(Arrays.stream(aggregatesAll.split(";"))
                                                          .filter(s -> !s.isEmpty())
                                                          .map(s -> containsInAnyOrder(Arrays.stream(s.split(" ")).map(String::trim).filter(e -> !e.isEmpty()).toArray()))
                                                          .toArray(Matcher[] ::new)));
        LOGGER.atDebug().addArgument(aggTimeouts).log("aggTimeouts: {}");
        assertThat(aggTimeouts, containsInAnyOrder(Arrays.stream(timeoutsAll.split(" ")).filter(s -> !s.isEmpty()).map(Long::parseLong).toArray(Long[] ::new)));
    }
}
