package de.gsi.microservice.datasource;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import de.gsi.microservice.filter.DataSourceFilter;
import de.gsi.serializer.IoSerialiser;
import de.gsi.serializer.spi.BinarySerialiser;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import de.gsi.microservice.EventStore;
import de.gsi.microservice.filter.EvtTypeFilter;
import de.gsi.microservice.filter.TimingCtx;
import de.gsi.serializer.IoClassSerialiser;
import de.gsi.serializer.spi.FastByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

class DataSourceProducerTest {
    private static final AtomicReference<TestObject> testObject = new AtomicReference<>();
    private final Integer requestBody = 10;
    private final Map<String, Object> requestFilter = new HashMap<>();

    private DataSourcePublisher.ThePromisedFuture<Float> getTestFuture() {
        return new DataSourcePublisher.ThePromisedFuture<>("endPoint", requestFilter, requestBody, Float.class, DataSourceFilter.ReplyType.GET, "TestClientID");
    }

    private static class TestDataSource extends DataSource {
        public static final Factory FACTORY = new Factory() {
            @Override
            public boolean matches(final String endpoint) {
                return endpoint.startsWith("test://");
            }

            @Override
            public Class<? extends IoSerialiser> getMatchingSerialiserType(final String endpoint) {
                return BinarySerialiser.class;
            }

            @Override
            public DataSource newInstance(final ZContext context, final String endpoint, final Duration timeout, final String clientId, final byte[] filters) {
                return new TestDataSource(context, endpoint, timeout, clientId, filters);
            }
        };
        private final static String INPROC = "inproc://testDataSource";
        private final ZContext context;
        private final ZMQ.Socket socket;
        private ZMQ.Socket internalSocket;
        private String endpoint;
        private String subscriptionId = "";
        private long nextHousekeeping = 0;
        private final IoClassSerialiser ioClassSerialiser = new IoClassSerialiser(new FastByteBuffer(2000));
        private String requestId = "";
        private boolean subscribed = false;

        public TestDataSource(final ZContext context, final String endpoint, final Duration timeOut, final String clientId, final byte[] filters) {
            super(context, endpoint, timeOut, clientId, filters);
            this.endpoint = endpoint;
            this.context = context;
            this.socket = context.createSocket(SocketType.DEALER);
            this.socket.bind(INPROC);
        }

        @Override
        public long housekeeping() {
            final long currentTime = System.currentTimeMillis();
            if (currentTime > nextHousekeeping) {
                if (subscribed && !subscriptionId.isEmpty()) { // notify a single update if a subscription was added
                    if (internalSocket == null) {
                        internalSocket = context.createSocket(SocketType.DEALER);
                        internalSocket.connect(INPROC);
                    }
                    final ZMsg msg = new ZMsg();
                    msg.add(subscriptionId);
                    msg.add(endpoint);
                    ioClassSerialiser.getDataBuffer().reset();
                    ioClassSerialiser.serialiseObject(testObject.get());
                    // todo: the serialiser does not report the correct position after serialisation
                    msg.add(Arrays.copyOfRange(ioClassSerialiser.getDataBuffer().elements(), 0, ioClassSerialiser.getDataBuffer().position() + 100));
                    msg.send(internalSocket);
                }
                if (!requestId.isEmpty() && testObject.get() != null) { // answer to get requests
                    if (internalSocket == null) {
                        internalSocket = context.createSocket(SocketType.DEALER);
                        internalSocket.connect(INPROC);
                    }
                    final ZMsg msg = new ZMsg();
                    msg.add(requestId);
                    msg.add(endpoint);
                    ioClassSerialiser.getDataBuffer().reset();
                    ioClassSerialiser.serialiseObject(testObject.get());
                    // todo: the serialiser does not report the correct position after serialisation
                    msg.add(Arrays.copyOfRange(ioClassSerialiser.getDataBuffer().elements(), 0, ioClassSerialiser.getDataBuffer().position() + 100));
                    msg.send(internalSocket);
                }
                nextHousekeeping = currentTime + 200;
            }
            return nextHousekeeping;
        }

        @Override
        public void get(final String requestId, final String endpoint, final byte[] filters, final byte[] data, final byte[] rbacToken) {
            this.requestId = requestId;
            this.endpoint = endpoint;
        }

        @Override
        public void set(final String requestId, final String endpoint, final byte[] filters, final byte[] data, final byte[] rbacToken) {
            throw new UnsupportedOperationException("cannot perform set");
        }

        @Override
        public String getEndpoint() {
            return endpoint;
        }

        @Override
        public ZMQ.Socket getSocket() {
            return socket;
        }

        @Override
        protected Factory getFactory() {
            return FACTORY;
        }

        @Override
        public ZMsg getMessage() {
            return ZMsg.recvMsg(socket);
        }

        @Override
        public void subscribe(final String reqId, final byte[] rbacToken) {
            subscriptionId = reqId;
            subscribed = true;
        }

        @Override
        public void unsubscribe() {
            subscribed = false;
        }
    }

    public static class TestObject {
        private final String foo;
        private final double bar;

        public TestObject(final String foo, final double bar) {
            this.foo = foo;
            this.bar = bar;
        }

        public TestObject() {
            this.foo = "";
            this.bar = 0.0;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o)
                return true;
            if (!(o instanceof TestObject))
                return false;
            final TestObject that = (TestObject) o;
            return bar == that.bar && Objects.equals(foo, that.foo);
        }

        @Override
        public int hashCode() {
            return Objects.hash(foo, bar);
        }

        @Override
        public String
        toString() {
            return "TestObject{"
                    + "foo='" + foo + '\'' + ", bar=" + bar + '}';
        }
    }

    @Test
    void testSubscribe() {
        final AtomicBoolean eventReceived = new AtomicBoolean(false);
        final TestObject referenceObject = new TestObject("foo", 1.337);
        testObject.set(referenceObject);

        DataSource.register(TestDataSource.FACTORY);

        final EventStore eventStore = EventStore.getFactory().setFilterConfig(TimingCtx.class, EvtTypeFilter.class).build();
        eventStore.register((event, sequence, endOfBatch) -> {
            assertEquals(testObject.get(), event.payload.get(TestObject.class));
            eventReceived.set(true);
        });

        final DataSourcePublisher dataSourcePublisher = new DataSourcePublisher(eventStore);

        eventStore.start();
        new Thread(dataSourcePublisher).start();

        dataSourcePublisher.subscribe("test://foobar/testdev/prop?ctx=testselector&filter=foobar", TestObject.class);

        Awaitility.waitAtMost(Duration.ofSeconds(1)).until(eventReceived::get);
    }

    @Test
    void testGet() throws InterruptedException, ExecutionException, TimeoutException {
        final TestObject referenceObject = new TestObject("foo", 1.337);
        testObject.set(referenceObject);

        DataSource.register(TestDataSource.FACTORY);

        final EventStore eventStore = EventStore.getFactory().setFilterConfig(TimingCtx.class, EvtTypeFilter.class).build();

        final DataSourcePublisher dataSourcePublisher = new DataSourcePublisher(eventStore);

        eventStore.start();
        new Thread(dataSourcePublisher).start();

        final Future<TestObject> future = dataSourcePublisher.get("test://foobar/testdev/prop?ctx=testselector&filter=foobar", TestObject.class);

        final TestObject result = future.get(1000, TimeUnit.MILLISECONDS);
        assertEquals(referenceObject, result);
    }

    @Test
    void testGetTimeout() {
        testObject.set(null); // makes the test event source not answer the get request

        DataSource.register(TestDataSource.FACTORY);

        final EventStore eventStore = EventStore.getFactory().setFilterConfig(TimingCtx.class, EvtTypeFilter.class).build();

        final DataSourcePublisher dataSourcePublisher = new DataSourcePublisher(eventStore);

        eventStore.start();
        new Thread(dataSourcePublisher).start();

        final Future<TestObject> future = dataSourcePublisher.get("test://foobar/testdev/prop?ctx=testselector&filter=foobar", TestObject.class);

        assertThrows(TimeoutException.class, () -> future.get(100, TimeUnit.MILLISECONDS));
    }

    @Test
    void testFuture() throws InterruptedException {
        final Float replyObject = (float) Math.PI;

        {
            final DataSourcePublisher.ThePromisedFuture<Float> future = getTestFuture();

            assertNotNull(future);
            assertEquals("endPoint", future.getEndpoint());
            assertEquals(requestFilter, future.getRequestFilter());
            assertEquals(requestBody, future.getRequestBody());
            assertEquals(Float.class, future.getRequestedDomainObjType());
            assertEquals(DataSourceFilter.ReplyType.GET, future.getReplyType());
            assertEquals("TestClientID", future.getInternalRequestID());

            future.setReply(replyObject);
            assertEquals(replyObject, future.get());
            assertFalse(future.cancel(true));
        }

        {
            // delayed reply
            final DataSourcePublisher.ThePromisedFuture<Float> future = getTestFuture();
            new Timer().schedule(new TimerTask() {
                @Override
                public void run() {
                    future.setReply(replyObject);
                }
            }, 300);
            Awaitility.waitAtMost(Duration.ofSeconds(1)).until(() -> replyObject.equals(future.get()));
        }

        {
            // cancelled reply
            final DataSourcePublisher.ThePromisedFuture<Float> future = getTestFuture();
            assertFalse(future.isCancelled());
            future.cancel(true);
            assertTrue(future.isCancelled());
            Awaitility.waitAtMost(Duration.ofSeconds(1)).until(() -> !replyObject.equals(future.get()));
        }
    }
}
