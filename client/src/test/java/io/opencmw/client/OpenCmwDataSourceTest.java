package io.opencmw.client;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import static io.opencmw.OpenCmwProtocol.EMPTY_FRAME;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;

import org.awaitility.Awaitility;
import org.hamcrest.Matchers;
import org.hamcrest.collection.IsMapContaining;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.function.ThrowingSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.Utils;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import io.opencmw.EventStore;
import io.opencmw.Filter;
import io.opencmw.MimeType;
import io.opencmw.client.DataSourcePublisher.NotificationListener;
import io.opencmw.client.cmwlight.CmwLightProtocol;
import io.opencmw.filter.EvtTypeFilter;
import io.opencmw.rbac.BasicRbacRole;
import io.opencmw.server.MajordomoBroker;
import io.opencmw.server.MajordomoWorker;

/**
 * @author Alexander Krimm
 */
@Timeout(20)
class OpenCmwDataSourceTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenCmwDataSourceTest.class);
    private static final String TEST_EXCEPTION_MSG = "test exception message";
    private EventStore eventStore;
    private DataSourcePublisher dataSourcePublisher;
    private MajordomoWorker<TestContext, TestObject, TestObject> worker;
    private MajordomoBroker broker;
    private URI brokerAddress;
    private URI brokerPubAddress;

    @BeforeEach
    void setupBrokerAndEventStore() throws IOException {
        broker = new MajordomoBroker("TestBroker", null, BasicRbacRole.values());
        brokerAddress = broker.bind(URI.create("mdp://*:" + Utils.findOpenPort()));
        brokerPubAddress = broker.bind(URI.create("mds://*:" + Utils.findOpenPort()));
        LOGGER.atDebug().addArgument(brokerAddress).addArgument(brokerPubAddress).log("started broker on {} and {}");

        worker = new MajordomoWorker<>(
                broker.getContext(),
                "/testWorker",
                TestContext.class,
                TestObject.class,
                TestObject.class);
        worker.start();
        broker.start();

        eventStore = EventStore.getFactory().setFilterConfig(TestContext.class, EvtTypeFilter.class).build();
        dataSourcePublisher = new DataSourcePublisher(null, eventStore, null, null, "testOpenCmwPublisher");
    }

    @AfterEach
    void shutdownBroker() {
        broker.stopBroker();
    }

    @Test
    void testSubscriptionWithListener() throws Exception {
        assertNotNull(dataSourcePublisher.getRawDataEventStore());
        final Queue<TestObject> updates = new ArrayBlockingQueue<>(20);
        eventStore.start();
        dataSourcePublisher.start();
        LockSupport.parkNanos(Duration.ofMillis(200).toNanos());

        try (final DataSourcePublisher.Client client = dataSourcePublisher.getClient()) {
            final URI requestURI = URI.create(brokerPubAddress + "/testWorker");
            LOGGER.atDebug().addArgument(requestURI).log("subscribing to endpoint: {}");
            final TestContext requestContext = new TestContext("FAIR.SELECTOR.C=2");
            final NotificationListener<TestObject, TestContext> listener = new NotificationListener<>() {
                @Override
                public void dataUpdate(final TestObject updatedObject, final TestContext contextObject) {
                    assertEquals(new TestContext("FAIR.SELECTOR.C=2"), contextObject);
                    updates.add(updatedObject);
                    System.err.println("update");
                }

                @Override
                public void updateException(final Throwable exception) {
                    fail("Unexpected exception notification", exception);
                }
            };
            final String reqId = client.subscribe(requestURI, TestObject.class, requestContext, TestContext.class, listener);

            LockSupport.parkNanos(Duration.ofMillis(10).toNanos());

            // notify property
            for (int i = 1; i < 5; i++) {
                final TestContext context = new TestContext("FAIR.SELECTOR.C=2");
                final TestObject obj = new TestObject("update", i);
                LOGGER.atDebug().addArgument(context).addArgument(obj).log("notifying: {} -> {}");
                worker.notify(context, obj);
            }

            // check if all notifications were received
            LOGGER.atDebug().log("Waiting for subscription updates to be received");
            Awaitility.await().atMost(Duration.ofSeconds(3)).until(() -> updates.size() >= 4);
            assertThat(updates, Matchers.containsInAnyOrder(
                                        new TestObject("update", 1),
                                        new TestObject("update", 2),
                                        new TestObject("update", 3),
                                        new TestObject("update", 4)));
            LOGGER.atDebug().log("Subscription updates complete");

            LOGGER.atDebug().addArgument(reqId).log("Unsubscribing: {}");
            client.unsubscribe(reqId);
            LockSupport.parkNanos(Duration.ofMillis(10).toNanos());
            LOGGER.atDebug().log("Sending notification update");
            worker.notify(new TestContext("FAIR.SELECTOR.C=2"), new TestObject("update", 1337));
            LockSupport.parkNanos(Duration.ofMillis(10).toNanos());
            LOGGER.atDebug().log("Sending notification update");
            worker.notify(new TestContext("FAIR.SELECTOR.C=2"), new TestObject("update", 1337));
            LockSupport.parkNanos(Duration.ofMillis(10).toNanos());
            LOGGER.atDebug().addArgument(updates).log("Making certain that update was not received: {}");
            LockSupport.parkNanos(Duration.ofMillis(10).toNanos());
            assertEquals(4, updates.size());
            assertThat(updates, Matchers.not(Matchers.contains(new TestObject("update", 1337))));
        }

        // stop event store
        eventStore.stop();
        dataSourcePublisher.stop();
    }

    @Test
    void testTopicNormalisation() throws Exception {
        assertEquals("/asdf/test#", OpenCmwDataSource.getZMQTopicFromURI(new URI("/asdf/test")));
        assertEquals("/test/abc?answer=42&foo=bar#", OpenCmwDataSource.getZMQTopicFromURI(new URI("https://example.com/test/abc?foo=bar&answer=42")));
        assertEquals("/asdf/test?alt=300%25&type=a%2Fb#", OpenCmwDataSource.getZMQTopicFromURI(new URI("/asdf/test?type=a/b&alt=300%25")));
    }

    @Test
    void testSubscription() throws Exception {
        final Map<String, TestObject> updates = new ConcurrentHashMap<>();
        eventStore.register((event, seq, last) -> {
            LOGGER.atDebug().addArgument(event).log("received event: {}");
            updates.put(event.getFilter(TestContext.class).ctx, event.payload.get(TestObject.class));
        });
        eventStore.start();
        dataSourcePublisher.start();
        LockSupport.parkNanos(Duration.ofMillis(20).toNanos());

        try (final DataSourcePublisher.Client client = dataSourcePublisher.getClient()) {
            final URI requestURI = URI.create(brokerPubAddress + "/testWorker?ctx=FAIR.SELECTOR.C=2"); // mdp-> mds
            LOGGER.atDebug().addArgument(requestURI).log("subscribing to endpoint: {}");
            final String reqId = client.subscribe(requestURI, TestObject.class);
            LockSupport.parkNanos(Duration.ofMillis(10).toNanos());

            // notify property
            for (int i = 1; i < 5; i++) {
                final TestContext context = new TestContext("FAIR.SELECTOR.C=2:P=" + i);
                final TestObject obj = new TestObject("update", i);
                LOGGER.atDebug().addArgument(context).addArgument(obj).log("notifying: {} -> {}");
                worker.notify(context, obj);
            }

            // check if all notifications where received
            LOGGER.atDebug().log("Waiting for subscription updates to be received");
            Awaitility.await().atMost(Duration.ofSeconds(3)).until(() -> updates.size() >= 4);
            for (int i = 1; i < 5; i++) {
                assertEquals(new TestObject("update", i), updates.get("FAIR.SELECTOR.C=2:P=" + i));
            }
            LOGGER.atDebug().log("Subscription updates complete");

            // todo: unsubscribe and assert that there are no new updates
            LOGGER.atDebug().addArgument(reqId).log("Unsubscribing: {}");
            client.unsubscribe(reqId);
            LockSupport.parkNanos(Duration.ofMillis(10).toNanos());
            LOGGER.atDebug().log("Sending notification update");
            worker.notify(new TestContext("FAIR.SELECTOR.C=2:P=" + 1337), new TestObject("update", 1337));
            LockSupport.parkNanos(Duration.ofMillis(10).toNanos());
            LOGGER.atDebug().log("Sending notification update");
            worker.notify(new TestContext("FAIR.SELECTOR.C=2:P=" + 1337), new TestObject("update", 1337));
            LockSupport.parkNanos(Duration.ofMillis(10).toNanos());
            LOGGER.atDebug().addArgument(updates).addArgument("Making certain that update was not received: {}");
            LockSupport.parkNanos(Duration.ofMillis(10).toNanos());
            assertEquals(4, updates.size());
            assertThat(updates, Matchers.not(IsMapContaining.hasEntry(new TestContext("FAIR.SELECTOR.C=2:P=" + 1337), new TestObject("update", 1337))));
        }

        // stop event store
        eventStore.stop();
        dataSourcePublisher.stop();
    }

    @Test
    void testGetRequestException() {
        worker.setHandler((ctx, requestCtx, request, replyCtx, reply) -> {
            throw new CmwLightProtocol.RdaLightException(TEST_EXCEPTION_MSG);
        });

        eventStore.start();
        dataSourcePublisher.start();
        LockSupport.parkNanos(Duration.ofMillis(200).toNanos());

        // get request
        final URI requestURI = URI.create(brokerAddress + "/testWorker?ctx=FAIR.SELECTOR.C=3&contentType=application/octet-stream");
        LOGGER.atDebug().addArgument(requestURI).log("requesting GET from endpoint: {}");
        final Future<TestObject> future;
        try (final DataSourcePublisher.Client client = dataSourcePublisher.getClient()) {
            future = client.get(requestURI, null, TestObject.class); // uri_without_query oder serviceName + resolver, requestContext, type
        }

        // assert result
        final Exception exception = assertThrows(Exception.class, () -> assertNull(future.get(1000, TimeUnit.MILLISECONDS)));
        assertThat(exception.getMessage(), Matchers.containsString(TEST_EXCEPTION_MSG));

        // eventStore.stop();
        dataSourcePublisher.stop();
    }

    @Test
    void testGetRequest() throws InterruptedException, ExecutionException, TimeoutException {
        final TestObject referenceObject = new TestObject("asdf", 42);
        worker.setHandler((ctx, requestCtx, request, replyCtx, reply) -> {
            assertEquals("FAIR.SELECTOR.C=3", requestCtx.ctx);
            replyCtx.ctx = "FAIR.SELECTOR.C=3:P=5";
            reply.set(referenceObject);
            LOGGER.atDebug().addArgument(requestCtx).addArgument(request).addArgument(replyCtx).addArgument(reply).log("got get request: {}, {} -> {}, {}");
        });

        eventStore.start();
        dataSourcePublisher.start();
        LockSupport.parkNanos(Duration.ofMillis(200).toNanos());

        // get request
        final URI requestURI = URI.create(brokerAddress + "/testWorker?ctx=FAIR.SELECTOR.C=3&contentType=application/octet-stream");
        LOGGER.atDebug().addArgument(requestURI).log("requesting GET from endpoint: {}");
        final Future<TestObject> future;
        try (final DataSourcePublisher.Client client = dataSourcePublisher.getClient()) {
            future = client.get(requestURI, null, TestObject.class); // uri_without_query oder serviceName + resolver, requestContext, type
        }

        // assert result
        final TestObject result = future.get(1000, TimeUnit.MILLISECONDS);
        assertEquals(referenceObject, result);

        eventStore.stop();
        dataSourcePublisher.stop();
    }
    @Test
    void testGetRequestWithContext() throws InterruptedException, ExecutionException, TimeoutException {
        final TestObject referenceObject = new TestObject("asdf", 42);
        worker.setHandler((ctx, requestCtx, request, replyCtx, reply) -> {
            assertEquals("FAIR.SELECTOR.C=3", requestCtx.ctx);
            replyCtx.ctx = "FAIR.SELECTOR.C=3:P=5";
            reply.set(referenceObject);
            LOGGER.atDebug().addArgument(requestCtx).addArgument(request).addArgument(replyCtx).addArgument(reply).log("got get request: {}, {} -> {}, {}");
        });

        eventStore.start();
        dataSourcePublisher.start();
        LockSupport.parkNanos(Duration.ofMillis(200).toNanos());

        // get request
        final URI requestURI = URI.create(brokerAddress + "/testWorker");
        final TestContext requestContext = new TestContext("FAIR.SELECTOR.C=3", MimeType.BINARY);
        LOGGER.atDebug().addArgument(requestURI).addArgument(requestContext).log("requesting GET from endpoint: {} with context: {}");
        final Future<TestObject> future;
        try (final DataSourcePublisher.Client client = dataSourcePublisher.getClient()) {
            future = client.get(requestURI, requestContext, TestObject.class);
        }

        // assert result
        final TestObject result = future.get(1000, TimeUnit.MILLISECONDS);
        assertEquals(referenceObject, result);

        eventStore.stop();
        dataSourcePublisher.stop();
    }

    @Test
    void testSetRequest() throws InterruptedException, ExecutionException, TimeoutException {
        final TestObject toSet = new TestObject("newValue", 666);
        worker.setHandler((ctx, requestCtx, request, replyCtx, reply) -> {
            assertEquals("FAIR.SELECTOR.C=3", requestCtx.ctx);
            replyCtx.ctx = "FAIR.SELECTOR.C=3:P=5";
            reply.set(request);
            LOGGER.atDebug().addArgument(requestCtx).addArgument(request).addArgument(replyCtx).addArgument(reply).log("got get request: {}, {} -> {}, {}");
        });

        eventStore.start();
        dataSourcePublisher.start();
        LockSupport.parkNanos(Duration.ofMillis(200).toNanos());

        // set request
        final URI requestURI = URI.create(brokerAddress + "/testWorker?ctx=FAIR.SELECTOR.C=3");
        LOGGER.atDebug().addArgument(requestURI).log("requesting GET from endpoint: {}");
        final Future<TestObject> future;
        try (final DataSourcePublisher.Client client = dataSourcePublisher.getClient()) {
            future = client.set(requestURI, toSet, null, TestObject.class);
        }

        // assert result
        final TestObject result = future.get(1000, TimeUnit.MILLISECONDS);
        assertEquals(toSet, result);

        eventStore.stop();
        dataSourcePublisher.stop();
    }

    @Test
    void testMiscellaneous() {
        // test TestObject constructor
        assertDoesNotThrow((ThrowingSupplier<TestObject>) TestObject::new);
        // test createInternalMsg
        final ZMsg msg1 = assertDoesNotThrow(() -> OpenCmwDataSource.createInternalMsg(new byte[] { 1, 2, 3 }, URI.create("test"), new ZFrame(new byte[] { 42, 23 }), null, OpenCmwDataSource.class));
        assertArrayEquals(new byte[] { 1, 2, 3 }, msg1.pop().getData());
        assertEquals("test", msg1.pop().getString(ZMQ.CHARSET));
        assertArrayEquals(new byte[] { 42, 23 }, msg1.pop().getData());
        assertEquals("", msg1.pop().getString(ZMQ.CHARSET));
        final ZMsg msg2 = assertDoesNotThrow(() -> OpenCmwDataSource.createInternalMsg(new byte[] { 1, 2, 3 }, URI.create("test"), null, "testexception", OpenCmwDataSource.class));
        assertArrayEquals(new byte[] { 1, 2, 3 }, msg2.pop().getData());
        assertEquals("test", msg2.pop().getString(ZMQ.CHARSET));
        assertArrayEquals(EMPTY_FRAME, msg2.pop().getData());
        assertThat(msg2.pop().getString(ZMQ.CHARSET), Matchers.containsString("testexception"));
    }

    @Test
    @SuppressWarnings({ "EmptyTryBlock" })
    void testConstructor() {
        final ExecutorService executors = Executors.newCachedThreadPool();
        final String clientID = "test-client";
        try (ZContext ctx = new ZContext()) {
            assertDoesNotThrow(() -> { // connect to non resolvable device -> source will retry to connect
                try (DataSource source = new OpenCmwDataSource(ctx, URI.create("mdp:/device/property"), Duration.ofMillis(100), executors, clientID)) {
                    assertNotNull(source.toString());
                }
            });
            assertThrows(NullPointerException.class, () -> {
                try (DataSource source = new OpenCmwDataSource(ctx, URI.create(""), Duration.ofMillis(100), executors, clientID)) {
                }
            });
            assertThrows(UnsupportedOperationException.class, () -> {
                try (DataSource source = new OpenCmwDataSource(ctx, URI.create("mdr:/device/property"), Duration.ofMillis(100), executors, clientID)) {
                }
            });
            assertThrows(UnsupportedOperationException.class, () -> {
                try (DataSource source = new OpenCmwDataSource(ctx, URI.create("xyz:/device/property"), Duration.ofMillis(100), executors, clientID)) {
                }
            });
        }
        ZContext ctx = new ZContext();
        ctx.close();
        assertDoesNotThrow(() -> {
            try (DataSource source = new OpenCmwDataSource(ctx, URI.create("mdp:/device/property"), Duration.ofMillis(100), executors, clientID)) {
            }
        });
    }

    public static class TestObject {
        private String foo;
        private double bar;

        public TestObject(final String foo, final double bar) {
            this.foo = foo;
            this.bar = bar;
        }

        public TestObject() {
            // needed and access via reflection
            this.foo = "";
            this.bar = Double.NaN;
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
            return "{foo='" + foo + '\'' + ", bar=" + bar + '}';
        }

        public void set(final TestObject other) {
            this.foo = other.foo;
            this.bar = other.bar;
        }
    }

    public static class TestContext implements Filter {
        public String ctx;
        public MimeType contentType = MimeType.BINARY;

        public TestContext() {
            ctx = "";
        }

        public TestContext(final String context, final MimeType contentType) {
            this.ctx = context;
            this.contentType = contentType;
        }

        public TestContext(final String context) {
            this.ctx = context;
        }

        @Override
        public String toString() {
            return ctx;
        }

        @Override
        public void clear() {
            ctx = "";
            contentType = MimeType.BINARY;
        }

        @Override
        public void copyTo(final Filter other) {
            if (other instanceof TestContext) {
                ((TestContext) other).ctx = ctx;
                ((TestContext) other).contentType = contentType;
            }
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            final TestContext that = (TestContext) o;
            return Objects.equals(ctx, that.ctx) && contentType == that.contentType;
        }

        @Override
        public String getKey() {
            return "testContext";
        }

        @Override
        public String getValue() {
            return "";
        }

        @Override
        public Filter get(final String ctxString) {
            return new TestContext();
        }

        @Override
        public boolean matches(final Filter other) {
            return equals(other);
        }

        @Override
        public int hashCode() {
            return Objects.hash(ctx, contentType);
        }
    }
}