package io.opencmw.client;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;

import org.awaitility.Awaitility;
import org.hamcrest.Matchers;
import org.hamcrest.collection.IsMapContaining;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.Utils;

import io.opencmw.EventStore;
import io.opencmw.Filter;
import io.opencmw.MimeType;
import io.opencmw.client.DataSourcePublisher.NotificationListener;
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
    private EventStore eventStore;
    private DataSourcePublisher dataSourcePublisher;
    private int brokerPort;
    private int brokerPortMds;
    private MajordomoWorker<TestContext, TestObject, TestObject> worker;
    private MajordomoBroker broker;

    @BeforeEach
    void setupBrokerAndEventStore() throws IOException {
        broker = new MajordomoBroker("TestBroker", "", BasicRbacRole.values());
        brokerPort = Utils.findOpenPort();
        brokerPortMds = Utils.findOpenPort();
        final String brokerAddress = broker.bind("mdp://*:" + brokerPort);
        final String brokerPubAddress = broker.bind("mds://*:" + brokerPortMds);
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
        dataSourcePublisher = new DataSourcePublisher(eventStore, null, null, "testOpenCmwPublisher");
    }

    @AfterEach
    void shutdownBroker() {
        worker.stopWorker();
        broker.stopBroker();
    }

    @Test
    void testSubscriptionWithListener() {
        assertNotNull(dataSourcePublisher.getRawDataEventStore());
        final Queue<TestObject> updates = new ArrayBlockingQueue<>(20);
        eventStore.start();
        dataSourcePublisher.start();
        LockSupport.parkNanos(Duration.ofMillis(200).toNanos());

        try (final DataSourcePublisher.Client client = dataSourcePublisher.getClient()) {
            final URI requestURI = URI.create("mds://localhost:" + brokerPortMds + "/testWorker");
            LOGGER.atDebug().addArgument(requestURI).log("subscribing to endpoint: {}");
            final TestContext requestContext = new TestContext("FAIR.SELECTOR.C=2");
            final NotificationListener<TestObject, TestContext> listener = new NotificationListener<>() {
                @Override
                public void dataUpdate(final TestObject updatedObject, final TestContext contextObject) {
                    assertEquals(new TestContext("FAIR.SELECTOR.C=2"), contextObject);
                    updates.add(updatedObject);
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
            LOGGER.atDebug().addArgument(updates).addArgument("Making certain that update was not received: {}");
            LockSupport.parkNanos(Duration.ofMillis(10).toNanos());
            assertEquals(4, updates.size());
            assertThat(updates, Matchers.not(Matchers.contains(new TestObject("update", 1337))));
        }

        // stop event store
        eventStore.stop();
        dataSourcePublisher.stop();
    }

    @Test
    void testSubscription() throws URISyntaxException {
        final Map<String, TestObject> updates = new ConcurrentHashMap<>();
        eventStore.register((event, seq, last) -> {
            LOGGER.atDebug().addArgument(event).log("received event: {}");
            updates.put(event.getFilter(TestContext.class).ctx, event.payload.get(TestObject.class));
        });
        eventStore.start();
        dataSourcePublisher.start();
        LockSupport.parkNanos(Duration.ofMillis(20).toNanos());

        try (final DataSourcePublisher.Client client = dataSourcePublisher.getClient()) {
            final URI requestURI = new URI("mds", "localhost:" + brokerPortMds, "/testWorker", "ctx=FAIR.SELECTOR.C=2", null); // mdp-> mds
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
    void testGetRequest() throws URISyntaxException, InterruptedException, ExecutionException, TimeoutException {
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
        final URI requestURI = new URI("mdp", "localhost:" + brokerPort, "/testWorker", "ctx=FAIR.SELECTOR.C=3&contentType=application/octet-stream", null);
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
        final URI requestURI = URI.create("mdp://localhost:" + brokerPort + "/testWorker");
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
    void testSetRequest() throws URISyntaxException, InterruptedException, ExecutionException, TimeoutException {
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
        final URI requestURI = new URI("mdp", "localhost:" + brokerPort, "/testWorker", "ctx=FAIR.SELECTOR.C=3", null);
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

    public static class TestObject {
        private String foo;
        private double bar;

        public TestObject(final String foo, final double bar) {
            this.foo = foo;
            this.bar = bar;
        }

        public TestObject() {
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
        public int hashCode() {
            return Objects.hash(ctx, contentType);
        }
    }
}