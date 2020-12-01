package de.gsi.microservice.datasource.rest;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;
import org.zeromq.ZMsg;

import de.gsi.dataset.remote.MimeType;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okhttp3.sse.EventSource;
import okhttp3.sse.EventSources;
import zmq.ZError;

class RestDataSourceTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(RestDataSourceTest.class);
    private static final String TEST_DATA = "Hello World!";
    private static final int DEFAULT_TIMEOUT_MILLIS = 1000;
    private static final int DEFAULT_WAIT_MILLIS = 10;
    private MockWebServer server;
    private OkHttpClient client;
    private EventSourceRecorder listener;

    @BeforeEach
    void before() throws IOException {
        this.server = new MockWebServer();
        server.setDispatcher(new CustomDispatcher());
        server.start();

        client = new OkHttpClient();
        listener = new EventSourceRecorder();
    }

    @AfterEach
    void after() throws IOException {
        server.close();
    }

    @Test
    void basicEvent() {
        enqueue(new MockResponse().setBody("data: hey\n\n").setHeader("content-type", "text/event-stream"));

        EventSource source = newEventSource();

        assertEquals("/", source.request().url().encodedPath());

        listener.assertOpen();
        listener.assertEvent(null, null, "hey");
        listener.assertClose();
    }

    @Test
    void basicRestDataSourceTests() {
        assertThrows(UnsupportedOperationException.class, () -> new RestDataSource(null, null));
        assertThrows(UnsupportedOperationException.class, () -> new RestDataSource(null, ""));
        assertThrows(IllegalArgumentException.class, () -> new RestDataSource(null, server.url("/sse").toString(), null, "clientName", null)); // NOSONAR
        RestDataSource dataSource = new RestDataSource(null, server.url("/sse").toString());
        assertNotNull(dataSource);
        assertNotNull(dataSource.getEndpoint());
        assertDoesNotThrow(() -> dataSource.housekeeping());
    }

    @Test
    void testRestDataSource() {
        try (final ZContext ctx = new ZContext()) {
            final RestDataSource dataSource = new RestDataSource(ctx, server.url("/sse").toString());
            assertNotNull(dataSource);

            dataSource.subscribe("1", new byte[0]);
            receiveAndCheckData(dataSource, "de.gsi.microservice.datasource.rest.RestDataSource#*", true);

            // test asynchronuous get
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(DEFAULT_WAIT_MILLIS));
            dataSource.enqueueRequest("testHashKey#1");
            final ZMsg returnMessage = receiveAndCheckData(dataSource, "testHashKey#1", true);
            assertEquals(0, returnMessage.getLast().getData().length);

            dataSource.stop();
        }
    }

    @Test
    void testRestDataSourceTimeOut() {
        try (final ZContext ctx = new ZContext()) {
            final RestDataSource dataSource = new RestDataSource(ctx, server.url("/testDelayed").toString(), Duration.ofMillis(10), "testClient", null);
            assertNotNull(dataSource);

            // test asynchronuous with time-out
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(DEFAULT_WAIT_MILLIS));
            dataSource.enqueueRequest("testHashKey#1");
            final ZMsg returnMessage = receiveAndCheckData(dataSource, "testHashKey#1", true);
            assertNotEquals(0, returnMessage.getLast().getData().length);

            dataSource.stop();
        }
    }

    @Test
    void testRestDataSourceConnectionError() {
        try (final ZContext ctx = new ZContext()) {
            final RestDataSource dataSource = new RestDataSource(ctx, server.url("/testError").toString());
            assertNotNull(dataSource);

            // three retries and a successful response
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(DEFAULT_WAIT_MILLIS));
            dataSource.cancelLastCall = 3; // required for unit-testing
            dataSource.enqueueRequest("testHashKey#1");
            ZMsg returnMessage = receiveAndCheckData(dataSource, "testHashKey#1", true);
            assertNotEquals(0, returnMessage.pollLast().getData().length);
            assertNotEquals(0, returnMessage.pollLast().getData().length);

            // four retries  without successful response
            dataSource.cancelLastCall = 4; // required for unit-testing
            dataSource.enqueueRequest("testHashKey#1");
            returnMessage = receiveAndCheckData(dataSource, "testHashKey#1", true);
            assertNotEquals(0, returnMessage.pollLast().getData().length);
            assertEquals(0, returnMessage.pollLast().getData().length);

            dataSource.stop();
        }
    }

    @Test
    @Disabled("not to be used in CI/CD environment")
    void testLsaRestDataSource() {
        try (final ZContext ctx = new ZContext()) {
            final String endPoint = "<add your favourite REST server here>?msg=HalloRaphael;mytime=" + System.currentTimeMillis();
            final RestDataSource dataSource = new RestDataSource(ctx, endPoint);
            assertNotNull(dataSource);
            dataSource.enqueueRequest("lsaHashKey#1");
            receiveAndCheckData(dataSource, "lsaHashKey#1", false);

            dataSource.stop();
        }
    }

    private void enqueue(MockResponse response) {
        final Dispatcher dispatcher = server.getDispatcher();
        if (!(dispatcher instanceof CustomDispatcher)) {
            throw new IllegalStateException("wrong dispatcher type: " + dispatcher);
        }
        CustomDispatcher customDispatcher = (CustomDispatcher) dispatcher;
        customDispatcher.enquedEvents.offer(response);
    }

    private EventSource newEventSource() {
        Request.Builder builder = new Request.Builder().url(server.url("/"));

        builder.header("Accept", "event-stream");

        Request request = builder.build();
        EventSource.Factory factory = EventSources.createFactory(client);
        return factory.newEventSource(request, listener);
    }

    private ZMsg receiveAndCheckData(final RestDataSource dataSource, final String hashKey, final boolean verbose) {
        final ZMQ.Poller poller = dataSource.getCtx().createPoller(1);
        final ZMQ.Socket socket = dataSource.getSocket();
        final int socketID = poller.register(socket, ZMQ.Poller.POLLIN);

        final int n = poller.poll(TimeUnit.MILLISECONDS.toMillis(DEFAULT_TIMEOUT_MILLIS));
        assertEquals(1, n, "external socket did not receive the expected number of message frames for hashKey = " + hashKey);
        ZMsg msg;
        if (poller.pollin(socketID)) {
            try {
                msg = dataSource.getMessage();
                if (verbose) {
                    LOGGER.atDebug().addArgument(msg).log("received reply via external socket: '{}'");
                }

                if (msg == null) {
                    throw new IllegalStateException("no data received");
                }
                final String text = msg.getFirst().toString();
                assertTrue(text.matches(hashKey.replace("?", ".?").replace("*", ".*?")), "mesage " + text + " did not match hashKey template " + hashKey);
            } catch (ZMQException e) {
                final int errorCode = socket.errno();
                LOGGER.atError().setCause(e).addArgument(errorCode).addArgument(ZError.toString(errorCode)).log("recvMsg error {} - {}");
                throw e;
            }
        } else {
            throw new IllegalStateException("no data received - pollin");
        }

        poller.close();
        return msg;
    }

    private static class CustomDispatcher extends Dispatcher {
        public BlockingQueue<MockResponse> enquedEvents = new LinkedBlockingQueue<>();
        @Override
        public MockResponse dispatch(RecordedRequest request) {
            if (!enquedEvents.isEmpty()) {
                // dispatch enqued events
                return enquedEvents.poll();
            }
            final String acceptHeader = request.getHeader("Accept");
            final String contentType = request.getHeader("content-type");
            LOGGER.atTrace().addArgument(request).addArgument(request.getPath()).addArgument(contentType).addArgument(acceptHeader) //
                    .log("server-request: {} path = {} contentType={} accept={}");

            final String path;
            try {
                path = request.getPath();
            } catch (NullPointerException e) {
                LOGGER.atError().setCause(e).log("server-request exception");
                return new MockResponse().setResponseCode(404);
            }
            switch (Objects.requireNonNull(path)) {
            case "/sse":
                if ("text/event-stream".equals(acceptHeader)) {
                    return new MockResponse().setBody("data: event-stream init\n\n").setHeader("content-type", "text/event-stream");
                }
                return new MockResponse().setBody(TEST_DATA).setHeader("content-type", MimeType.TEXT.toString());
            case "/test":
                return new MockResponse().setResponseCode(200).setBody("special test data");
            case "/testDelayed":
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1000));
                return new MockResponse().setResponseCode(200).setBody("special delayed test data");
            case "/testError":
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(2 * DEFAULT_WAIT_MILLIS));
                return new MockResponse().setResponseCode(200).setBody("special error test data");
            default:
            }
            return new MockResponse().setResponseCode(404);
        }
    }
}
