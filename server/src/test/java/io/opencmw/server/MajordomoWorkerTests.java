package io.opencmw.server;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.*;

import static io.opencmw.OpenCmwConstants.setDefaultSocketParameters;
import static io.opencmw.OpenCmwProtocol.Command.SET_REQUEST;
import static io.opencmw.OpenCmwProtocol.MdpMessage;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.Utils;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.util.ZData;

import io.opencmw.MimeType;
import io.opencmw.OpenCmwProtocol;
import io.opencmw.QueryParameterParser;
import io.opencmw.domain.BinaryData;
import io.opencmw.domain.NoData;
import io.opencmw.filter.TimingCtx;
import io.opencmw.rbac.BasicRbacRole;
import io.opencmw.serialiser.IoBuffer;
import io.opencmw.serialiser.IoClassSerialiser;
import io.opencmw.serialiser.annotations.MetaInfo;
import io.opencmw.serialiser.spi.BinarySerialiser;
import io.opencmw.serialiser.spi.ClassFieldDescription;
import io.opencmw.serialiser.spi.CmwLightSerialiser;
import io.opencmw.serialiser.spi.FastByteBuffer;
import io.opencmw.serialiser.spi.JsonSerialiser;

/**
 * Basic test for MajordomoWorker abstract class
 * @author rstein
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MajordomoWorkerTests {
    public static final String TEST_SERVICE_NAME = "basicHtml";
    private static final Logger LOGGER = LoggerFactory.getLogger(MajordomoWorkerTests.class);
    private MajordomoBroker broker;
    private TestHtmlService basicHtmlService;
    private MajordomoTestClientSync clientSession;

    @Test
    void testGetterSetter() {
        assertDoesNotThrow(() -> new MajordomoWorker<>(broker.getContext(), "testServiceName", TestContext.class, RequestDataType.class, ReplyDataType.class, BasicRbacRole.ADMIN));
        assertDoesNotThrow(() -> new MajordomoWorker<>(URI.create("mdp://localhost"), "testServiceName", TestContext.class, RequestDataType.class, ReplyDataType.class, BasicRbacRole.ADMIN));

        MajordomoWorker<TestContext, RequestDataType, ReplyDataType> internal = new MajordomoWorker<>(broker.getContext(), "testServiceName",
                TestContext.class, RequestDataType.class, ReplyDataType.class, BasicRbacRole.ADMIN);

        assertThrows(UnsupportedOperationException.class, () -> internal.registerHandler((rawCtx) -> {}));

        final MajordomoWorker.Handler<TestContext, RequestDataType, ReplyDataType> handler = (rawCtx, reqCtx, in, repCtx, out) -> {};
        assertThat(internal.getHandler(), is(not(equalTo(handler))));
        assertDoesNotThrow(() -> internal.setHandler(handler));
        assertEquals(handler, internal.getHandler(), "handler get/set identity");

        final MajordomoWorker.Handler<TestContext, RequestDataType, ReplyDataType> htmlHandler = (rawCtx, reqCtx, in, repCtx, out) -> {};
        assertThat(internal.getHtmlHandler(), is(not(equalTo(handler))));
        assertDoesNotThrow(() -> internal.setHtmlHandler(htmlHandler));
        assertEquals(htmlHandler, internal.getHtmlHandler(), "HTML-handler get/set identity");
    }

    @Test
    void simpleTest() throws IOException {
        final MajordomoBroker broker = new MajordomoBroker("TestBroker", null, BasicRbacRole.values());
        // broker.setDaemon(true); // use this if running in another app that controls threads
        final URI brokerAddress = broker.bind(URI.create("mdp://*:" + Utils.findOpenPort()));
        final URI brokerPubAddress = broker.bind(URI.create("mds://*:" + Utils.findOpenPort()));
        assertNotNull(brokerAddress);
        assertNotNull(brokerPubAddress);
        broker.start();

        RequestDataType inputData = new RequestDataType();
        inputData.name = "<Hello World! - Γειά σου Κόσμε!>";

        final String requestTopic = "mdp://myServer:5555/helloWorld?ctx=FAIR.SELECTOR.C=2&testProperty=TestValue";
        final String testServiceName = URI.create(requestTopic).getPath();

        // add external (albeit inproc) Majordomo worker to the broker
        MajordomoWorker<TestContext, RequestDataType, ReplyDataType> internal = new MajordomoWorker<>(broker.getContext(), testServiceName,
                TestContext.class, RequestDataType.class, ReplyDataType.class, BasicRbacRole.ADMIN);
        internal.setHandler((rawCtx, reqCtx, in, repCtx, out) -> {
            LOGGER.atInfo().addArgument(reqCtx).log("received reqCtx  = {}");
            LOGGER.atInfo().addArgument(in.name).log("received in.name  = {}");

            // processing data
            out.name = in.name + "-modified";
            repCtx.ctx = TimingCtx.getStatic("FAIR.SELECTOR.C=3");
            repCtx.ctx.bpcts = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());

            LOGGER.atInfo().addArgument(repCtx).log("received reqCtx  = {}");
            LOGGER.atInfo().addArgument(out.name).log("received out.name = {}");
            repCtx.contentType = MimeType.BINARY; // set default return type to OpenCMW's YaS
        });
        internal.start();

        // using simple synchronous client
        MajordomoTestClientSync clientSession = new MajordomoTestClientSync(brokerAddress, "customClientName");

        IoBuffer ioBuffer = new FastByteBuffer(4000);
        IoClassSerialiser serialiser = new IoClassSerialiser(ioBuffer);
        serialiser.serialiseObject(inputData);
        byte[] input = Arrays.copyOf(ioBuffer.elements(), ioBuffer.position());
        {
            final MdpMessage reply = clientSession.send(SET_REQUEST, requestTopic, input); // w/o RBAC
            if (!reply.errors.isBlank()) {
                LOGGER.atError().addArgument(reply).log("reply with exceptions:\n{}");
            }
            assertEquals("", reply.errors, "worker threw exception in reply");
            IoClassSerialiser deserialiser = new IoClassSerialiser(FastByteBuffer.wrap(reply.data));
            final ReplyDataType result = deserialiser.deserialiseObject(ReplyDataType.class);

            assertTrue(result.name.startsWith(inputData.name), "serialise-deserialise identity");
        }

        broker.stopBroker();
    }

    @Test
    void testSerialiserIdentity() {
        RequestDataType inputData = new RequestDataType();
        inputData.name = "<Hello World! - Γειά σου Κόσμε!>";

        IoBuffer ioBuffer = new FastByteBuffer(4000);
        IoClassSerialiser serialiser = new IoClassSerialiser(ioBuffer);
        serialiser.serialiseObject(inputData);
        ioBuffer.flip();
        byte[] reply = Arrays.copyOf(ioBuffer.elements(), ioBuffer.limit() + 4);

        IoClassSerialiser deserialiser = new IoClassSerialiser(FastByteBuffer.wrap(reply));
        final RequestDataType result = deserialiser.deserialiseObject(RequestDataType.class);
        assertTrue(result.name.startsWith(inputData.name), "serialise-deserialise identity");
    }

    @BeforeAll
    @Timeout(10)
    void startBroker() throws IOException {
        broker = new MajordomoBroker("TestBroker", null, BasicRbacRole.values());
        // broker.setDaemon(true); // use this if running in another app that controls threads
        final URI brokerAddress = broker.bind(URI.create("mdp://*:" + Utils.findOpenPort()));
        broker.start();

        basicHtmlService = new TestHtmlService(broker.getContext());
        basicHtmlService.start();
        clientSession = new MajordomoTestClientSync(brokerAddress, "customClientName");
    }

    @AfterAll
    @Timeout(10)
    void stopBroker() {
        broker.stopBroker();
    }

    @ParameterizedTest
    @ValueSource(strings = { "", "BINARY", "JSON", "CMWLIGHT", "HTML" })
    void basicEchoTest(final String contentType) {
        final BinaryData inputData = new BinaryData();
        inputData.resourceName = "test";
        inputData.contentType = MimeType.TEXT;
        inputData.data = "testBinaryData".getBytes(UTF_8);

        IoBuffer ioBuffer = new FastByteBuffer(4000, true, null);
        IoClassSerialiser serialiser = new IoClassSerialiser(ioBuffer);
        serialiser.setAutoMatchSerialiser(false);
        switch (contentType) {
        case "HTML":
        case "JSON":
            serialiser.setMatchedIoSerialiser(JsonSerialiser.class);
            serialiser.serialiseObject(inputData);
            break;
        case "CMWLIGHT":
            serialiser.setMatchedIoSerialiser(CmwLightSerialiser.class);
            serialiser.serialiseObject(inputData);
            break;
        case "":
        case "BINARY":
        default:
            serialiser.setMatchedIoSerialiser(BinarySerialiser.class);
            serialiser.serialiseObject(inputData);
            break;
        }
        ioBuffer.flip();
        byte[] requestData = Arrays.copyOf(ioBuffer.elements(), ioBuffer.limit());
        final String mimeType = contentType.isBlank() ? "" : ("?contentType=" + contentType) + ("HTML".equals(contentType) ? "&noMenu" : "");
        final OpenCmwProtocol.MdpMessage rawReply = clientSession.send(SET_REQUEST, TEST_SERVICE_NAME + mimeType, requestData);
        assertNotNull(rawReply, "rawReply not being null");
        assertEquals("", rawReply.errors, "no exception thrown");
        assertNotNull(rawReply.data, "user-data not being null");

        // test input/output equality
        switch (contentType) {
        case "HTML":
            // HTML return
            final String replyHtml = new String(rawReply.data);
            // very crude check whether required reply field ids are present - we skip detailed HTML parsing -> more efficiently done by a human and browser
            assertThat(replyHtml, containsString("id=\"resourceName\""));
            assertThat(replyHtml, containsString("id=\"contentType\""));
            assertThat(replyHtml, containsString("id=\"data\""));
            assertThat(replyHtml, containsString(inputData.resourceName));
            assertThat(replyHtml, containsString(inputData.contentType.name()));
            return;
        case "":
        case "JSON":
        case "CMWLIGHT":
        case "BINARY":
            IoClassSerialiser deserialiser = new IoClassSerialiser(FastByteBuffer.wrap(rawReply.data));
            final BinaryData reply = deserialiser.deserialiseObject(BinaryData.class);
            ioBuffer.flip();

            assertNotNull(reply);
            assertEquals(inputData.resourceName, reply.resourceName, "identity resourceName field");
            assertEquals(inputData.contentType, reply.contentType, "identity contentType field");
            assertArrayEquals(inputData.data, reply.data, "identity data field");
            return;
        default:
            throw new IllegalStateException("unimplemented contentType test: contentType=" + contentType);
        }
    }

    @Test
    void basicDefaultHtmlHandlerTest() {
        assertDoesNotThrow(() -> new DefaultHtmlHandler<>(this.getClass(), null, map -> {
            map.put("extraKey", "extraValue");
            map.put("extraUnknownObject", new NoData());
        }));
    }

    @Test
    void testGenerateQueryParameter() {
        final TestParameterClass testParam = new TestParameterClass();
        final Map<ClassFieldDescription, String> map = DefaultHtmlHandler.generateQueryParameter(testParam);
        assertNotNull(map);
    }

    @Test
    void testNotifySubscription() throws Exception {
        // start low-level subscription
        final AtomicInteger subCounter = new AtomicInteger(0);
        final AtomicBoolean run = new AtomicBoolean(true);
        final AtomicBoolean startedSubscriber = new AtomicBoolean(false);
        final Thread subscriptionThread = new Thread(() -> {
            try (ZMQ.Socket sub = broker.getContext().createSocket(SocketType.SUB)) {
                setDefaultSocketParameters(sub);
                sub.connect(MajordomoBroker.INTERNAL_ADDRESS_PUBLISHER);
                sub.subscribe(TEST_SERVICE_NAME);
                while (run.get() && !Thread.interrupted()) {
                    startedSubscriber.set(true);
                    final MdpMessage rawReply = MdpMessage.receive(sub, false);
                    if (rawReply == null) {
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                        continue;
                    }
                    IoClassSerialiser deserialiser = new IoClassSerialiser(FastByteBuffer.wrap(rawReply.data));
                    final BinaryData reply = deserialiser.deserialiseObject(BinaryData.class);
                    final Map<String, List<String>> queryMap = QueryParameterParser.getMap(rawReply.topic.getQuery());
                    final List<String> testValues = queryMap.get("testValue");
                    final int iteration = subCounter.getAndIncrement();

                    // very basic check that the correct notifications have been received in order
                    assertEquals("resourceName" + iteration, reply.resourceName, "resourceName field");
                    assertEquals(1, testValues.size(), "test query parameter number - " + testValues + " - " + rawReply.topic.getQuery());
                    assertEquals("notify" + iteration, testValues.get(0), "test query parameter name");
                }
                sub.unsubscribe(TEST_SERVICE_NAME);
            }
        });
        subscriptionThread.start();

        // wait until all services are initialised
        await().alias("wait for thread2 to start").atMost(1, TimeUnit.SECONDS).until(startedSubscriber::get, equalTo(true));

        // send bursts of 10 messages
        for (int i = 0; i < 10; i++) {
            TestContext notifyCtx = new TestContext();
            notifyCtx.testValue = "notify" + i;
            BinaryData reply = new BinaryData();
            reply.resourceName = "resourceName" + i;
            basicHtmlService.notify(notifyCtx, reply);
        }

        await().alias("wait for reply messages").atMost(2, TimeUnit.SECONDS).until(subCounter::get, equalTo(10));
        run.set(false);
        await().alias("wait for subscription thread to shut-down").atMost(1, TimeUnit.SECONDS).until(subscriptionThread::isAlive, equalTo(false));
        assertFalse(subscriptionThread.isAlive(), "subscription thread shut-down");
        assertEquals(10, subCounter.get(), "received expected number of subscription replies");
    }

    public static class TestContext {
        @MetaInfo(description = "FAIR timing context selector, e.g. FAIR.SELECTOR.C=0, ALL, ...")
        public TimingCtx ctx = TimingCtx.getStatic("FAIR.SELECTOR.ALL");
        @MetaInfo(unit = "a.u.", description = "random test parameter")
        public String testValue = "default value";
        @MetaInfo(description = "requested MIME content type, eg. 'application/binary', 'text/html','text/json', ..")
        public MimeType contentType = MimeType.UNKNOWN;

        public TestContext() {
            // needs default constructor
        }

        @Override
        public String toString() {
            return "TestContext{ctx=" + ctx + ", testValue='" + testValue + "', contentType=" + contentType.getMediaType() + '}';
        }
    }

    @MetaInfo(description = "request type class description", direction = "IN")
    public static class RequestDataType {
        @MetaInfo(description = " RequestDataType name to show up in the OpenAPI docs")
        public String name;
        public int counter;
        public byte[] payload;

        public RequestDataType() {
            // needs default constructor
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            final RequestDataType requestDataType = (RequestDataType) o;

            if (!Objects.equals(name, requestDataType.name))
                return false;
            return Arrays.equals(payload, requestDataType.payload);
        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + Arrays.hashCode(payload);
            return result;
        }

        @Override
        public String toString() {
            return "RequestDataType{inputName='" + name + "', counter=" + counter + "', payload=" + ZData.toString(payload) + '}';
        }
    }

    @MetaInfo(description = "reply type class description", direction = "OUT")
    public static class ReplyDataType {
        @MetaInfo(description = "ReplyDataType name to show up in the OpenAPI docs")
        public String name;
        @MetaInfo(description = "a return value", unit = "A", direction = "OUT", groups = { "A", "B" })
        public int returnValue;
        public ReplyDataType() {
            // needs default constructor
        }

        @Override
        public String toString() {
            return "ReplyDataType{outputName='" + name + "', returnValue=" + returnValue + '}';
        }
    }

    @MetaInfo(description = "test HTML enabled MajordomoWorker implementation")
    private static class TestHtmlService extends MajordomoWorker<MajordomoWorkerTests.TestContext, BinaryData, BinaryData> {
        private TestHtmlService(ZContext ctx) {
            super(ctx, TEST_SERVICE_NAME, MajordomoWorkerTests.TestContext.class, BinaryData.class, BinaryData.class);
            setHtmlHandler(new DefaultHtmlHandler<>(this.getClass(), null, map -> {
                map.put("extraKey", "extraValue");
                map.put("extraUnknownObject", new NoData());
            }));
            super.setHandler((rawCtx, reqCtx, request, repCtx, reply) -> {
                reply.data = request.data;
                reply.contentType = request.contentType;
                reply.resourceName = request.resourceName;
                rawCtx.htmlData = Objects.requireNonNullElse(rawCtx.htmlData, new HashMap<>());
                rawCtx.htmlData.put("extraKey2", "extraValue2");
            });
        }
    }

    static class TestParameterClass {
        public String testString = "test1";
        public Object genericObject = new Object();
        public UnknownClass ctx = new UnknownClass();
    }

    static class UnknownClass {
        public String name = "UnknownClass";
    }
}
