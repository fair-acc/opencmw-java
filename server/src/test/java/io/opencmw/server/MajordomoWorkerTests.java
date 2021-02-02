package io.opencmw.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import static io.opencmw.OpenCmwProtocol.MdpMessage;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.Utils;
import org.zeromq.util.ZData;

import io.opencmw.MimeType;
import io.opencmw.filter.TimingCtx;
import io.opencmw.rbac.BasicRbacRole;
import io.opencmw.serialiser.IoBuffer;
import io.opencmw.serialiser.IoClassSerialiser;
import io.opencmw.serialiser.annotations.MetaInfo;
import io.opencmw.serialiser.spi.FastByteBuffer;

class MajordomoWorkerTests {
    private static final Logger LOGGER = LoggerFactory.getLogger(MajordomoWorkerTests.class);

    @Test
    void simpleTest() throws IOException {
        final MajordomoBroker broker = new MajordomoBroker("TestBroker", "", BasicRbacRole.values());
        // broker.setDaemon(true); // use this if running in another app that controls threads
        final String brokerAddress = broker.bind("mdp://*:" + Utils.findOpenPort());
        final String brokerPubAddress = broker.bind("mds://*:" + Utils.findOpenPort());
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
            repCtx.ctx = TimingCtx.get("FAIR.SELECTOR.C=3");
            repCtx.ctx.bpcts = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());

            LOGGER.atInfo().addArgument(repCtx).log("received reqCtx  = {}");
            LOGGER.atInfo().addArgument(out.name).log("received out.name = {}");
        });
        internal.start();

        // using simple synchronous client
        MajordomoTestClientSync clientSession = new MajordomoTestClientSync(brokerAddress, "customClientName");

        IoBuffer ioBuffer = new FastByteBuffer(4000);
        IoClassSerialiser serialiser = new IoClassSerialiser(ioBuffer);
        serialiser.serialiseObject(inputData);
        byte[] input = Arrays.copyOf(ioBuffer.elements(), ioBuffer.position() + 4); //TODO: investigate why we need 4 bytes of buffer at the end
        {
            final MdpMessage reply = clientSession.send(requestTopic, input); // w/o RBAC
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

    public static class TestContext {
        @MetaInfo(description = "FAIR timing context selector, e.g. FAIR.SELECTOR.C=0, ALL, ...")
        public TimingCtx ctx = TimingCtx.get("FAIR.SELECTOR.ALL");
        @MetaInfo(unit = "a.u.", description = "random test parameter")
        public String testValue = "default value";
        @MetaInfo(description = "requested MIME content type, eg. 'application/binary', 'text/html','text/json', ..")
        public MimeType contentType = MimeType.BINARY;

        public TestContext() {
            // needs default constructor
        }

        @Override
        public String toString() {
            return "TestContext{ctx=" + ctx + ", testValue='" + testValue + "', contentType=" + contentType + '}';
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
        public String toString() {
            return "RequestDataType{inputName='" + name + "', counter=" + counter + "', payload=" + ZData.toString(payload) + '}';
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
}
