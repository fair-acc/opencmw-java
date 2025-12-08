package io.opencmw.client;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.*;

import io.opencmw.EventStore;
import io.opencmw.Filter;
import io.opencmw.MimeType;
import io.opencmw.client.DataSourcePublisher.NotificationListener;
import io.opencmw.filter.EvtTypeFilter;
import io.opencmw.serialiser.IoClassSerialiser;
import io.opencmw.serialiser.spi.BinarySerialiser;
import io.opencmw.serialiser.spi.FastByteBuffer;

/**
 * @author Alexander Krimm
 */
@Timeout(20)
class OpenCmwCppInteropTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenCmwCppInteropTest.class);
    private EventStore eventStore;
    private DataSourcePublisher dataSourcePublisher;
    private final URI brokerAddress = URI.create("mdp://localhost:12345");
    private final URI brokerPubAddress = URI.create("mds://localhost:12346");

    @BeforeEach
    void setupBrokerAndEventStore() throws IOException {
        eventStore = EventStore.getFactory().setFilterConfig(EvtTypeFilter.class).build();
        dataSourcePublisher = new DataSourcePublisher(null, eventStore, null, null, "testOpenCmwPublisher");
    }

    @Test
    void testSerialiserCompatibility() throws Exception {
        Assumptions.assumeTrue(new File("../CompatibilityTest").exists()); // skip if the test binary is not available
        int value = 42;
        int arraySize = 3;
        Process cppOpencmw = new ProcessBuilder("../CompatibilityTest", "serialise", "--value", value + "", "--array-size", arraySize + "").start();

        final TestData testData = new TestData();
        testData.booleanValue = value % 2 == 0;
        testData.int8Value = (byte) value;
        testData.int16Value = (short) value;
        testData.int32Value = value;
        testData.int64Value = value;
        testData.floatValue = value;
        testData.doubleValue = value;
        testData.charValue = (char) value;
        testData.stringValue = "test 42";
        testData.boolArray = new boolean[] { true, false, true };
        testData.int8Array = new byte[] { 42, 43, 44 };
        testData.int16Array = new short[] { 42, 43, 44 };
        testData.int32Array = new int[] { 42, 43, 44 };
        testData.int64Array = new long[] { 42L, 43L, 44L };
        testData.floatArray = new float[] { 42.0f, 43.0f, 44.0f };
        testData.doubleArray = new double[] { 42.0, 43.0, 44.0 };
        testData.charArray = new char[] { (char) 42, (char) 43, (char) 44 };
        testData.stringArray = new String[] { "test42", "test43", "test44" };

        final FastByteBuffer buffer = new FastByteBuffer(10000);
        // buffer.setAutoResize(true);
        IoClassSerialiser serialiser = new IoClassSerialiser(buffer, BinarySerialiser.class);
        buffer.reset(); // '0' writing at start of buffer
        serialiser.serialiseObject(testData);
        Files.write(new File("java.hex").toPath(), Arrays.copyOfRange(buffer.elements(), 0, buffer.position()));

        cppOpencmw.waitFor();
        byte[] opencmwCppString = cppOpencmw.getInputStream().readAllBytes();
        Files.write(new File("cpp.hex").toPath(), opencmwCppString);

        // this assertion does not work, because the name of the Object is different between java and cpp, so all indices are shifted by the difference in object name length
        // assertEquals(opencmwCppString, opencmwJavaString);

        final FastByteBuffer cppSerialisedBuffer = FastByteBuffer.wrap(opencmwCppString);
        cppSerialisedBuffer.reset();
        IoClassSerialiser cppSerialisedDeserialiser = new IoClassSerialiser(cppSerialisedBuffer, BinarySerialiser.class);
        TestData cppDeserialisedObject = cppSerialisedDeserialiser.deserialiseObject(TestData.class);
        assertEquals(testData, cppDeserialisedObject);
    }

    @Test
    void testSubscriptionWithListener() throws Exception {
        Assumptions.assumeTrue(new File("../CompatibilityTest").exists()); // skip if the test binary is not available
        int port = brokerAddress.getPort();
        int portMds = brokerPubAddress.getPort();
        Process cppOpencmw = new ProcessBuilder("../CompatibilityTest", "serve", "--port", port + "", "--port-mds", portMds + "").inheritIO().start();
        assertNotNull(dataSourcePublisher.getRawDataEventStore());
        final Queue<TestData> updates = new ArrayBlockingQueue<>(20);
        eventStore.register((event, seq, last) -> {
            if (!event.throwables.isEmpty()) {
                System.err.println("errors");
                event.throwables.forEach((Throwable e) -> System.err.println(e.getMessage()));
            } else {
                if (event.payload.getType() == TestData.class) {
                    System.out.println("got update with payload: doubleValue = " + event.payload.get(TestData.class).doubleValue);
                } else {
                    System.err.println("unexpected payload type" + event.payload.getType().getSimpleName());
                }
            }
        });

        eventStore.start();
        dataSourcePublisher.start();
        LockSupport.parkNanos(Duration.ofMillis(200).toNanos());

        try (final DataSourcePublisher.Client client = dataSourcePublisher.getClient()) {
            final URI requestURI = URI.create(brokerPubAddress + "/testProperty");
            final TestContext requestContext = new TestContext();
            // requestContext.contextA = "P1";
            // requestContext.contextB = "T2";
            requestContext.contentType = MimeType.BINARY;
            final NotificationListener<TestData, TestContext> listener = new NotificationListener<>() {
                @Override
                public void dataUpdate(final TestData updatedObject, final TestContext contextObject) {
                    // assertEquals(new TestContext(), contextObject);
                    updates.add(updatedObject);
                    System.err.println("update");
                }

                @Override
                public void updateException(final Throwable exception) {
                    fail("Unexpected exception notification", exception);
                }
            };
            LOGGER.atInfo().addArgument(requestURI).log("subscribing to endpoint: {}");
            final String reqId = client.subscribe(requestURI, TestData.class, requestContext, TestContext.class, listener);

            LockSupport.parkNanos(Duration.ofMillis(10).toNanos());

            // check if all notifications were received
            LOGGER.atDebug().log("Waiting for subscription updates to be received");
            Awaitility.await().atMost(Duration.ofSeconds(15)).until(() -> updates.size() >= 4);
            LOGGER.atDebug().log("Subscription updates complete");

            LOGGER.atDebug().addArgument(reqId).log("Unsubscribing: {}");
            client.unsubscribe(reqId);
        }

        // stop event store
        eventStore.stop();
        dataSourcePublisher.stop();
        cppOpencmw.destroy();
    }

    @Test
    void testGetRequest() throws InterruptedException, ExecutionException, TimeoutException, IOException {
        Assumptions.assumeTrue(new File("../CompatibilityTest").exists()); // skip if the test binary is not available
        int port = brokerAddress.getPort();
        int portMds = brokerPubAddress.getPort();
        Process cppOpencmw = new ProcessBuilder("../CompatibilityTest", "serve", "--port", port + "", "--port-mds", portMds + "").inheritIO().start();
        eventStore.start();
        dataSourcePublisher.start();
        LockSupport.parkNanos(Duration.ofMillis(200).toNanos());

        // get request
        final URI requestURI = URI.create(brokerAddress + "/testProperty?contentType=application/octet-stream&contextA=test&contextB=asdf");
        LOGGER.atInfo().addArgument(requestURI).log("requesting GET from endpoint: {}");
        final Future<TestData> future;
        try (final DataSourcePublisher.Client client = dataSourcePublisher.getClient()) {
            future = client.get(requestURI, null, TestData.class); // uri_without_query oder serviceName + resolver, requestContext, type
        } catch (Exception e) {
            System.err.println(e.getMessage());
            throw e;
        }

        // assert result
        final TestData result = future.get(1000, TimeUnit.MILLISECONDS);

        System.out.println(result.doubleValue);

        eventStore.stop();
        dataSourcePublisher.stop();
        cppOpencmw.destroy();
    }

    @Test
    void testGetRequestWithAnnotations() throws InterruptedException, ExecutionException, TimeoutException, IOException {
        Assumptions.assumeTrue(new File("../CompatibilityTest").exists()); // skip if the test binary is not available
        int port = brokerAddress.getPort();
        int portMds = brokerPubAddress.getPort();
        Process cppOpencmw = new ProcessBuilder("../CompatibilityTest", "serve", "--port", port + "", "--port-mds", portMds + "").inheritIO().start();
        eventStore.start();
        dataSourcePublisher.start();
        LockSupport.parkNanos(Duration.ofMillis(200).toNanos());

        // get request
        final URI requestURI = URI.create(brokerAddress + "/annotatedProperty?contentType=application/octet-stream&contextA=test&contextB=asdf");
        LOGGER.atInfo().addArgument(requestURI).log("requesting GET from endpoint: {}");
        final Future<TestData> future;
        try (final DataSourcePublisher.Client client = dataSourcePublisher.getClient()) {
            future = client.get(requestURI, null, TestData.class); // uri_without_query oder serviceName + resolver, requestContext, type
        } catch (Exception e) {
            System.err.println(e.getMessage());
            throw e;
        }

        // assert result
        final TestData result = future.get(1000, TimeUnit.MILLISECONDS);

        System.out.println(result.doubleValue);

        eventStore.stop();
        dataSourcePublisher.stop();
        cppOpencmw.destroy();
    }

    public static class TestData {
        boolean booleanValue;
        byte int8Value;
        short int16Value;
        int int32Value;
        long int64Value;
        float floatValue;
        double doubleValue;
        char charValue;
        String stringValue;
        boolean[] boolArray;
        byte[] int8Array;
        short[] int16Array;
        int[] int32Array;
        long[] int64Array;
        float[] floatArray;
        double[] doubleArray;
        char[] charArray;
        String[] stringArray;
        // TestData nestedData;

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof TestData td2))
                return false;
            return booleanValue == td2.booleanValue && int8Value == td2.int8Value && int16Value == td2.int16Value && int32Value == td2.int32Value && int64Value == td2.int64Value && floatValue == td2.floatValue && doubleValue == td2.doubleValue && charValue == td2.charValue && stringValue.equals(td2.stringValue) && Arrays.equals(boolArray, td2.boolArray) && Arrays.equals(int8Array, td2.int8Array) && Arrays.equals(int16Array, td2.int16Array) && Arrays.equals(int32Array, td2.int32Array) && Arrays.equals(int64Array, td2.int64Array) && Arrays.equals(floatArray, td2.floatArray) && Arrays.equals(doubleArray, td2.doubleArray) && Arrays.equals(charArray, td2.charArray) && Arrays.equals(stringArray, td2.stringArray);
        }

        @Override
        public int hashCode() {
            return Objects.hash(booleanValue, int8Value, int16Value, int32Value, int64Value, floatValue, doubleValue, charValue, stringValue, Arrays.hashCode(boolArray), Arrays.hashCode(int8Array), Arrays.hashCode(int16Array), Arrays.hashCode(int32Array), Arrays.hashCode(int64Array), Arrays.hashCode(floatArray), Arrays.hashCode(doubleArray), Arrays.hashCode(charArray), Arrays.hashCode(stringArray));
        }
    }

    public static class TestContext implements Filter {
        public MimeType contentType = MimeType.BINARY;

        @Override
        public void clear() {
            contentType = MimeType.BINARY;
        }

        @Override
        public void copyTo(Filter other) {
            if (!(other instanceof TestContext otherContext)) {
                throw new RuntimeException("Trying to copy TestContext into a different filter type");
            }
            otherContext.contentType = contentType;
        }

        @Override
        public String getKey() {
            return "";
        }

        @Override
        public String getValue() {
            return "";
        }

        @Override
        public Filter get(String ctxString) {
            return null;
        }

        @Override
        public boolean matches(Filter other) {
            return false;
        }
    }
}
