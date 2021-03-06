package io.opencmw.client.benchmark;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.Utils;

import io.opencmw.EventStore;
import io.opencmw.Filter;
import io.opencmw.client.DataSourcePublisher;
import io.opencmw.filter.EvtTypeFilter;
import io.opencmw.rbac.BasicRbacRole;
import io.opencmw.server.MajordomoBroker;
import io.opencmw.server.MajordomoWorker;

import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.HardwareAbstractionLayer;

/**
 * Benchmark for OpenCMW's majordomo and client implementation.
 * Starts a broker, a worker and DataSourcePublisher and benchmarks:
 * - Round trip time for get requests
 *
 * The timings are evaluated at different data rates.
 *
 * Example output:
 * <pre>
 * Measurement results:
 * CPU:Intel(R) Core(TM) i7-8565U CPU @ 1.80GHz
 * description; n_exec; n_workers #0; #1; #2; #3; #4; avg
 * get,  sync, future,     domain-object ; 10000; 1;   3502.19;   8820.09;   9605.81;   9607.85;   9604.65;   9409.60
 * get,  sync, eventStore, domain-object ; 10000; 1;   8526.33;   8780.76;   8584.98;   8653.16;   8576.64;   8648.88
 * get,  sync, eventStore, raw-byte[]    ; 10000; 1;   7544.06;   7435.91;   7398.87;   5887.83;   5886.43;   6652.26
 * get, async, eventStore, domain-object ; 10000; 1;   8263.84;  11893.78;  14655.36;  14224.01;  13789.92;  13640.77
 * get, async, eventStore, raw-byte[]    ; 10000; 1;  15179.01;  14727.11;  14152.44;  13722.89;  15003.56;  14401.50
 * sub, async, eventStore, domain-object ; 10000; 1;  18981.42;  25884.94;  24339.60;  18284.27;  22403.44;  22728.06
 * sub, async, eventStore, raw-byte[]    ; 10000; 1;  24239.33;  22523.09;  27169.31;  25580.29;  33291.53;  27141.05
 * sub, async, callback,   domain-object ; 10000; 1;  21935.83;  29934.65;  25776.69;  28245.24;  28820.02;  28194.15
 * </pre>
 */
@SuppressWarnings({ "PMD.DoNotUseThreads", "PMD.AvoidInstantiatingObjectsInLoops" })
class MdpImplementationBenchmark {
    private static final Logger LOGGER = LoggerFactory.getLogger(MdpImplementationBenchmark.class);
    public static final ExecutorService executorService = Executors.newFixedThreadPool(10);
    public static final int N_REPEAT = 5; // how often to repeat each measurement (fist run is usually slower)
    public static final int N_WARMUP = 1; // how many iterations to execute before taking measurements
    public static final int N_EXEC = 10_000; // number of individual requests to issue for one measurement
    public static final int N_WORKERS = 1; // number of MDP workers
    public static final byte[] PAYLOAD = new byte[100]; // Payload added to the data object
    public static final int SLEEP_DURATION = 100; // sleep time for subscriptions to be established
    public static final long TIMEOUT_NANOS = 100_000_000; // timeout for single requests
    public static final String[] DESCRIPTIONS = new String[] {
        "get,  sync, future,     domain-object ",
        "get,  sync, eventStore, domain-object ",
        "get,  sync, eventStore, raw-byte[]    ",
        "get, async, eventStore, domain-object ",
        "get, async, eventStore, raw-byte[]    ",
        "sub, async, eventStore, domain-object ",
        "sub, async, eventStore, raw-byte[]    ",
        "sub, async, callback,   domain-object "
    };
    public static final int N_TESTS = DESCRIPTIONS.length; // number of different tests
    public static final String DOUBLE_FORMAT = "%9.2f";

    private MdpImplementationBenchmark() {
        // utility class
    }

    public static void main(String[] args) {
        final double[][] measurements = new double[N_TESTS][N_REPEAT];

        LOGGER.atInfo().log("setting up test broker and event store");
        final EventStore eventStore = EventStore.getFactory().setFilterConfig(TestContext.class, EvtTypeFilter.class).build();
        final MajordomoBroker broker = new MajordomoBroker("TestBroker", null, BasicRbacRole.values());
        final List<MajordomoWorker<TestContext, TestObject, TestObject>> workers = new ArrayList<>(N_WORKERS);
        for (int i = 0; i < N_WORKERS; ++i) {
            workers.add(new MajordomoWorker<>(broker.getContext(), "/testWorker", TestContext.class, TestObject.class, TestObject.class));
        }
        final AtomicInteger receiveCount = new AtomicInteger(0);
        eventStore.register((evt, seq, last) -> {
            if (evt.payload != null && evt.payload.get(TestObject.class) != null) {
                receiveCount.incrementAndGet();
            } else {
                LOGGER.atError().addArgument(evt.throwables).log("Error(s) received: {}");
            }
        });
        try (final DataSourcePublisher dataSourcePublisher = new DataSourcePublisher(null, eventStore, null, executorService, "testOpenCmwPublisher")) {
            final AtomicInteger receiveCountRaw = new AtomicInteger(0);
            dataSourcePublisher.getRawDataEventStore().register((evt, seq, last) -> {
                if (evt.throwables.isEmpty()) {
                    receiveCountRaw.incrementAndGet();
                } else {
                    LOGGER.atError().addArgument(evt.throwables).log("error(s) received: {}");
                }
            });
            final URI brokerAddress = broker.bind(URI.create("mdp://*:" + Utils.findOpenPort()));
            final URI brokerAddressPub = broker.bind(URI.create("mds://*:" + Utils.findOpenPort()));
            LOGGER.atInfo().addArgument(brokerAddress).addArgument(brokerAddressPub).log("started broker on {} and {}");

            workers.forEach(worker -> worker.setHandler((ctx, requestCtx, request, replyCtx, reply) -> {
                replyCtx.ctx = "FAIR.SELECTOR.C=3:P=5";
                reply.set(new TestObject(1, PAYLOAD));
            }));

            eventStore.start();
            workers.forEach(MajordomoWorker::start);
            broker.start();
            dataSourcePublisher.start();
            LockSupport.parkNanos(Duration.ofMillis(200).toNanos());

            LOGGER.atInfo().addArgument(N_EXEC).addArgument(PAYLOAD.length).addArgument(N_WORKERS).log("run tests: n={}, payloadSize={}bytes, nWorkers={}");
            for (int j = 0; j < N_WARMUP; j++) {
                for (int i = 0; i < N_REPEAT; ++i) {
                    measurements[0][i] = synchronousGet(DESCRIPTIONS[0], dataSourcePublisher, brokerAddress);
                }
                for (int i = 0; i < N_REPEAT; ++i) {
                    measurements[1][i] = synchronousGetRaw(DESCRIPTIONS[1], dataSourcePublisher, brokerAddress, receiveCount);
                }
                for (int i = 0; i < N_REPEAT; ++i) {
                    measurements[2][i] = synchronousGetRaw(DESCRIPTIONS[2], dataSourcePublisher, brokerAddress, receiveCountRaw);
                }
                for (int i = 0; i < N_REPEAT; ++i) {
                    measurements[3][i] = asyncGetRaw(DESCRIPTIONS[3], dataSourcePublisher, brokerAddress, receiveCount);
                }
                for (int i = 0; i < N_REPEAT; ++i) {
                    measurements[4][i] = asyncGetRaw(DESCRIPTIONS[4], dataSourcePublisher, brokerAddress, receiveCountRaw);
                }
                for (int i = 0; i < N_REPEAT; ++i) {
                    measurements[5][i] = subscribeEventStore(DESCRIPTIONS[5], dataSourcePublisher, workers.get(0), brokerAddressPub, receiveCount);
                }
                for (int i = 0; i < N_REPEAT; ++i) {
                    measurements[6][i] = subscribeEventStore(DESCRIPTIONS[6], dataSourcePublisher, workers.get(0), brokerAddressPub, receiveCountRaw);
                }
                for (int i = 0; i < N_REPEAT; ++i) {
                    measurements[7][i] = subscribeListener(DESCRIPTIONS[7], dataSourcePublisher, workers.get(0), brokerAddressPub);
                }
            }
            LOGGER.atInfo().log("shutting down everything");
        } catch (IOException e) {
            LOGGER.atError().setCause(e).log("error during benchmark");
        }
        workers.forEach(MajordomoWorker::stopWorker);
        LockSupport.parkNanos(Duration.ofMillis(50).toNanos());
        broker.stopBroker();

        SystemInfo systemInfo = new SystemInfo();
        HardwareAbstractionLayer hardwareAbstractionLayer = systemInfo.getHardware();
        final CentralProcessor.ProcessorIdentifier cpuID = hardwareAbstractionLayer.getProcessor().getProcessorIdentifier();
        // evaluate and print measurement results
        System.out.println("Measurement results:");
        System.out.println("CPU:" + cpuID.getName());
        System.out.println("description; n_exec; n_workers " + IntStream.range(0, N_REPEAT).mapToObj(i -> "#" + i).collect(Collectors.joining("; ")) + "; avg");
        for (int i = 0; i < measurements.length; i++) {
            final double[] measurement = measurements[i];
            System.out.print(DESCRIPTIONS[i] + "; " + N_EXEC + "; " + N_WORKERS + "; ");
            for (double datapoint : measurement) {
                System.out.printf(DOUBLE_FORMAT, datapoint);
                System.out.print("; ");
            }
            // average excluding the first measurement as warm up
            final double avg = Arrays.stream(measurement, 1, N_REPEAT).sum() / (double) (N_REPEAT - 1);
            System.out.printf(DOUBLE_FORMAT, avg);
            System.out.print('\n');
        }
        System.exit(0);
    }

    private static double synchronousGet(final String description, final DataSourcePublisher dataSourcePublisher, final URI brokerAddress) {
        final URI requestUri = URI.create(brokerAddress + "/testWorker");
        final TestContext requestContext = new TestContext("testContext");
        try (final DataSourcePublisher.Client client = dataSourcePublisher.getClient()) {
            return measure(description, () -> {
                try {
                    client.get(requestUri, requestContext, TestObject.class).get(1000, TimeUnit.MILLISECONDS);
                } catch (TimeoutException | InterruptedException | ExecutionException e) {
                    throw new IllegalStateException("error during test execution", e);
                }
            });
        }
    }

    private static double synchronousGetRaw(final String description, final DataSourcePublisher dataSourcePublisher, final URI brokerAddress, final AtomicInteger receiveCount) {
        final URI requestUri = URI.create(brokerAddress + "/testWorker");
        final TestContext requestContext = new TestContext("testContext");
        final AtomicInteger lastCount = new AtomicInteger(receiveCount.get());
        try (final DataSourcePublisher.Client client = dataSourcePublisher.getClient()) {
            return measure(description, () -> {
                client.get(requestUri, requestContext, TestObject.class);
                //noinspection StatementWithEmptyBody
                while (lastCount.get() == receiveCount.get()) {
                    // do nothing
                }
                lastCount.set(receiveCount.get());
            });
        }
    }

    private static double asyncGetRaw(final String description, final DataSourcePublisher dataSourcePublisher, final URI brokerAddress, final AtomicInteger receiveCount) {
        final URI requestUri = URI.create(brokerAddress + "/testWorker");
        final TestContext requestContext = new TestContext("testContext");
        final AtomicInteger lastCount = new AtomicInteger(receiveCount.get());
        try (final DataSourcePublisher.Client client = dataSourcePublisher.getClient()) {
            return measureAsync(description, () -> client.get(requestUri, requestContext, TestObject.class), () -> {
                //noinspection StatementWithEmptyBody
                while (lastCount.get() + N_EXEC > receiveCount.get()) {
                    // do nothing
                }
            });
        }
    }

    private static double subscribeEventStore(final String description, final DataSourcePublisher dataSourcePublisher, final MajordomoWorker<TestContext, TestObject, TestObject> worker, final URI brokerAddressPub, final AtomicInteger receivedCount) {
        final URI requestUri = URI.create(brokerAddressPub + "/testWorker");
        final TestContext requestContext = new TestContext("testContext");
        final TestObject testObject = new TestObject();
        final TestContext testContext = new TestContext("testContext");
        try (final DataSourcePublisher.Client client = dataSourcePublisher.getClient()) {
            final String reqId = client.subscribe(requestUri, TestObject.class, requestContext, TestContext.class, null);
            LockSupport.parkNanos(Duration.ofMillis(SLEEP_DURATION).toNanos());
            worker.notify(testContext, testObject); // first notification after subscribe is swallowed
            LockSupport.parkNanos(Duration.ofMillis(SLEEP_DURATION).toNanos());
            receivedCount.set(0);
            // do nothing
            AtomicInteger sendCounter = new AtomicInteger(0);
            final double result = measure(description, () -> {
                // executed in notification thread
                worker.notify(testContext, testObject);
                sendCounter.getAndIncrement(); }, () -> {
                // executed in client thread
                final long timeout = System.nanoTime() + TIMEOUT_NANOS;
                //noinspection StatementWithEmptyBody
                while (receivedCount.get() < sendCounter.get() && timeout > System.nanoTime()) { // NOPMD NOSONAR busy polling
                    // do nothing
                } });
            if (receivedCount.get() < N_EXEC) {
                throw new IllegalStateException("notification timed out - received only " + receivedCount.get() + " of " + sendCounter.get() + " notifications");
            }
            client.unsubscribe(reqId);
            LockSupport.parkNanos(Duration.ofMillis(SLEEP_DURATION).toNanos());
            return result;
        }
    }

    private static double subscribeListener(final String description, final DataSourcePublisher dataSourcePublisher, final MajordomoWorker<TestContext, TestObject, TestObject> worker, final URI brokerPortMds) {
        final URI requestUri = URI.create(brokerPortMds + "/testWorker");
        final TestContext requestContext = new TestContext("testContext");
        final TestObject testObject = new TestObject();
        final TestContext testContext = new TestContext("testContext");
        final AtomicInteger sentNotifications = new AtomicInteger(0);
        final AtomicInteger receivedNotifications = new AtomicInteger(0);
        try (final DataSourcePublisher.Client client = dataSourcePublisher.getClient()) {
            final String reqId = client.subscribe(requestUri, TestObject.class, requestContext, TestContext.class, new DataSourcePublisher.NotificationListener<>() {
                @Override
                public void dataUpdate(final TestObject updatedObject, final TestContext contextObject) {
                    receivedNotifications.getAndIncrement();
                }

                @Override
                public void updateException(final Throwable exception) {
                    throw new IllegalStateException("error received by notification handler", exception);
                }
            });
            LockSupport.parkNanos(Duration.ofMillis(SLEEP_DURATION).toNanos());
            worker.notify(testContext, testObject); // first notification after subscribe is swallowed
            LockSupport.parkNanos(Duration.ofMillis(SLEEP_DURATION).toNanos());
            final double result = measure(description, () -> {
                worker.notify(testContext, testObject);
                sentNotifications.getAndIncrement(); }, () -> {
                // executed in client thread
                final long timeout = System.nanoTime() + TIMEOUT_NANOS;
                //noinspection StatementWithEmptyBody
                while (receivedNotifications.get() < sentNotifications.get() && timeout > System.nanoTime()) { // NOPMD NOSONAR busy polling
                    // do nothing
                } });
            if (receivedNotifications.get() < N_EXEC) {
                throw new IllegalStateException("notification timed out - received only " + receivedNotifications.get() + " of " + sentNotifications.get() + " notifications");
            }
            client.unsubscribe(reqId);
            LockSupport.parkNanos(Duration.ofMillis(SLEEP_DURATION).toNanos());
            return result;
        }
    }

    private static double measure(final String topic, final Runnable... runnable) {
        final long start = System.nanoTime();

        // some operations
        ArrayList<Future<Boolean>> jobs = new ArrayList<>();
        for (Runnable run : runnable) {
            jobs.add(executorService.submit(() -> {
                for (int i = 0; i < MdpImplementationBenchmark.N_EXEC; i++) {
                    run.run();
                }
                return true;
            }));
        }
        for (final Future<Boolean> job : jobs) {
            try {
                final Boolean result = job.get();
                assert result : "job did not finish for topic: " + topic;
            } catch (InterruptedException | ExecutionException e) {
                throw new IllegalStateException("test execution failed for topic: " + topic, e);
            }
        }

        final long stop = System.nanoTime();
        final double result = (1_000_000_000L * MdpImplementationBenchmark.N_EXEC) / (double) (stop - start);
        LOGGER.atInfo().addArgument(String.format("%-40s:  %10d calls/second", topic, (1_000_000_000L * MdpImplementationBenchmark.N_EXEC) / (stop - start))).log("{}");
        return result;
    }

    private static double measureAsync(final String topic, final Runnable runnable, final Runnable completed) {
        final long start = System.nanoTime();

        for (int i = 0; i < MdpImplementationBenchmark.N_EXEC; i++) {
            runnable.run();
        }
        completed.run();

        final long stop = System.nanoTime();
        final double result = (1_000_000_000L * MdpImplementationBenchmark.N_EXEC) / (double) (stop - start);
        LOGGER.atInfo().addArgument(String.format("%-40s:  %10d calls/second", topic, (1_000_000_000L * MdpImplementationBenchmark.N_EXEC) / (stop - start))).log("{}");
        return result;
    }

    public static class TestObject {
        public int seq = 0;
        public byte[] payload = null;

        public TestObject(final int seq, final byte[] payload) {
            this.seq = seq;
            this.payload = payload;
        }

        public TestObject() {
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            final TestObject that = (TestObject) o;
            return seq == that.seq && Arrays.equals(payload, that.payload);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(seq);
            result = 31 * result + Arrays.hashCode(payload);
            return result;
        }

        @Override
        public String toString() {
            return "TestObject{seq=" + seq + ", payload=" + Arrays.toString(payload) + '}';
        }

        public void set(final TestObject other) {
            this.seq = other.seq;
            this.payload = other.payload;
        }
    }

    public static class TestContext implements Filter {
        public String ctx = "";

        public TestContext() {
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
        }

        @Override
        public void copyTo(final Filter other) {
            if (other instanceof TestContext) {
                ((TestContext) other).ctx = ctx;
            }
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            final TestContext that = (TestContext) o;
            return that.ctx.equals(this.ctx);
        }

        @Override
        public String getKey() {
            return "testContext";
        }

        @Override
        public String getValue() {
            return ctx;
        }

        @Override
        public Filter get(final String ctxString) {
            return new TestContext(ctxString);
        }

        @Override
        public boolean matches(final Filter other) {
            return (other instanceof TestContext) && this.ctx != null && this.ctx.equals(((TestContext) other).ctx);
        }

        @Override
        public int hashCode() {
            return ctx.hashCode();
        }
    }
}
