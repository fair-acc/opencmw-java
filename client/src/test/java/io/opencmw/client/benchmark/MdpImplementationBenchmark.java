package io.opencmw.client.benchmark;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.*;

import io.opencmw.EventStore;
import io.opencmw.Filter;
import io.opencmw.client.DataSourcePublisher;
import io.opencmw.filter.EvtTypeFilter;
import io.opencmw.rbac.BasicRbacRole;
import io.opencmw.server.MajordomoBroker;
import io.opencmw.server.MajordomoWorker;

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
 * description; n_exec; n_workers #0; #1; #2; #3; #4; avg
 * get,  sync, future,     domainObj; 10000; 2;   1631,99;   1760,52;   1815,10;   1732,48;   1999,73;   1826,96
 * get,  sync, eventStore, domainObj; 10000; 2;   2448,23;   4207,49;   2881,22;   2333,16;   2795,14;   3054,25
 * get,  sync, eventStore, ZMsg     ; 10000; 2;   4232,92;   3430,57;   2943,89;   3536,30;   3672,78;   3395,89
 * get, async, eventStore, domainObj; 10000; 2;  12460,94;  16210,89;  17489,04;  17712,35;  19830,50;  17810,70
 * get, async, eventStore, ZMsg     ; 10000; 2;  18491,28;  18322,47;  13879,22;  17203,65;  16070,47;  16368,95
 * sub,  sync, eventStore, domainObj; 10000; 2;   3880,40;   4043,71;   3547,90;   3626,17;   3402,25;   3655,01
 * sub,  sync, eventStore, ZMsg     ; 10000; 2;   4841,42;   6142,45;   5690,95;   5384,73;   4667,64;   5471,44
 * </pre>
 */
@SuppressWarnings({ "PMD.DoNotUseThreads", "PMD.AvoidInstantiatingObjectsInLoops" })
class MdpImplementationBenchmark {
    private static final Logger LOGGER = LoggerFactory.getLogger(MdpImplementationBenchmark.class);
    public static final int N_REPEAT = 5; // how often to repeat each measurement (fist run is usually slower)
    public static final int N_EXEC = 10_000; // number of individual requests to issue for one measurement
    public static final int N_WORKERS = 2; // number of MDP workers
    public static final byte[] PAYLOAD = new byte[100]; // Payload added to the data object
    public static final int SLEEP_DURATION = 100; // sleep time for subscriptions to be established
    public static final long TIMEOUT_NANOS = 100_000_000; // timeout for single requests
    public static final int N_TESTS = 7; // number of different tests
    public static final String[] DESCRIPTIONS = new String[] {
        "get,  sync, future,     domainObj",
        "get,  sync, eventStore, domainObj",
        "get,  sync, eventStore, ZMsg     ",
        "get, async, eventStore, domainObj",
        "get, async, eventStore, ZMsg     ",
        "sub,  sync, eventStore, domainObj",
        "sub,  sync, eventStore, ZMsg     "
        //"subscribe,  sync, eventStore, domainObj"
    };
    public static final String DOUBLE_FORMAT = "%9.2f";

    private MdpImplementationBenchmark() {
        // utility class
    }

    public static void main(String[] args) {
        final double[][] measurements = new double[N_TESTS][N_REPEAT];

        LOGGER.atInfo().log("setting up test broker and event store");
        final EventStore eventStore = EventStore.getFactory().setFilterConfig(TestContext.class, EvtTypeFilter.class).build();
        final MajordomoBroker broker = new MajordomoBroker("TestBroker", "", BasicRbacRole.values());
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
        try (final DataSourcePublisher dataSourcePublisher = new DataSourcePublisher(eventStore, null, null, "testOpenCmwPublisher")) {
            final AtomicInteger receiveCountRaw = new AtomicInteger(0);
            dataSourcePublisher.getRawDataEventStore().register((evt, seq, last) -> {
                if (evt.payload != null && evt.payload.get(DataSourcePublisher.InternalDomainObject.class) != null) {
                    receiveCountRaw.incrementAndGet();
                } else {
                    LOGGER.atError().addArgument(evt.throwables).log("Error(s) received: {}");
                }
            });
            final int brokerPort = Utils.findOpenPort();
            final int brokerPortMds = Utils.findOpenPort();
            final String brokerAddress = broker.bind("mdp://*:" + brokerPort);
            final String brokerPubAddress = broker.bind("mds://*:" + brokerPortMds);
            LOGGER.atDebug().addArgument(brokerAddress).addArgument(brokerPubAddress).log("started broker on {} and {}");

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
            for (int i = 0; i < N_REPEAT; ++i) {
                measurements[0][i] = synchronousGet(DESCRIPTIONS[0], dataSourcePublisher, brokerPort);
            }
            for (int i = 0; i < N_REPEAT; ++i) {
                measurements[1][i] = synchronousGetRaw(DESCRIPTIONS[1], dataSourcePublisher, brokerPort, receiveCount);
            }
            for (int i = 0; i < N_REPEAT; ++i) {
                measurements[2][i] = synchronousGetRaw(DESCRIPTIONS[2], dataSourcePublisher, brokerPort, receiveCountRaw);
            }
            for (int i = 0; i < N_REPEAT; ++i) {
                measurements[3][i] = asyncGetRaw(DESCRIPTIONS[3], dataSourcePublisher, brokerPort, receiveCount);
            }
            for (int i = 0; i < N_REPEAT; ++i) {
                measurements[4][i] = asyncGetRaw(DESCRIPTIONS[4], dataSourcePublisher, brokerPort, receiveCountRaw);
            }
            for (int i = 0; i < N_REPEAT; ++i) {
                measurements[5][i] = synchronousSubscribeEventStore(DESCRIPTIONS[5], dataSourcePublisher, workers.get(0), brokerPortMds, receiveCount);
            }
            for (int i = 0; i < N_REPEAT; ++i) {
                measurements[6][i] = synchronousSubscribeEventStore(DESCRIPTIONS[6], dataSourcePublisher, workers.get(0), brokerPortMds, receiveCountRaw);
            }
            // for (int i = 0; i < N_REPEAT; ++i) {
            //     measurements[7][i] = synchronousSubscribeListener(descriptions[7], dataSourcePublisher, workers.get(0), brokerPortMds);
            // }
            LOGGER.atInfo().log("shutting down everything");

        } catch (IOException e) {
            LOGGER.atError().setCause(e).log("error during benchmark");
        }
        workers.forEach(MajordomoWorker::stopWorker);
        LockSupport.parkNanos(Duration.ofMillis(50).toNanos());
        broker.stopBroker();

        // evaluate and print measurement results
        System.out.println("Measurement results:");
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
    }

    private static double synchronousGet(final String description, final DataSourcePublisher dataSourcePublisher, final int brokerPort) {
        final URI requestUri = URI.create("mdp://localhost:" + brokerPort + "/testWorker");
        final TestContext requestContext = new TestContext("testContext");
        try (final DataSourcePublisher.Client client = dataSourcePublisher.getClient()) {
            return measure(description, N_EXEC, () -> {
                try {
                    client.get(requestUri, requestContext, TestObject.class).get(1000, TimeUnit.MILLISECONDS);
                } catch (TimeoutException e) {
                    LOGGER.atInfo().log("get request timed out");
                } catch (InterruptedException | ExecutionException e) {
                    LOGGER.atError().setCause(e).log("error during test execution");
                }
            });
        }
    }

    private static double synchronousGetRaw(final String description, final DataSourcePublisher dataSourcePublisher, final int brokerPort, final AtomicInteger receiveCount) {
        final URI requestUri = URI.create("mdp://localhost:" + brokerPort + "/testWorker");
        final TestContext requestContext = new TestContext("testContext");
        final AtomicInteger lastCount = new AtomicInteger(receiveCount.get());
        try (final DataSourcePublisher.Client client = dataSourcePublisher.getClient()) {
            return measure(description, N_EXEC, () -> {
                client.get(requestUri, requestContext, TestObject.class);
                while (lastCount.get() == receiveCount.get()) {
                    // do nothing
                }
                lastCount.set(receiveCount.get());
            });
        }
    }

    private static double asyncGetRaw(final String description, final DataSourcePublisher dataSourcePublisher, final int brokerPort, final AtomicInteger receiveCount) {
        final URI requestUri = URI.create("mdp://localhost:" + brokerPort + "/testWorker");
        final TestContext requestContext = new TestContext("testContext");
        final AtomicInteger lastCount = new AtomicInteger(receiveCount.get());
        try (final DataSourcePublisher.Client client = dataSourcePublisher.getClient()) {
            return measureAsync(description, N_EXEC,
                    () -> client.get(requestUri, requestContext, TestObject.class), () -> {
                        while (lastCount.get() + N_EXEC > receiveCount.get()) {
                            // do nothing
                        }
                    });
        }
    }

    private static double synchronousSubscribeEventStore(final String description, final DataSourcePublisher dataSourcePublisher, final MajordomoWorker<TestContext, TestObject, TestObject> worker, final int brokerPortMds, final AtomicInteger receivedCount) {
        final URI requestUri = URI.create("mds://localhost:" + brokerPortMds + "/testWorker");
        final TestContext requestContext = new TestContext("testContext");
        final TestObject testObject = new TestObject();
        final TestContext testContext = new TestContext("testContext");
        final AtomicBoolean received = new AtomicBoolean(false);
        try (final DataSourcePublisher.Client client = dataSourcePublisher.getClient()) {
            final String reqId = client.subscribe(requestUri, TestObject.class, requestContext, TestContext.class, null);
            LOGGER.atInfo().addArgument(reqId).log("Subscribed with reqId: '{}'");
            LockSupport.parkNanos(Duration.ofMillis(SLEEP_DURATION).toNanos());
            worker.notify(testContext, testObject); // first notification after subscribe is swallowed
            LockSupport.parkNanos(Duration.ofMillis(SLEEP_DURATION).toNanos());
            final AtomicInteger sentCount = new AtomicInteger(receivedCount.get());
            // do nothing
            final double result = measure(description, N_EXEC, () -> {
                worker.notify(testContext, testObject);
                sentCount.incrementAndGet();
                final long timeout = System.nanoTime() + TIMEOUT_NANOS;
                while (receivedCount.get() < sentCount.get() && timeout > System.nanoTime()) {
                    // do nothing
                }
                if (receivedCount.get() < sentCount.get()) {
                    receivedCount.incrementAndGet();
                    LOGGER.atInfo().log("Notification timed out");
                }
            });
            client.unsubscribe(reqId);
            LockSupport.parkNanos(Duration.ofMillis(SLEEP_DURATION).toNanos());
            return result;
        }
    }

    private static double synchronousSubscribeListener(final String description, final DataSourcePublisher dataSourcePublisher, final MajordomoWorker<TestContext, TestObject, TestObject> worker, final int brokerPortMds) {
        final URI requestUri = URI.create("mds://localhost:" + brokerPortMds + "/testWorker");
        final TestContext requestContext = new TestContext("testContext");
        final TestObject testObject = new TestObject();
        final TestContext testContext = new TestContext("testContext");
        final AtomicBoolean received = new AtomicBoolean(false);
        try (final DataSourcePublisher.Client client = dataSourcePublisher.getClient()) {
            final String reqId = client.subscribe(requestUri, TestObject.class, requestContext, TestContext.class, new DataSourcePublisher.NotificationListener<>() {
                @Override
                public void dataUpdate(final TestObject updatedObject, final TestContext contextObject) {
                    received.set(true);
                }

                @Override
                public void updateException(final Throwable exception) {
                    LOGGER.atError().setCause(exception).log("Error received by notification hanlder:");
                }
            });
            LOGGER.atInfo().addArgument(reqId).log("Subscribed with reqId: '{}'");
            LockSupport.parkNanos(Duration.ofMillis(SLEEP_DURATION).toNanos());
            worker.notify(testContext, testObject); // first notification after subscribe is swallowed
            LockSupport.parkNanos(Duration.ofMillis(SLEEP_DURATION).toNanos());
            // do nothing
            final double result = measure(description, N_EXEC, () -> {
                worker.notify(testContext, testObject);
                final long timeout = System.nanoTime() + TIMEOUT_NANOS;
                while (!received.compareAndSet(true, false)) {
                    if (timeout < System.nanoTime()) {
                        LOGGER.atWarn().log("Timeout for subscription notification");
                        break;
                    }
                    // do nothing
                }
            });
            client.unsubscribe(reqId);
            LockSupport.parkNanos(Duration.ofMillis(SLEEP_DURATION).toNanos());
            return result;
        }
    }

    private static double measure(final String topic, final int nExec, final Runnable... runnable) {
        final long start = System.nanoTime();

        for (Runnable run : runnable) {
            for (int i = 0; i < nExec; i++) {
                run.run();
            }
        }

        final long stop = System.nanoTime();
        final double result = (1_000_000_000L * nExec) / (double) (stop - start);
        LOGGER.atInfo().addArgument(String.format("%-40s:  %10d calls/second", topic, (1_000_000_000L * nExec) / (stop - start))).log("{}");
        return result;
    }

    private static double measureAsync(final String topic, final int nExec, final Runnable runnable, final Runnable completed) {
        final long start = System.nanoTime();

        for (int i = 0; i < nExec; i++) {
            runnable.run();
        }
        completed.run();

        final long stop = System.nanoTime();
        final double result = (1_000_000_000L * nExec) / (double) (stop - start);
        LOGGER.atInfo().addArgument(String.format("%-40s:  %10d calls/second", topic, (1_000_000_000L * nExec) / (stop - start))).log("{}");
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
