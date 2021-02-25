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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.*;

import io.opencmw.EventStore;
import io.opencmw.Filter;
import io.opencmw.MimeType;
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
 * run tests: n=10000, payloadSize=100bytes
 * synchronous get, future, full object deserialisation #1     :         822 calls/second
 * synchronous get, future, full object deserialisation #2     :        2139 calls/second
 * synchronous get, future, full object deserialisation #3     :        2577 calls/second
 * synchronous get, eventStore, full object deserialisation #1 :        1933 calls/second
 * synchronous get, eventStore, full object deserialisation #2 :        2024 calls/second
 * synchronous get, eventStore, full object deserialisation #3 :        1990 calls/second
 * synchronous get, eventStore, raw message #1                 :        2094 calls/second
 * synchronous get, eventStore, raw message #2                 :        2080 calls/second
 * synchronous get, eventStore, raw message #3                 :        1968 calls/second
 * asynchronous get, eventStore, full object deserialisation #1:        2782 calls/second
 * asynchronous get, eventStore, full object deserialisation #2:        3071 calls/second
 * asynchronous get, eventStore, full object deserialisation #3:        2887 calls/second
 * asynchronous get, eventStore, raw message #1                :        3046 calls/second
 * asynchronous get, eventStore, raw message #2                :        2849 calls/second
 * asynchronous get, eventStore, raw message #3                :        3400 calls/second
 * shutting down everything
 *
 */
@SuppressWarnings({ "PMD.DoNotUseThreads", "PMD.AvoidInstantiatingObjectsInLoops" })
class MdpImplementationBenchmark {
    private static final Logger LOGGER = LoggerFactory.getLogger(MdpImplementationBenchmark.class);
    public static final int N_REPEAT = 3; // how often to repeat each measurement (fist run is usually slower)
    public static final int N_EXEC = 10_000; // number of individual requests to issue for one measurement
    public static final int N_WORKERS = 1; // number of MDP workers
    public static final byte[] PAYLOAD = new byte[100]; // Payload added to the data object
    private static int nExecutions = N_EXEC;

    private MdpImplementationBenchmark() {
        // utility class
    }

    public static void main(String[] args) {
        if (args.length == 1) {
            nExecutions = Integer.parseInt(args[0]);
        }

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
                if (evt.payload != null && evt.payload.get(ZMsg.class) != null) {
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

            LOGGER.atInfo().addArgument(nExecutions).addArgument(PAYLOAD.length).log("run tests: n={}, payloadSize={}bytes");
            for (int i = 0; i < N_REPEAT; ++i) {
                synchronousGet("synchronous get, future, full object deserialisation #" + i + "     ", dataSourcePublisher, brokerPort);
            }
            for (int i = 0; i < N_REPEAT; ++i) {
                synchronousGetRaw("synchronous get, eventStore, full object deserialisation #" + i + " ", dataSourcePublisher, brokerPort, receiveCount);
            }
            for (int i = 0; i < N_REPEAT; ++i) {
                synchronousGetRaw("synchronous get, eventStore, raw message #" + i + "                 ", dataSourcePublisher, brokerPort, receiveCountRaw);
            }
            for (int i = 0; i < N_REPEAT; ++i) {
                asyncGetRaw("asynchronous get, eventStore, full object deserialisation #" + i, dataSourcePublisher, brokerPort, receiveCount);
            }
            for (int i = 0; i < N_REPEAT; ++i) {
                asyncGetRaw("asynchronous get, eventStore, raw message #" + i + "                ", dataSourcePublisher, brokerPort, receiveCountRaw);
            }

            LOGGER.atInfo().log("shutting down everything");

        } catch (IOException e) {
            LOGGER.atError().setCause(e).log("error during benchmark");
        }
        workers.forEach(MajordomoWorker::stopWorker);
        LockSupport.parkNanos(Duration.ofMillis(50).toNanos());
        broker.stopBroker();
    }

    private static void synchronousGet(final String description, final DataSourcePublisher dataSourcePublisher, final int brokerPort) {
        final URI requestUri = URI.create("mdp://localhost:" + brokerPort + "/testWorker");
        final TestContext requestContext = new TestContext("testContext");
        try (final DataSourcePublisher.Client client = dataSourcePublisher.getClient()) {
            measure(description, nExecutions, () -> {
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

    private static void synchronousGetRaw(final String description, final DataSourcePublisher dataSourcePublisher, final int brokerPort, final AtomicInteger receiveCount) {
        final URI requestUri = URI.create("mdp://localhost:" + brokerPort + "/testWorker");
        final TestContext requestContext = new TestContext("testContext");
        final AtomicInteger lastCount = new AtomicInteger(receiveCount.get());
        try (final DataSourcePublisher.Client client = dataSourcePublisher.getClient()) {
            measure(description, nExecutions, () -> {
                client.get(requestUri, requestContext, TestObject.class);
                while (lastCount.get() == receiveCount.get()) {
                    // do nothing
                }
                lastCount.set(receiveCount.get());
            });
        }
    }

    private static void asyncGetRaw(final String description, final DataSourcePublisher dataSourcePublisher, final int brokerPort, final AtomicInteger receiveCount) {
        final URI requestUri = URI.create("mdp://localhost:" + brokerPort + "/testWorker");
        final TestContext requestContext = new TestContext("testContext");
        final AtomicInteger lastCount = new AtomicInteger(receiveCount.get());
        try (final DataSourcePublisher.Client client = dataSourcePublisher.getClient()) {
            measureAsync(description, nExecutions,
                    () -> client.get(requestUri, requestContext, TestObject.class), () -> {
                        while (lastCount.get() + nExecutions > receiveCount.get()) {
                            // do nothing
                        }
                    });
        }
    }

    private static void measure(final String topic, final int nExec, final Runnable... runnable) {
        final long start = System.currentTimeMillis();

        for (Runnable run : runnable) {
            for (int i = 0; i < nExec; i++) {
                run.run();
            }
        }

        final long stop = System.currentTimeMillis();
        LOGGER.atInfo().addArgument(String.format("%-40s:  %10d calls/second", topic, (1000L * nExec) / (stop - start))).log("{}");
    }

    private static void measureAsync(final String topic, final int nExec, final Runnable runnable, final Runnable completed) {
        final long start = System.currentTimeMillis();

        for (int i = 0; i < nExec; i++) {
            runnable.run();
        }
        completed.run();

        final long stop = System.currentTimeMillis();
        LOGGER.atInfo().addArgument(String.format("%-40s:  %10d calls/second", topic, (1000L * nExec) / (stop - start))).log("{}");
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
