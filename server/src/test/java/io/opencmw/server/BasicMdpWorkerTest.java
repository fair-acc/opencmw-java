package io.opencmw.server;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import static io.opencmw.OpenCmwProtocol.Command.W_NOTIFY;
import static io.opencmw.OpenCmwProtocol.EMPTY_FRAME;
import static io.opencmw.OpenCmwProtocol.EMPTY_URI;
import static io.opencmw.OpenCmwProtocol.MdpSubProtocol.PROT_WORKER;

import java.net.URI;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.zeromq.ZContext;

import io.opencmw.OpenCmwProtocol;
import io.opencmw.rbac.BasicRbacRole;
import io.opencmw.utils.SystemProperties;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Timeout(60)
class BasicMdpWorkerTest {
    @BeforeAll
    @Timeout(10)
    void init() {
        System.getProperties().setProperty("OpenCMW.heartBeat", "50");
        assertEquals(50, SystemProperties.getValueIgnoreCase("OpenCMW.heartBeat", 2500), "reduced heart-beat interval");
    }

    @Test
    void testConstructors() {
        assertDoesNotThrow(() -> new BasicMdpWorker(new ZContext(), "testService"));
        assertDoesNotThrow(() -> new BasicMdpWorker(new ZContext(), "testService", BasicRbacRole.ADMIN));
        assertDoesNotThrow(() -> new BasicMdpWorker(URI.create("mdp://localhost"), "testService"));
        assertDoesNotThrow(() -> new BasicMdpWorker(URI.create("mdp://localhost"), "testService", BasicRbacRole.ADMIN));
    }

    @Test
    void testGetterSetter() {
        BasicMdpWorker worker = new BasicMdpWorker(new ZContext(), "testService", BasicRbacRole.ADMIN);
        BasicMdpWorker worker2 = new BasicMdpWorker(new ZContext(), "testService", BasicRbacRole.ADMIN);

        assertThat(worker.getReconnectDelay(), not(equalTo(Duration.of(123, ChronoUnit.MILLIS))));
        assertDoesNotThrow(() -> worker.setReconnectDelay(123, TimeUnit.MILLISECONDS));
        assertThat(worker.getReconnectDelay(), equalTo(Duration.of(123, ChronoUnit.MILLIS)));

        assertEquals("testService", worker.getServiceName());

        assertNotNull(worker.getUniqueID());
        assertNotNull(worker2.getUniqueID());
        assertThat(worker.getUniqueID(), not(equalTo(worker2.getUniqueID())));

        assertNotNull(worker.getRbacRoles());
        assertThat(worker.getRbacRoles(), contains(BasicRbacRole.ADMIN));

        final BasicMdpWorker.RequestHandler handler = c -> {};
        assertThat(worker.getRequestHandler(), not(equalTo(handler)));
        assertDoesNotThrow(() -> worker.registerHandler(handler));
        assertThat(worker.getRequestHandler(), equalTo(handler));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true /*, false */ })
    void testBasicThreadHander(boolean internal) {
        // N.B. this is not a full-blown tests and only covers the basic failure mode, full tests is implemented in MajordomoBrokerTests
        BasicMdpWorker worker = internal ? new BasicMdpWorker(new ZContext(), "testService", BasicRbacRole.ADMIN) : new BasicMdpWorker(URI.create("mdp://*:8080/"), "testService", BasicRbacRole.ADMIN);

        final AtomicBoolean run = new AtomicBoolean(false);
        final AtomicBoolean stop = new AtomicBoolean(false);
        new Thread(() -> {
            run.set(true);
            worker.start();
            stop.set(true);
        }).start();

        await().alias("wait for thread to start worker").atMost(1, TimeUnit.SECONDS).until(run::get, equalTo(true));
        await().alias("wait for thread to have finished starting worker").atMost(1, TimeUnit.SECONDS).until(stop::get, equalTo(true));
        run.set(false);
        stop.set(false);

        assertTrue(worker.shallRun.get(), "run loop is running");

        // check basic notification
        final OpenCmwProtocol.MdpMessage msg = new OpenCmwProtocol.MdpMessage(null, PROT_WORKER, W_NOTIFY, "testService".getBytes(UTF_8), EMPTY_FRAME, EMPTY_URI, EMPTY_FRAME, "", null);
        assertFalse(worker.notify(msg)); // is filtered: no subscription -> false
        assertTrue(worker.notifyRaw(msg)); // is unfiltered: no subscription -> true

        // wait for five heartbeats -> checks poller, heartbeat and reconnect features (to some extend)
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(5L * worker.heartBeatInterval));

        new Thread(() -> {
            run.set(true);
            worker.stopWorker();
            stop.set(true);
        }).start();
        await().alias("wait for thread to stop worker").atMost(1, TimeUnit.SECONDS).until(run::get, equalTo(true));
        await().alias("wait for thread to have finished stopping worker").atMost(1, TimeUnit.SECONDS).until(stop::get, equalTo(true));
    }
}