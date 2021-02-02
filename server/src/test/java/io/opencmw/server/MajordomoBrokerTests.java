package io.opencmw.server;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.*;

import static io.opencmw.OpenCmwProtocol.Command.FINAL;
import static io.opencmw.OpenCmwProtocol.Command.GET_REQUEST;
import static io.opencmw.OpenCmwProtocol.MdpMessage;
import static io.opencmw.OpenCmwProtocol.MdpSubProtocol;
import static io.opencmw.OpenCmwProtocol.MdpSubProtocol.PROT_CLIENT;
import static io.opencmw.OpenCmwProtocol.MdpSubProtocol.PROT_WORKER;
import static io.opencmw.utils.AnsiDefs.ANSI_RED;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.junit.jupiter.api.Test;
import org.zeromq.SocketType;
import org.zeromq.Utils;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import io.opencmw.rbac.BasicRbacRole;
import io.opencmw.rbac.RbacToken;

class MajordomoBrokerTests {
    private static final byte[] DEFAULT_RBAC_TOKEN = new RbacToken(BasicRbacRole.ADMIN, "HASHCODE").getBytes();
    private static final String DEFAULT_MMI_SERVICE = "mmi.service";
    private static final String DEFAULT_ECHO_SERVICE = "mmi.echo";
    private static final String DEFAULT_REQUEST_MESSAGE = "Hello World!";
    private static final byte[] DEFAULT_REQUEST_MESSAGE_BYTES = DEFAULT_REQUEST_MESSAGE.getBytes(UTF_8);

    @Test
    void basicLowLevelRequestReplyTest() throws IOException {
        MajordomoBroker broker = new MajordomoBroker("TestBroker", "", BasicRbacRole.values());
        // broker.setDaemon(true); // use this if running in another app that controls threads
        final String brokerAddress = broker.bind("mdp://*:" + Utils.findOpenPort());
        assertFalse(broker.isRunning(), "broker not running");
        broker.start();
        assertTrue(broker.isRunning(), "broker running");
        // test interfaces
        assertNotNull(broker.getContext());
        assertNotNull(broker.getInternalRouterSocket());
        assertNotNull(broker.getServices());
        assertEquals(4, broker.getServices().size());
        assertDoesNotThrow(() -> broker.addInternalService(new BasicMdpWorker(broker.getContext(), "demoService")));
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100)); // wait until services are started
        assertEquals(5, broker.getServices().size());
        assertDoesNotThrow(() -> broker.removeService("demoService"));
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100)); // wait until services are stopped
        assertEquals(5, broker.getServices().size());

        // wait until all services are initialised
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(200));

        final ZMQ.Socket clientSocket = broker.getContext().createSocket(SocketType.DEALER);
        clientSocket.setIdentity("demoClient".getBytes(UTF_8));
        System.err.println("brokerAddress = " + brokerAddress);
        clientSocket.connect(brokerAddress.replace("mdp", "tcp"));

        // wait until client is connected
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(200));

        final byte[] clientRequestID = "unit-test-clientRequestID".getBytes(UTF_8);
        new MdpMessage(null, PROT_CLIENT, GET_REQUEST, DEFAULT_ECHO_SERVICE.getBytes(UTF_8), clientRequestID, URI.create(DEFAULT_ECHO_SERVICE), DEFAULT_REQUEST_MESSAGE_BYTES, "", new byte[0]).send(clientSocket);
        System.err.println("sent request");
        final MdpMessage clientMessage = MdpMessage.receive(clientSocket);
        System.err.println("received reply");
        assertNotNull(clientMessage, "reply message w/o RBAC token not being null");
        assertNotNull(clientMessage.toString());
        assertNotNull(clientMessage.senderID); // default dealer socket does not export sender ID (only ROUTER and/or enabled sockets)
        assertEquals(MdpSubProtocol.PROT_CLIENT, clientMessage.protocol, "equal protocol");
        assertEquals(FINAL, clientMessage.command, "matching command");
        assertArrayEquals(DEFAULT_ECHO_SERVICE.getBytes(UTF_8), clientMessage.serviceNameBytes, "equal service name");
        assertNotNull(clientMessage.data, "user-data not being null");
        assertArrayEquals(DEFAULT_REQUEST_MESSAGE_BYTES, clientMessage.data, "equal data");
        assertFalse(clientMessage.hasRbackToken());
        assertNotNull(clientMessage.rbacToken);
        assertEquals(0, clientMessage.rbacToken.length, "rback token length (should be 0: not defined)");

        broker.stopBroker();
    }

    @Test
    void basicSynchronousRequestReplyTest() throws IOException {
        MajordomoBroker broker = new MajordomoBroker("TestBroker", "", BasicRbacRole.values());
        // broker.setDaemon(true); // use this if running in another app that controls threads
        final int openPort = Utils.findOpenPort();
        broker.bind("tcp://*:" + openPort);
        broker.start();
        assertEquals(4, broker.getServices().size());

        // add external (albeit inproc) Majordomo worker to the broker
        BasicMdpWorker internal = new BasicMdpWorker(broker.getContext(), "inproc.echo", BasicRbacRole.ADMIN);
        internal.registerHandler(ctx -> ctx.rep.data = ctx.req.data); //  output = input : echo service is complex :-)
        internal.start();

        // add external Majordomo worker to the broker
        BasicMdpWorker external = new BasicMdpWorker(broker.getContext(), "ext.echo", BasicRbacRole.ADMIN);
        external.registerHandler(ctx -> ctx.rep.data = ctx.req.data); //  output = input : echo service is complex :-)
        external.start();

        // add external (albeit inproc) Majordomo worker to the broker
        BasicMdpWorker exceptionService = new BasicMdpWorker(broker.getContext(), "inproc.exception", BasicRbacRole.ADMIN);
        exceptionService.registerHandler(input -> { throw new IllegalAccessError("this is always thrown"); }); //  always throw an exception
        exceptionService.start();

        // wait until all services are initialised
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(200));
        assertEquals(7, broker.getServices().size());

        // using simple synchronous client
        MajordomoTestClientSync clientSession = new MajordomoTestClientSync("tcp://localhost:" + openPort, "customClientName");
        assertEquals(3, clientSession.getRetries());
        assertDoesNotThrow(() -> clientSession.setRetries(4));
        assertEquals(4, clientSession.getRetries());
        assertEquals(2500, clientSession.getTimeout());
        assertDoesNotThrow(() -> clientSession.setTimeout(2000));
        assertEquals(2000, clientSession.getTimeout());
        assertNotNull(clientSession.getUniqueID());

        {
            final String serviceName = "mmi.echo";
            final MdpMessage replyWithoutRbac = clientSession.send(serviceName, DEFAULT_REQUEST_MESSAGE_BYTES); // w/o RBAC
            assertNotNull(replyWithoutRbac, "reply message w/o RBAC token not being null");
            assertNotNull(replyWithoutRbac.data, "user-data not being null");
            assertArrayEquals(DEFAULT_REQUEST_MESSAGE_BYTES, replyWithoutRbac.data, "equal data");
        }

        {
            final String serviceName = "inproc.echo";
            final MdpMessage replyWithoutRbac = clientSession.send(serviceName, DEFAULT_REQUEST_MESSAGE_BYTES); // w/o RBAC
            assertNotNull(replyWithoutRbac, "reply message w/o RBAC token not being null");
            assertNotNull(replyWithoutRbac.data, "user-data not being null");
            assertArrayEquals(DEFAULT_REQUEST_MESSAGE_BYTES, replyWithoutRbac.data, "equal data");
        }

        {
            final String serviceName = "ext.echo";
            final MdpMessage replyWithoutRbac = clientSession.send(serviceName, DEFAULT_REQUEST_MESSAGE_BYTES); // w/o RBAC
            assertNotNull(replyWithoutRbac, "reply message w/o RBAC token not being null");
            assertNotNull(replyWithoutRbac.data, "user-data not being null");
            assertArrayEquals(DEFAULT_REQUEST_MESSAGE_BYTES, replyWithoutRbac.data, "equal data");
        }

        {
            final String serviceName = "inproc.exception";
            final MdpMessage replyWithoutRbac = clientSession.send(serviceName, DEFAULT_REQUEST_MESSAGE_BYTES); // w/o RBAC
            assertNotNull(replyWithoutRbac, "reply message w/o RBAC token not being null");
            assertNotNull(replyWithoutRbac.data, "user-data not being null");
            assertNotNull(replyWithoutRbac.errors, "user-data not being null");
            assertFalse(replyWithoutRbac.errors.isBlank(), "check that error stack trace is non-null/non-blank");
            final String refString = ANSI_RED + "io.opencmw.server.BasicMdpWorker caught exception for service 'inproc.exception'";
            assertEquals(refString, replyWithoutRbac.errors.substring(0, refString.length()), "correct exception message");
        }

        {
            final String serviceName = "mmi.echo";
            final MdpMessage replyWithRbac = clientSession.send(serviceName, DEFAULT_REQUEST_MESSAGE_BYTES, DEFAULT_RBAC_TOKEN); // with RBAC
            assertNotNull(replyWithRbac, "reply message with RBAC token not being null");
            assertNotNull(replyWithRbac.data, "user-data not being null");
            assertArrayEquals(DEFAULT_REQUEST_MESSAGE_BYTES, replyWithRbac.data, "equal data");
            assertNotNull(replyWithRbac.rbacToken, "RBAC token not being null");
            assertEquals(0, replyWithRbac.rbacToken.length, "non-defined RBAC token length");
        }

        internal.stopWorker();
        external.stopWorker();
        exceptionService.stopWorker();
        broker.stopBroker();
    }

    @Test
    void basicMmiTests() throws IOException {
        MajordomoBroker broker = new MajordomoBroker("TestBroker", "", BasicRbacRole.values());
        // broker.setDaemon(true); // use this if running in another app that controls threads
        final int openPort = Utils.findOpenPort();
        broker.bind("tcp://*:" + openPort);
        broker.start();

        // using simple synchronous client
        MajordomoTestClientSync clientSession = new MajordomoTestClientSync("tcp://localhost:" + openPort, "customClientName");

        {
            final MdpMessage replyWithoutRbac = clientSession.send("mmi.echo", DEFAULT_REQUEST_MESSAGE_BYTES); // w/o RBAC
            assertNotNull(replyWithoutRbac, "reply message w/o RBAC token not being null");
            assertNotNull(replyWithoutRbac.data, "user-data not being null");
            assertArrayEquals(DEFAULT_REQUEST_MESSAGE_BYTES, replyWithoutRbac.data, "MMI echo service request");
        }

        {
            final MdpMessage replyWithoutRbac = clientSession.send(DEFAULT_MMI_SERVICE, DEFAULT_MMI_SERVICE.getBytes(UTF_8)); // w/o RBAC
            assertNotNull(replyWithoutRbac, "reply message w/o RBAC token not being null");
            assertNotNull(replyWithoutRbac.data, "user-data not being null");
            assertEquals("200", new String(replyWithoutRbac.data, UTF_8), "known MMI service request");
        }

        {
            final MdpMessage replyWithoutRbac = clientSession.send(DEFAULT_MMI_SERVICE, DEFAULT_ECHO_SERVICE.getBytes(UTF_8)); // w/o RBAC
            assertNotNull(replyWithoutRbac, "reply message w/o RBAC token not being null");
            assertNotNull(replyWithoutRbac.data, "user-data not being null");
            assertEquals("200", new String(replyWithoutRbac.data, UTF_8), "known MMI service request");
        }

        {
            // MMI service request: service should not exist
            final MdpMessage replyWithoutRbac = clientSession.send(DEFAULT_MMI_SERVICE, DEFAULT_REQUEST_MESSAGE_BYTES); // w/o RBAC
            assertNotNull(replyWithoutRbac, "reply message w/o RBAC token not being null");
            assertNotNull(replyWithoutRbac.data, "user-data not being null");
            assertEquals("400", new String(replyWithoutRbac.data, UTF_8), "known MMI service request");
        }

        {
            // unknown service name
            final MdpMessage replyWithoutRbac = clientSession.send("unknownService", DEFAULT_REQUEST_MESSAGE_BYTES); // w/o RBAC
            assertNotNull(replyWithoutRbac, "reply message w/o RBAC token not being null");
            assertNotNull(replyWithoutRbac.data, "user-data not being null");
            assertEquals("501", new String(replyWithoutRbac.data, UTF_8), "unknown MMI service request");
        }

        broker.stopBroker();
    }

    @Test
    void basicASynchronousRequestReplyTest() throws IOException {
        MajordomoBroker broker = new MajordomoBroker("TestBroker", "", BasicRbacRole.values());
        // broker.setDaemon(true); // use this if running in another app that controls threads
        final int openPort = Utils.findOpenPort();
        broker.bind("tcp://*:" + openPort);
        broker.start();

        final AtomicInteger counter = new AtomicInteger(0);
        new Thread(() -> {
            // using simple synchronous client
            MajordomoTestClientAsync clientSession = new MajordomoTestClientAsync("tcp://localhost:" + openPort);
            assertEquals(2500, clientSession.getTimeout());
            assertDoesNotThrow(() -> clientSession.setTimeout(2000));
            assertEquals(2000, clientSession.getTimeout());

            // send bursts of 10 messages
            for (int i = 0; i < 5; i++) {
                clientSession.send("mmi.echo", DEFAULT_REQUEST_MESSAGE_BYTES);
                clientSession.send(DEFAULT_ECHO_SERVICE, DEFAULT_REQUEST_MESSAGE_BYTES);
            }

            // receive bursts of 10 messages
            for (int i = 0; i < 10; i++) {
                final MdpMessage reply = clientSession.recv();
                assertNotNull(reply, "reply message w/o RBAC token not being null");
                assertNotNull(reply.data, "user-data not being null");
                assertArrayEquals(DEFAULT_REQUEST_MESSAGE_BYTES, reply.data);
                counter.getAndIncrement();
            }
        }).start();

        await().alias("wait for reply messages").atMost(1, TimeUnit.SECONDS).until(counter::get, equalTo(10));
        assertEquals(10, counter.get(), "received expected number of replies");

        broker.stopBroker();
    }

    @Test
    void testSubscription() throws IOException {
        final MajordomoBroker broker = new MajordomoBroker("TestBroker", "", BasicRbacRole.values());
        // broker.setDaemon(true); // use this if running in another app that controls threads
        final String brokerAddress = broker.bind("mdp://*:" + Utils.findOpenPort());
        final String brokerPubAddress = broker.bind("mds://*:" + Utils.findOpenPort());
        broker.start();

        final String testServiceName = "device/property";
        final byte[] testServiceBytes = "device/property".getBytes(UTF_8);

        // add external (albeit inproc) Majordomo worker to the broker
        BasicMdpWorker internal = new BasicMdpWorker(broker.getContext(), testServiceName, BasicRbacRole.ADMIN);
        internal.registerHandler(ctx -> ctx.rep.data = ctx.req.data); //  output = input : echo service is complex :-)
        internal.start();

        final MdpMessage testMessage = new MdpMessage(null, PROT_WORKER, FINAL, testServiceBytes, "clientRequestID".getBytes(UTF_8), URI.create(new String(testServiceBytes)), DEFAULT_REQUEST_MESSAGE_BYTES, "", new byte[0]);

        final AtomicInteger counter = new AtomicInteger(0);
        final AtomicBoolean run = new AtomicBoolean(true);
        final AtomicBoolean started1 = new AtomicBoolean(false);
        new Thread(() -> {
            // using simple synchronous client
            MajordomoTestClientAsync clientSession = new MajordomoTestClientAsync(brokerAddress);
            assertEquals(2500, clientSession.getTimeout());
            assertDoesNotThrow(() -> clientSession.setTimeout(2000));
            assertEquals(2000, clientSession.getTimeout());
            clientSession.subscribe(testServiceBytes);

            // send bursts of 10 messages
            for (int i = 0; i < 10 && run.get(); i++) {
                started1.set(true);
                final MdpMessage reply = clientSession.recv();
                assertNotNull(reply, "reply message w/o RBAC token not being null");
                assertNotNull(reply.data, "user-data not being null");
                assertArrayEquals(DEFAULT_REQUEST_MESSAGE_BYTES, reply.data);
                counter.getAndIncrement();
            }
        }).start();

        // low-level subscription
        final AtomicInteger subCounter = new AtomicInteger(0);
        final AtomicBoolean started2 = new AtomicBoolean(false);
        final Thread subcriptionThread = new Thread(() -> {
            // low-level subscription
            final ZMQ.Socket sub = broker.getContext().createSocket(SocketType.SUB);
            sub.setHWM(0);
            sub.connect(brokerPubAddress.replace("mds://", "tcp://"));
            sub.subscribe("device/property");
            sub.subscribe("device/otherProperty");
            sub.unsubscribe("device/otherProperty");
            while (run.get() && !Thread.interrupted()) {
                started2.set(true);
                final ZMsg msg = ZMsg.recvMsg(sub, false);
                if (msg == null) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                    continue;
                }
                final int count = subCounter.getAndIncrement();
                // System.err.println("received subscription #" + count + ": " + msg)
            }
            sub.unsubscribe("device/property");
        });
        subcriptionThread.start();

        // wait until all services are initialised
        await().alias("wait for thread1 to start").atMost(1, TimeUnit.SECONDS).until(started1::get, equalTo(true));
        await().alias("wait for thread2 to start").atMost(1, TimeUnit.SECONDS).until(started2::get, equalTo(true));
        // send bursts of 10 messages
        for (int i = 0; i < 10; i++) {
            internal.notify(testMessage);
        }

        await().alias("wait for reply messages").atMost(2, TimeUnit.SECONDS).until(counter::get, equalTo(10));
        run.set(false);
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
        assertFalse(subcriptionThread.isAlive(), "subscription thread shut-down");
        assertEquals(10, counter.get(), "received expected number of replies");
        assertEquals(10, subCounter.get(), "received expected number of subscription replies");

        broker.stopBroker();
    }
}
