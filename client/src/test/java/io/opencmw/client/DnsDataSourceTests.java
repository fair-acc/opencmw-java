package io.opencmw.client;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.zeromq.util.ZData;

import io.opencmw.OpenCmwConstants;
import io.opencmw.domain.BinaryData;
import io.opencmw.domain.NoData;
import io.opencmw.rbac.BasicRbacRole;
import io.opencmw.server.MajordomoBroker;
import io.opencmw.server.MajordomoWorker;

import zmq.util.Utils;

/**
 * Basic DNS DataSourcePublisher tests - to be further augmented with actual DNS resolver implementation (presently missing)
 *
 * @author rstein
 */
@Timeout(60)
class DnsDataSourceTests {
    private final static int TIMEOUT_STARTUP = 5; // [s]
    private final static int TIMEOUT = 1000; // [ms]
    private static MajordomoBroker dnsBroker;
    private static MajordomoBroker brokerB;
    private static MajordomoBroker brokerC;
    private static URI dnsBrokerAddress;
    private static OpenCmwDnsResolver openCmwResolver;

    private void launchSubscription(final DataSourcePublisher.Client client, final AtomicInteger notificationCounterA, final URI subscriptionURI) {
        client.subscribe(subscriptionURI, DomainData.class, null, NoData.class, new DataSourcePublisher.NotificationListener<>() {
            @Override
            public void dataUpdate(final DomainData updatedObject, final NoData contextObject) {
                notificationCounterA.getAndIncrement();
            }

            @Override
            public void updateException(final Throwable exception) {
                fail("subscription exception occurred ", exception);
            }
        });
    }

    @Test
    void testBasicResolver() throws InterruptedException, ExecutionException, TimeoutException {
        // uses only device info
        try (DataSourcePublisher dataSource = new DataSourcePublisher(null, null, "test-client");
                DataSourcePublisher.Client client = dataSource.getClient()) {
            Future<DomainData> reply1 = client.get(URI.create(dnsBrokerAddress + "/deviceA/property"), null, DomainData.class);
            assertEquals("deviceA/property", reply1.get(TIMEOUT, TimeUnit.MILLISECONDS).data);

            final Future<BinaryData> reply2 = client.get(URI.create(dnsBrokerAddress + "/mmi.service"), null, BinaryData.class);
            assertEquals("deviceA/property,dnsBroker/mmi.dns,dnsBroker/mmi.echo,dnsBroker/mmi.openapi,dnsBroker/mmi.service", ZData.toString(reply2.get(TIMEOUT, TimeUnit.MILLISECONDS).data));

            final Future<BinaryData> reply3 = client.get(URI.create(dnsBrokerAddress + "/mmi.dns"), null, BinaryData.class);
            final byte[] dnsResult = reply3.get(TIMEOUT, TimeUnit.MILLISECONDS).data;
            assertNotNull(dnsResult);
            final Map<URI, List<URI>> dnsMapAll = OpenCmwDnsResolver.parseDnsReply(dnsResult);
            assertFalse(dnsMapAll.isEmpty());
            assertEquals(3, dnsMapAll.size(), "number of brokers");

            final List<String> queryDevice = Arrays.asList("mds:/deviceB", "deviceA", "/deviceA", "mds:/deviceC", "dnsBroker", "unknown");
            final String query = String.join(",", queryDevice);
            final Future<BinaryData> reply4 = client.get(URI.create(dnsBrokerAddress + "/mmi.dns?" + query), null, BinaryData.class);
            final byte[] dnsSpecificResult = reply4.get(TIMEOUT, TimeUnit.MILLISECONDS).data;
            assertNotNull(dnsSpecificResult);
            final Map<URI, List<URI>> dnsMapSelective = OpenCmwDnsResolver.parseDnsReply(dnsSpecificResult);
            assertFalse(dnsMapSelective.isEmpty());
            assertEquals(queryDevice.size(), dnsMapSelective.size(), "number of returned dns resolves");
        }
    }

    @Test
    void testOpenCmwDnsResolver() throws Exception {
        final List<URI> queryDevice = Stream.of("mds:/deviceB", "deviceA", "/deviceA", "mds:/deviceC", "dnsBroker", "unknown").map(URI::create).collect(Collectors.toList());
        final Map<URI, List<URI>> dnsMapSelective = openCmwResolver.resolveNames(queryDevice);
        assertFalse(dnsMapSelective.isEmpty());
        assertEquals(queryDevice.size(), dnsMapSelective.size(), "number of returned dns resolves");

        checkNumberOfEndPoints(dnsMapSelective, 1, "mds:/deviceB");
        checkNumberOfEndPoints(dnsMapSelective, 1, "mds:/deviceB");
        checkNumberOfEndPoints(dnsMapSelective, 3, "deviceA"); // two private entries (mdp & mds), one public mdp
        checkNumberOfEndPoints(dnsMapSelective, 3, "/deviceA"); // two private entries (mdp & mds), one public mdp
        checkNumberOfEndPoints(dnsMapSelective, 1, "mds:/deviceC");
        checkNumberOfEndPoints(dnsMapSelective, 12, "dnsBroker");
        checkNumberOfEndPoints(dnsMapSelective, 0, "unknown");
    }

    @Test
    void testWithoutResolver() throws InterruptedException, ExecutionException, TimeoutException {
        // uses full/known URI definition
        try (DataSourcePublisher dataSource = new DataSourcePublisher(null, null, "test-client");
                DataSourcePublisher.Client client = dataSource.getClient()) {
            Future<DomainData> reply1 = client.get(URI.create(dnsBrokerAddress + "/deviceA/property"), null, DomainData.class);
            assertEquals("deviceA/property", reply1.get(TIMEOUT, TimeUnit.MILLISECONDS).data);

            final Future<BinaryData> reply2 = client.get(URI.create(dnsBrokerAddress + "/mmi.service"), null, BinaryData.class);
            assertEquals("deviceA/property,dnsBroker/mmi.dns,dnsBroker/mmi.echo,dnsBroker/mmi.openapi,dnsBroker/mmi.service", ZData.toString(reply2.get(TIMEOUT, TimeUnit.MILLISECONDS).data));

            final Future<BinaryData> reply3 = client.get(URI.create(dnsBrokerAddress + "/dnsBroker/mmi.service"), null, BinaryData.class);
            assertEquals("deviceA/property,dnsBroker/mmi.dns,dnsBroker/mmi.echo,dnsBroker/mmi.openapi,dnsBroker/mmi.service", ZData.toString(reply3.get(TIMEOUT, TimeUnit.MILLISECONDS).data));
        }
    }

    @Test
    void testWithResolver() throws InterruptedException, ExecutionException, TimeoutException {
        DataSource.getFactory(URI.create("mdp:/mmi.dns")).registerDnsResolver(new OpenCmwDnsResolver(dnsBrokerAddress));
        // uses full/known URI definition
        try (DataSourcePublisher dataSource = new DataSourcePublisher(null, null, "test-client");
                DataSourcePublisher.Client client = dataSource.getClient()) {
            Future<DomainData> reply1 = client.get(URI.create("mdp:/deviceA/property"), null, DomainData.class);
            assertEquals("deviceA/property", reply1.get(TIMEOUT, TimeUnit.MILLISECONDS).data);

            final Future<BinaryData> reply2 = client.get(URI.create("mdp:/mmi.service"), null, BinaryData.class);
            assertEquals("deviceA/property,dnsBroker/mmi.dns,dnsBroker/mmi.echo,dnsBroker/mmi.openapi,dnsBroker/mmi.service", ZData.toString(reply2.get(TIMEOUT, TimeUnit.MILLISECONDS).data));

            final Future<BinaryData> reply3 = client.get(URI.create("mdp:/dnsBroker/mmi.service"), null, BinaryData.class);
            assertEquals("deviceA/property,dnsBroker/mmi.dns,dnsBroker/mmi.echo,dnsBroker/mmi.openapi,dnsBroker/mmi.service", ZData.toString(reply3.get(TIMEOUT, TimeUnit.MILLISECONDS).data));
        }
    }

    @Test
    void testWithWorkerStartStopping() throws IOException {
        DataSource.getFactory(URI.create("mdp:/mmi.dns")).registerDnsResolver(new OpenCmwDnsResolver(dnsBrokerAddress));
        System.setProperty(OpenCmwConstants.RECONNECT_THRESHOLD1, "10000"); // to reduce waiting time for reconnects
        System.setProperty(OpenCmwConstants.RECONNECT_THRESHOLD2, "1000"); // to reduce waiting time for reconnects

        try (DataSourcePublisher dataSource = new DataSourcePublisher(null, null, "test-client");
                DataSourcePublisher.Client client = dataSource.getClient()) {
            AtomicInteger notificationCounterA = new AtomicInteger();
            AtomicInteger notificationCounterD = new AtomicInteger();
            launchSubscription(client, notificationCounterA, URI.create("mds:/deviceA/property"));
            launchSubscription(client, notificationCounterD, URI.create("mds:/deviceD/property"));

            await().alias("subscribe and receive from an existing 'deviceA/property'").atMost(Duration.ofSeconds(TIMEOUT_STARTUP)).until(() -> notificationCounterA.get() >= 10);
            assertNotEquals(0, notificationCounterA.get());

            final MajordomoBroker brokerD = getTestBroker("CustomBrokerD", "deviceD/property", dnsBrokerAddress);
            assertNotNull(brokerD, "new brokerD is not running");
            await().alias("wait for all CustomBrokerD services to report in").atMost(Duration.ofSeconds(TIMEOUT_STARTUP)).until(() -> providedServices("CustomBrokerD", "deviceD", 2));
            assertTrue(providedServices("CustomBrokerD", "deviceD", 2), "check that all required CustomBrokerD services have reported in");
            // brokerD started

            await().alias("subscribe and receive from an existing 'deviceD/property' - first stage").atMost(Duration.ofSeconds(TIMEOUT_STARTUP)).until(() -> notificationCounterD.get() >= 10);
            assertNotEquals(0, notificationCounterD.get());
            brokerD.stopBroker();
            //LockSupport.parkNanos(Duration.ofMillis(TIMEOUT).toNanos()); // wait until the old brokerD and connected services have shut down
            // reset counter
            dnsBroker.getDnsCache().clear();
            notificationCounterD.set(0);
            assertEquals(0, notificationCounterD.get(), "NotificationListener not acquiring any more new events");

            final MajordomoBroker newBrokerD = getTestBroker("CustomBrokerD", "deviceD/property", dnsBrokerAddress);
            assertNotNull(newBrokerD, "new brokerD is not running");
            await().alias("wait for all CustomBrokerD services to report in").atMost(Duration.ofSeconds(TIMEOUT_STARTUP)).until(() -> providedServices("CustomBrokerD", "deviceD", 2));
            assertTrue(providedServices("CustomBrokerD", "deviceD", 2), "check that all required CustomBrokerD services have reported in");
            // brokerD started with new port

            await().alias("subscribe and receive from an existing 'deviceD/property' - second stage").atMost(Duration.ofSeconds(TIMEOUT_STARTUP)).until(() -> notificationCounterD.get() >= 10);
            assertNotEquals(0, notificationCounterD.get());

            // finished test
            newBrokerD.stopBroker();
        }
    }

    static boolean providedServices(final @NotNull String brokerName, final @NotNull String serviceName, final int requiredEndpoints) {
        return dnsBroker.getDnsCache().get(brokerName).getUri().stream().filter(s -> s != null && s.toString().contains(serviceName)).count() == requiredEndpoints;
    }

    static void checkNumberOfEndPoints(final Map<URI, List<URI>> dnsMapSelective, final int expected, final String endpointName) {
        final URI uri = URI.create(endpointName);
        final List<URI> list = dnsMapSelective.get(uri);
        assertEquals(expected, list.size(), endpointName + " available endpoints: " + list);
    }

    @BeforeAll
    @Timeout(10)
    static void init() throws IOException {
        System.setProperty(OpenCmwConstants.HEARTBEAT, "100"); // to reduce waiting time for changes
        dnsBroker = getTestBroker("dnsBroker", "deviceA/property", null);
        dnsBrokerAddress = dnsBroker.bind(URI.create("mdp://*:" + Utils.findOpenPort()));
        openCmwResolver = new OpenCmwDnsResolver(dnsBroker.getContext(), dnsBrokerAddress, Duration.ofMillis(TIMEOUT));
        brokerB = getTestBroker("CustomBrokerB", "deviceB/property", dnsBrokerAddress);
        brokerC = getTestBroker("CustomBrokerC", "deviceC/property", dnsBrokerAddress);
        await().atMost(Duration.ofSeconds(TIMEOUT_STARTUP)).until(() -> dnsBroker.getDnsCache().size() == 3);
        assertEquals(3, dnsBroker.getDnsCache().size(), "reported: " + String.join(",", dnsBroker.getDnsCache().keySet()));
        await().alias("wait for all CustomBrokerB services to report in").atMost(Duration.ofSeconds(TIMEOUT_STARTUP)).until(() -> providedServices("CustomBrokerB", "deviceB", 2));
        await().alias("wait for all CustomBrokerC services to report in").atMost(Duration.ofSeconds(TIMEOUT_STARTUP)).until(() -> providedServices("CustomBrokerC", "deviceC", 2));
        await().alias("wait for all dnsBroker services to report in").atMost(Duration.ofSeconds(TIMEOUT_STARTUP)).until(() -> providedServices("dnsBroker", "deviceA", 3));

        assertTrue(providedServices("dnsBroker", "deviceA", 3), "check that all required dnsBroker services have reported in");
        assertTrue(providedServices("CustomBrokerB", "deviceB", 2), "check that all required CustomBrokerB services have reported in");
        assertTrue(providedServices("CustomBrokerC", "deviceC", 2), "check that all required CustomBrokerC services have reported in");

        LockSupport.parkNanos(Duration.ofMillis(TIMEOUT).toNanos()); // wait until brokers and DNS client have been initialised
    }

    @AfterAll
    @Timeout(10)
    static void finish() {
        // delete previous resolvers
        DataSource.getFactory(URI.create("mdp:/mmi.dns")).getRegisteredDnsResolver().clear();
        openCmwResolver.close();
        brokerC.stopBroker();
        brokerB.stopBroker();
        dnsBroker.stopBroker();
    }

    static MajordomoBroker getTestBroker(final String brokerName, final String devicePropertyName, final URI dnsServer) throws IOException {
        final MajordomoBroker broker = new MajordomoBroker(brokerName, dnsServer, BasicRbacRole.values());
        final URI privateBrokerAddress = broker.bind(URI.create("mdp://*:" + Utils.findOpenPort())); // not directly visible by clients
        final URI privateBrokerPubAddress = broker.bind(URI.create("mds://*:" + Utils.findOpenPort())); // not directly visible by clients
        assertNotNull(privateBrokerAddress, "private broker address for " + brokerName);
        assertNotNull(privateBrokerPubAddress, "private broker address for " + brokerName);

        final MajordomoWorker<NoData, NoData, DomainData> worker = new MajordomoWorker<>(broker.getContext(), devicePropertyName, NoData.class, NoData.class, DomainData.class);
        worker.setHandler((raw, reqCtx, req, repCtx, rep) -> rep.data = devicePropertyName); // simple property returning the <device>/<property> description
        final Timer timer = new Timer("NotifyTimer-" + devicePropertyName, true);
        final NoData noData = new NoData();
        final DomainData domainData = new DomainData();
        domainData.data = devicePropertyName;
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    worker.notify(noData, domainData);
                } catch (Exception e) { // NOPMD
                    fail("exception in notify");
                }
            }
        }, 0, 100);
        broker.addInternalService(worker);
        broker.start();
        return broker;
    }

    public static class DomainData {
        public String data;
    }
}
