package io.opencmw.client;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.zeromq.util.ZData;

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
class DnsDataSourceTests {
    private final static int TIMEOUT = 1000; // [ms]
    private static MajordomoBroker dnsBroker;
    private static MajordomoBroker brokerB;
    private static MajordomoBroker brokerC;
    private static URI dnsBrokerAddress;

    @BeforeAll
    static void init() throws IOException {
        dnsBroker = getTestBroker("dnsBroker", "deviceA/property", null);
        dnsBrokerAddress = dnsBroker.bind(URI.create("mdp://*:" + Utils.findOpenPort()));
        brokerB = getTestBroker("CustomBrokerB", "deviceB/property", dnsBrokerAddress);
        brokerC = getTestBroker("CustomBrokerC", "deviceC/property", dnsBrokerAddress);
        LockSupport.parkNanos(Duration.ofMillis(1000).toNanos());
    }

    @Test
    void testBasicResolver() throws InterruptedException, ExecutionException, TimeoutException {
        // uses only device info
        try (DataSourcePublisher dataSource = new DataSourcePublisher(null, null, "test-client");
                DataSourcePublisher.Client client = dataSource.getClient()) {
            Future<DomainData> reply1 = client.get(URI.create(dnsBrokerAddress + "/deviceA/property"), null, DomainData.class);
            assertEquals("deviceA/property", reply1.get(TIMEOUT, TimeUnit.MILLISECONDS).data);

            final Future<BinaryData> reply2 = client.get(URI.create(dnsBrokerAddress + "/mmi.service"), null, BinaryData.class);
            assertEquals("deviceA/property,mmi.dns,mmi.echo,mmi.openapi,mmi.service", ZData.toString(reply2.get(TIMEOUT, TimeUnit.MILLISECONDS).data));

            final Future<BinaryData> reply3 = client.get(URI.create(dnsBrokerAddress + "/mmi.dns"), null, BinaryData.class);
            assertEquals("", ZData.toString(reply3.get(TIMEOUT, TimeUnit.MILLISECONDS).data));
        }
    }

    @Test
    void testWithoutResolver() throws InterruptedException, ExecutionException, TimeoutException {
        // uses full/known URI definition
        try (DataSourcePublisher dataSource = new DataSourcePublisher(null, null, "test-client");
                DataSourcePublisher.Client client = dataSource.getClient()) {
            Future<DomainData> reply1 = client.get(URI.create(dnsBrokerAddress + "/deviceA/property"), null, DomainData.class);
            assertEquals("deviceA/property", reply1.get(TIMEOUT, TimeUnit.MILLISECONDS).data);

            final Future<BinaryData> reply2 = client.get(URI.create(dnsBrokerAddress + "/mmi.service"), null, BinaryData.class);
            assertEquals("deviceA/property,mmi.dns,mmi.echo,mmi.openapi,mmi.service", ZData.toString(reply2.get(TIMEOUT, TimeUnit.MILLISECONDS).data));

            final Future<BinaryData> reply3 = client.get(URI.create(dnsBrokerAddress + "/dnsBroker/mmi.service"), null, BinaryData.class);
            assertEquals("deviceA/property,mmi.dns,mmi.echo,mmi.openapi,mmi.service", ZData.toString(reply3.get(TIMEOUT, TimeUnit.MILLISECONDS).data));
        }
    }

    @AfterAll
    static void finish() {
        brokerC.stopBroker();
        brokerB.stopBroker();
        dnsBroker.stopBroker();
    }

    static MajordomoBroker getTestBroker(final String brokerName, final String devicePropertyName, final URI dnsServer) {
        final MajordomoBroker broker = new MajordomoBroker(brokerName, dnsServer, BasicRbacRole.values());

        final MajordomoWorker<NoData, NoData, DomainData> worker = new MajordomoWorker<>(broker.getContext(), devicePropertyName, NoData.class, NoData.class, DomainData.class);
        worker.setHandler((raw, reqCtx, req, repCtx, rep) -> rep.data = devicePropertyName); // simple property returning the <device>/<property> description
        broker.addInternalService(worker);
        broker.start();
        return broker;
    }

    public static class DomainData {
        public String data;
    }
}
