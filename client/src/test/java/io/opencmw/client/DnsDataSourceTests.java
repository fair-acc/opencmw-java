package io.opencmw.client;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
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
class DnsDataSourceTests {
    private final static int TIMEOUT = 1000; // [ms]
    private static MajordomoBroker dnsBroker;
    private static MajordomoBroker brokerB;
    private static MajordomoBroker brokerC;
    private static URI dnsBrokerAddress;

    @BeforeAll
    static void init() throws IOException {
        System.setProperty(OpenCmwConstants.HEARTBEAT, "100"); // to reduce waiting time for changes
        dnsBroker = getTestBroker("dnsBroker", "deviceA/property", null);
        dnsBrokerAddress = dnsBroker.bind(URI.create("mdp://*:" + Utils.findOpenPort()));
        brokerB = getTestBroker("CustomBrokerB", "deviceB/property", dnsBrokerAddress);
        brokerC = getTestBroker("CustomBrokerC", "deviceC/property", dnsBrokerAddress);
        LockSupport.parkNanos(Duration.ofMillis(500).toNanos());
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
            final Map<String, List<URI>> dnsMapAll = parseDnsReply(dnsResult);
            assertFalse(dnsMapAll.isEmpty());
            assertEquals(3, dnsMapAll.size(), "number of brokers");

            final List<String> queryDevice = Arrays.asList("mds:/deviceB", "deviceA", "/deviceA", "mds:/deviceC", "dnsBroker", "unknown");
            final String query = String.join(",", queryDevice);
            final Future<BinaryData> reply4 = client.get(URI.create(dnsBrokerAddress + "/mmi.dns?" + query), null, BinaryData.class);
            final byte[] dnsSpecificResult = reply4.get(TIMEOUT, TimeUnit.MILLISECONDS).data;
            assertNotNull(dnsSpecificResult);
            final Map<String, List<URI>> dnsMapSelective = parseDnsReply(dnsSpecificResult);
            assertFalse(dnsMapAll.isEmpty());
            assertEquals(queryDevice.size(), dnsMapSelective.size(), "number of returned dns resolves");
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
            assertEquals("deviceA/property,dnsBroker/mmi.dns,dnsBroker/mmi.echo,dnsBroker/mmi.openapi,dnsBroker/mmi.service", ZData.toString(reply2.get(TIMEOUT, TimeUnit.MILLISECONDS).data));

            final Future<BinaryData> reply3 = client.get(URI.create(dnsBrokerAddress + "/dnsBroker/mmi.service"), null, BinaryData.class);
            assertEquals("deviceA/property,dnsBroker/mmi.dns,dnsBroker/mmi.echo,dnsBroker/mmi.openapi,dnsBroker/mmi.service", ZData.toString(reply3.get(TIMEOUT, TimeUnit.MILLISECONDS).data));
        }
    }

    @AfterAll
    static void finish() {
        brokerC.stopBroker();
        brokerB.stopBroker();
        dnsBroker.stopBroker();
    }

    public static Map<String, List<URI>> parseDnsReply(final byte[] dnsReply) {
        final HashMap<String, List<URI>> map = new HashMap<>();
        if (dnsReply == null || dnsReply.length == 0 || !isUTF8(dnsReply)) {
            return map;
        }
        final String reply = new String(dnsReply, UTF_8);
        if (reply.isBlank()) {
            return map;
        }

        // parse reply
        Pattern dnsPattern = Pattern.compile("\\[(.*?)]"); // N.B. need only one instance of this
        final Matcher matchPattern = dnsPattern.matcher(reply);
        while (matchPattern.find()) {
            final String device = matchPattern.group(1);
            final String[] message = device.split("(: )", 2);
            assert message.length == 2 : "could not split into 2 segments: " + device;
            final List<URI> uriList = map.computeIfAbsent(message[0], deviceName -> new ArrayList<>());
            for (String uriString : StringUtils.split(message[1], ",")) {
                if (!"null".equalsIgnoreCase(uriString)) {
                    try {
                        uriList.add(new URI(StringUtils.strip(uriString)));
                    } catch (final URISyntaxException e) {
                        System.err.println("could not parse device '" + message[0] + "' uri: '" + uriString + "' cause: " + e);
                    }
                }
            }
        }

        return map;
    }

    public static boolean isUTF8(byte[] array) {
        final CharsetDecoder decoder = UTF_8.newDecoder();
        final ByteBuffer buf = ByteBuffer.wrap(array);
        try {
            decoder.decode(buf);
        } catch (CharacterCodingException e) {
            return false;
        }
        return true;
    }

    static MajordomoBroker getTestBroker(final String brokerName, final String devicePropertyName, final URI dnsServer) throws IOException {
        final MajordomoBroker broker = new MajordomoBroker(brokerName, dnsServer, BasicRbacRole.values());
        final URI privateBrokerAddress = broker.bind(URI.create("mdp://*:" + Utils.findOpenPort())); // not directly visible by clients
        final URI privateBrokerPubAddress = broker.bind(URI.create("mds://*:" + Utils.findOpenPort())); // not directly visible by clients
        assertNotNull(privateBrokerAddress, "private broker address for " + brokerName);
        assertNotNull(privateBrokerPubAddress, "private broker address for " + brokerName);

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
