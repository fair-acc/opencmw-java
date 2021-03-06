package io.opencmw.client.cmwlight;

import static org.junit.jupiter.api.Assertions.*;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.LockSupport;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.*;

@Timeout(20)
class CmwLightDataSourceTest {
    final static Logger LOGGER = LoggerFactory.getLogger(CmwLightDataSourceTest.class);
    @Test
    void testCmwLightSubscription() throws CmwLightProtocol.RdaLightException, URISyntaxException {
        // setup zero mq socket to mock cmw server
        try (final ZContext context = new ZContext(1)) {
            ZMQ.Socket socket = context.createSocket(SocketType.DEALER);
            socket.bind("tcp://localhost:7777");

            final CmwLightDataSource client = new CmwLightDataSource(context, new URI("rda3://localhost:7777/testdevice/testprop?ctx=test.selector&nFilter=int:1"), "testClientId");

            client.connect();
            client.housekeeping();

            // check connection request was received
            final CmwLightMessage connectMsg = CmwLightProtocol.parseMsg(ZMsg.recvMsg(socket));
            assertEquals(CmwLightProtocol.MessageType.CLIENT_CONNECT, connectMsg.messageType);
            assertEquals(CmwLightProtocol.VERSION, connectMsg.version);
            client.housekeeping(); // allow the subscription to be sent out

            // send connection ack
            CmwLightProtocol.sendMsg(socket, CmwLightMessage.connectAck("1.3.7"));
            CmwLightProtocol.sendMsg(socket, CmwLightMessage.SERVER_HB);
            client.getMessage(); // Make client receive ack and update connection status
            client.housekeeping(); // allow the subscription to be sent out

            // assert that the client has connected
            Awaitility.await().atMost(Duration.ofSeconds(2)).until(() -> {
                client.getMessage(); // Make client receive ack and update connection status
                client.housekeeping(); // allow the subscription to be sent out
                return client.connectionState.get().equals(CmwLightDataSource.ConnectionState.CONNECTED);
            });

            // request subscription
            final String reqId = "testId";
            final URI endpoint = new URI("rda3://localhost:7777/testdevice/testprop?ctx=FAIR.SELECTOR.ALL&nFilter=int:1");
            client.subscribe(reqId, endpoint, null);

            final CmwLightMessage subMsg = getNextNonHeartbeatMsg(socket, client, false);
            assertEquals(CmwLightProtocol.MessageType.CLIENT_REQ, subMsg.messageType);
            assertEquals(CmwLightProtocol.RequestType.SUBSCRIBE, subMsg.requestType);
            assertEquals(Map.of("nFilter", 1), subMsg.requestContext.filters);

            // acknowledge subscription
            final long sourceId = 1337L;
            CmwLightProtocol.sendMsg(socket, CmwLightMessage.subscribeReply(subMsg.sessionId, subMsg.id, subMsg.deviceName, subMsg.propertyName, Map.of(CmwLightProtocol.FieldName.SOURCE_ID_TAG.value(), sourceId)));

            // assert that the subscription was established
            Awaitility.await().atMost(Duration.ofSeconds(2)).until(() -> {
                client.getMessage(); // Make client receive ack and update connection status
                client.housekeeping(); // allow the subscription to be sent out
                return client.replyIdMap.containsKey(sourceId);
            });

            // send 10 updates
            for (int i = 0; i < 10; i++) {
                final String cycleName = "FAIR.SELECTOR.C=" + (i + 1);
                CmwLightProtocol.sendMsg(socket, CmwLightMessage.notificationReply(subMsg.sessionId, sourceId, "", "", new ZFrame("data"), i,
                                                         new CmwLightMessage.DataContext(cycleName, 123456789, 123456788, null), CmwLightProtocol.UpdateType.NORMAL));

                // assert that the subscription update was received
                Awaitility.await().atMost(Duration.ofSeconds(2)).until(() -> {
                    final ZMsg reply = client.getMessage(); // Make client receive ack and update connection status
                    client.housekeeping(); // allow the subscription to be sent out

                    return reply.size() == 4 && reply.pollFirst().getString(Charset.defaultCharset()).equals("testId")
                            && Objects.requireNonNull(reply.pollFirst()).getString(Charset.defaultCharset()).equals(new CmwLightDataSource.ParsedEndpoint(endpoint, cycleName).toURI().toString())
                            && Objects.requireNonNull(reply.pollFirst()).getString(Charset.defaultCharset()).equals("data")
                            && Objects.requireNonNull(reply.pollFirst()).getData().length == 0;
                });
            }
        }
    }

    /*
    / get next message sent from client to server ignoring heartbeats, periodically send heartbeat and perform housekeeping
    */
    private CmwLightMessage getNextNonHeartbeatMsg(final ZMQ.Socket socket, final CmwLightDataSource client, boolean debug) throws CmwLightProtocol.RdaLightException {
        int i = 0;
        while (true) {
            final ZMsg msg = ZMsg.recvMsg(socket, false);
            final CmwLightMessage result = msg == null ? null : CmwLightProtocol.parseMsg(msg);
            if (debug) {
                if (result == null) {
                    System.out.print('.');
                } else {
                    System.out.println(result);
                }
            }
            if (result != null && result.messageType != CmwLightProtocol.MessageType.CLIENT_HB) {
                return result;
            }
            if (i % 10 == 0) { // send server heartbeat every second
                CmwLightProtocol.sendMsg(socket, CmwLightMessage.SERVER_HB);
            }
            client.housekeeping();
            client.getMessage();
            LockSupport.parkNanos(100000);
            i++;
        }
    }

    @Test
    void testParsedEndpoint() throws URISyntaxException, CmwLightProtocol.RdaLightException {
        final String refDevice = "deviceA";
        final String refProperty = "MyProperty";
        final String refPath = '/' + refDevice + '/' + refProperty;
        final String testAuthority = "server:1337";
        final Map<String, Object> filterMap = Map.of("filterA", 3, "filterB", true, "filterC", "foo=bar", "filterD", 1234567890987654321L, "filterE", 1.5, "filterF", -3.5f);
        final String testQuery = "ctx=Test.Context.C=5&filterA=int:3&filterB=bool:true&filterC=foo=bar&filterD=long:1234567890987654321&filterE=double:1.5&filterF=float:-3.5";
        final URI testUri1 = new URI("rda3", testAuthority, refPath, testQuery, null);
        final URI testUri2 = new URI("rda3", null, refPath, testQuery, null);

        final CmwLightDataSource.ParsedEndpoint parsed1 = new CmwLightDataSource.ParsedEndpoint(testUri1);
        assertEquals(testAuthority, parsed1.authority);
        assertEquals(refDevice, parsed1.device);
        assertEquals(refProperty, parsed1.property);
        final CmwLightDataSource.ParsedEndpoint parsed2 = new CmwLightDataSource.ParsedEndpoint(testUri2);
        assertNull(parsed2.authority);
        assertEquals(refDevice, parsed2.device);
        assertEquals(refProperty, parsed2.property);

        assertEquals(parsed1, parsed1);
        assertNotEquals(parsed1, new Object());
        assertEquals(testUri1, parsed1.toURI());
        assertNotEquals(testUri1, parsed2.toURI()); // since testURI2 has no authority given
        assertEquals(parsed1, parsed2);
        assertEquals(parsed1.hashCode(), parsed2.hashCode());

        // simple test for faulty sub-property definition - fail early
        final URI faultyTestUri = new URI("rda3", "server:1337", "/deviceA/MyProperty/SubProperty", "filterA=short:3", null);
        assertThrows(CmwLightProtocol.RdaLightException.class, () -> new CmwLightDataSource.ParsedEndpoint(faultyTestUri).toURI());
    }
}
