package io.opencmw.client.cmwlight;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import static io.opencmw.OpenCmwProtocol.EMPTY_FRAME;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.zeromq.*;

import io.opencmw.QueryParameterParser;
import io.opencmw.client.DataSource;
import io.opencmw.client.DnsResolver;
import io.opencmw.serialiser.spi.CmwLightSerialiser;

@Timeout(20)
class CmwLightDataSourceTest {
    private static final DnsResolver MOCK_CMWLIGHT_RESOLVER = new DnsResolver() {
        final private Map<URI, List<URI>> hosts = Map.of(URI.create("rda3:/testService"), List.of(URI.create("rda3://localhost:7777/testService")));

        @Override
        public List<String> getApplicableSchemes() {
            return CmwLightDataSource.FACTORY.getApplicableSchemes();
        }

        @Override
        public Map<URI, List<URI>> resolveNames(final List<URI> devicesToResolve) {
            return devicesToResolve.stream().filter(hosts::containsKey).collect(Collectors.toMap(Function.identity(), hosts::get));
        }

        @Override
        public void close() throws Exception {
            // nothing to do here
        }
    };

    @Test
    void testCmwLightSubscription() throws CmwLightProtocol.RdaLightException, URISyntaxException, IOException {
        // setup zero mq socket to mock cmw server
        try (final ZContext context = new ZContext(1); ZMQ.Socket socket = context.createSocket(SocketType.DEALER)) {
            socket.bind("tcp://localhost:7777");

            assertThrows(UnsupportedOperationException.class, () -> CmwLightDataSource.FACTORY.newInstance(context, URI.create("http://server/device/property?query=test"), Duration.ofSeconds(1), Executors.newSingleThreadExecutor(), "testclient"));
            final CmwLightDataSource client = (CmwLightDataSource) assertDoesNotThrow(() -> CmwLightDataSource.FACTORY.newInstance(context, URI.create("rda3://localhost:7777/testdevice/testprop?ctx=test.selector&nFilter=int:1"), Duration.ofSeconds(1), Executors.newCachedThreadPool(), "testClientId"));
            client.connect();
            client.housekeeping();

            assertDoesNotThrow(() -> client.registerDnsResolver(new DirectoryLightClient("unknownHost:42")));

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
                return client.getConnectionState().equals(ZMonitor.Event.CONNECTED);
            });

            // request subscription
            final String reqId = "testId";
            final URI endpoint = new URI("rda3://localhost:7777/testdevice/testprop?ctx=FAIR.SELECTOR.ALL&nFilter=int:1");
            client.subscribe(reqId, endpoint, null);

            final CmwLightMessage subMsg = getNextNonHeartbeatMsg(socket, client);
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

                    return reply != null && reply.size() == 4
                            && Objects.requireNonNull(reply.pollFirst()).getString(ZMQ.CHARSET).equals(reqId)
                            && Objects.requireNonNull(reply.pollFirst()).getString(ZMQ.CHARSET).equals(new CmwLightDataSource.ParsedEndpoint(endpoint, cycleName).toURI().toString())
                            && Objects.requireNonNull(reply.pollFirst()).getString(ZMQ.CHARSET).equals("data")
                            && Objects.requireNonNull(reply.pollFirst()).getData().length == 0;
                });
            }

            // send notification exception
            CmwLightProtocol.sendMsg(socket, CmwLightMessage.notificationExceptionReply(subMsg.sessionId, sourceId, "", "", "testException", 133713371337L, 133713371337L, (byte) 0));
            // assert that the subscription notify exception was received
            Awaitility.await().atMost(Duration.ofSeconds(2)).until(() -> {
                final ZMsg reply = client.getMessage(); // Make client receive ack and update connection status
                client.housekeeping(); // allow the subscription to be sent out

                return reply != null && reply.size() == 4
                        && Objects.requireNonNull(reply.pollFirst()).getString(ZMQ.CHARSET).equals(reqId)
                        && Objects.requireNonNull(reply.pollFirst()).getString(ZMQ.CHARSET).equals(endpoint.toString())
                        && Objects.requireNonNull(reply.pollFirst()).getData().length == 0
                        && Objects.requireNonNull(reply.pollFirst()).getString(ZMQ.CHARSET).contains("testException");
            });

            // unsubscribe
            client.unsubscribe(reqId);
            // assert that the unsubscribe request was sent
            final CmwLightMessage unsubscribeMsg = getNextNonHeartbeatMsg(socket, client);
            assertEquals(CmwLightProtocol.MessageType.CLIENT_REQ, unsubscribeMsg.messageType);
            assertEquals(CmwLightProtocol.RequestType.UNSUBSCRIBE, unsubscribeMsg.requestType);
            // confirm the unsubscribe
            final CmwLightMessage unsubscribeReply = CmwLightMessage.unsubscribeRequest(subMsg.sessionId, sourceId, subMsg.deviceName, subMsg.propertyName, Map.of(CmwLightProtocol.FieldName.SOURCE_ID_TAG.value(), sourceId), CmwLightProtocol.UpdateType.NORMAL);
            unsubscribeReply.messageType = CmwLightProtocol.MessageType.SERVER_REP;
            CmwLightProtocol.sendMsg(socket, unsubscribeReply);
            // assert that the subscription was removed
            Awaitility.await().atMost(Duration.ofSeconds(2)).until(() -> {
                client.getMessage(); // Make client receive ack and update connection status
                client.housekeeping(); // allow the subscription to be sent out
                return !client.replyIdMap.containsKey(sourceId);
            });

            // subscription with error
            final String subErrorReqId = "subErrorReqId";
            client.subscribe(subErrorReqId, endpoint, null);

            final CmwLightMessage subErrorMsg = getNextNonHeartbeatMsg(socket, client);
            assertEquals(CmwLightProtocol.MessageType.CLIENT_REQ, subErrorMsg.messageType);
            assertEquals(CmwLightProtocol.RequestType.SUBSCRIBE, subErrorMsg.requestType);
            assertEquals(Map.of("nFilter", 1), subErrorMsg.requestContext.filters);

            // send subscription error
            CmwLightProtocol.sendMsg(socket, CmwLightMessage.subscribeExceptionReply(subErrorMsg.sessionId, subErrorMsg.id, subErrorMsg.deviceName, subErrorMsg.propertyName, "testException", 1337L, 1337L, (byte) 0));

            // receive subscription error
            Awaitility.await().atMost(Duration.ofSeconds(2)).until(() -> {
                final ZMsg reply = client.getMessage(); // Make client receive ack and update connection status
                client.housekeeping(); // perform housekeeping duties

                return reply != null && reply.size() == 4
                        && Objects.requireNonNull(reply.pollFirst()).getString(ZMQ.CHARSET).equals(subErrorReqId)
                        && Objects.requireNonNull(reply.pollFirst()).getString(ZMQ.CHARSET).equals(endpoint.toString())
                        && Objects.requireNonNull(reply.pollFirst()).getData().length == 0
                        && Objects.requireNonNull(reply.pollFirst()).getString(ZMQ.CHARSET).contains("testException");
            });

            // unsubscibe failing subscription
            client.unsubscribe(subErrorReqId);
            assertEquals(0, client.subscriptions.size()); // because the subscription was not successfull, it will be deleted without network action

            // get request
            final String getReqId = "getReqId";
            client.get(getReqId, endpoint, null, null);
            // assert that get request was sent
            final CmwLightMessage getMsg = getNextNonHeartbeatMsg(socket, client);
            assertEquals(CmwLightProtocol.MessageType.CLIENT_REQ, getMsg.messageType);
            assertEquals(CmwLightProtocol.RequestType.GET, getMsg.requestType);
            assertEquals(Map.of("nFilter", 1), getMsg.requestContext.filters);
            // send get request reply and assert that it was received
            final String cycleName = "FAIR.SELECTOR.C=1337";
            final long getSourceId = client.pendingRequests.keySet().stream().findFirst().orElseThrow();
            CmwLightProtocol.sendMsg(socket, CmwLightMessage.getReply(subMsg.sessionId, getSourceId, "", "", new ZFrame("data"),
                                                     new CmwLightMessage.DataContext(cycleName, 123456789, 123456788, null)));
            Awaitility.await().atMost(Duration.ofSeconds(2)).until(() -> {
                final ZMsg reply = client.getMessage(); // Make client receive ack and update connection status
                client.housekeeping(); // perform housekeeping duties

                return reply != null && reply.size() == 4
                        && Objects.requireNonNull(reply.pollFirst()).getString(ZMQ.CHARSET).equals(getReqId)
                        && Objects.requireNonNull(reply.pollFirst()).getString(ZMQ.CHARSET).equals(new CmwLightDataSource.ParsedEndpoint(endpoint, cycleName).toURI().toString())
                        && Objects.requireNonNull(reply.pollFirst()).getString(ZMQ.CHARSET).equals("data")
                        && Objects.requireNonNull(reply.pollFirst()).getData().length == 0;
            });

            // get request with error
            final String getErrorReqId = "getErrorReqId";
            client.get(getErrorReqId, endpoint, null, null);
            // assert that get request was sent
            final CmwLightMessage getErrorMsg = getNextNonHeartbeatMsg(socket, client);
            assertEquals(CmwLightProtocol.MessageType.CLIENT_REQ, getErrorMsg.messageType);
            assertEquals(CmwLightProtocol.RequestType.GET, getErrorMsg.requestType);
            assertEquals(Map.of("nFilter", 1), getErrorMsg.requestContext.filters);
            // send get request reply and assert that it was received
            assertEquals(1, client.pendingRequests.size());
            final long getErrorSourceId = client.pendingRequests.keySet().stream().findFirst().orElseThrow();
            CmwLightProtocol.sendMsg(socket, CmwLightMessage.exceptionReply(subMsg.sessionId, getErrorSourceId, "", "", "testException", 133713371337L, 133713371337L, (byte) 0));
            Awaitility.await().atMost(Duration.ofSeconds(2)).until(() -> {
                final ZMsg reply = client.getMessage(); // Make client receive ack and update connection status
                client.housekeeping(); // perform housekeeping duties

                return reply != null && reply.size() == 4
                        && Objects.requireNonNull(reply.pollFirst()).getString(ZMQ.CHARSET).equals(getErrorReqId)
                        && Objects.requireNonNull(reply.pollFirst()).getString(ZMQ.CHARSET).equals(endpoint.toString())
                        && Objects.requireNonNull(reply.pollFirst()).getData().length == 0
                        && Objects.requireNonNull(reply.pollFirst()).getString(ZMQ.CHARSET).contains("testException");
            });

            client.close();
        }
    }

    /*
    / get next message sent from client to server ignoring heartbeats, periodically send heartbeat and perform housekeeping
    */
    private CmwLightMessage getNextNonHeartbeatMsg(final ZMQ.Socket socket, final CmwLightDataSource client) throws CmwLightProtocol.RdaLightException {
        int i = 0;
        while (true) {
            final ZMsg msg = ZMsg.recvMsg(socket, false);
            final CmwLightMessage result = msg == null ? null : CmwLightProtocol.parseMsg(msg);
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
    void testFactory() {
        final DataSource.Factory factory = CmwLightDataSource.FACTORY;
        assertThat(factory.getApplicableSchemes(), equalTo(List.of("rda3")));
        assertThat(factory.getMatchingSerialiserType(URI.create("")), equalTo(CmwLightSerialiser.class));
        assertThat(factory.getRegisteredDnsResolver(), empty());
        assertDoesNotThrow(() -> factory.registerDnsResolver(new DirectoryLightClient("localhost:5021")));
        assertThat(factory.getRegisteredDnsResolver(), contains(instanceOf(DirectoryLightClient.class)));
        assertTrue(factory.matches(URI.create("rda3://server/device/property?query=test")));
        assertFalse(factory.matches(URI.create("tcp://server/device/property?query=test")));
        assertTrue(factory.matches(URI.create("rda3:/testdevice/property")));
        assertFalse(factory.matches(URI.create("https://testserver")));
    }

    @Test
    void testResolver() {
        final DataSource.Factory factory = CmwLightDataSource.FACTORY;
        factory.registerDnsResolver(MOCK_CMWLIGHT_RESOLVER);
        try (final ZContext context = new ZContext(1)) {
            assertDoesNotThrow(() -> factory.newInstance(context, URI.create("rda3:/testService"), Duration.ofSeconds(1), Executors.newCachedThreadPool(), "testOpenCmwDataSource"));
            assertThrows(NullPointerException.class, () -> factory.newInstance(context, URI.create("rda3:/unknownService"), Duration.ofSeconds(1), Executors.newCachedThreadPool(), "testOpenCmwDataSource"));
        }
        factory.getRegisteredDnsResolver().remove(MOCK_CMWLIGHT_RESOLVER);
    }

    @Test
    void testSubscription() {
        final CmwLightDataSource.Subscription sub = new CmwLightDataSource.Subscription(URI.create("testuri"), "testdevice", "testproperty", "testselector", Map.of("testFilter", 42));
        assertEquals("Subscription{property='testproperty', device='testdevice', selector='testselector', filters={testFilter=42}, subscriptionState=UNSUBSCRIBED, backOff=20, id=" + sub.id + ", updateId=-1, timeoutValue=-1}", sub.toString());
    }

    @Test
    void testRequest() {
        final CmwLightDataSource.Request testRequest = new CmwLightDataSource.Request(CmwLightProtocol.RequestType.GET, "testReqId", URI.create("test"), EMPTY_FRAME, EMPTY_FRAME);
        assertEquals(CmwLightProtocol.RequestType.GET, testRequest.requestType);
        assertArrayEquals(EMPTY_FRAME, testRequest.data);
        assertArrayEquals(EMPTY_FRAME, testRequest.rbacToken);
        assertEquals(URI.create("test"), testRequest.endpoint);
        assertEquals("testReqId", testRequest.requestId);
    }

    @Test
    void testParsedEndpoint() throws URISyntaxException, CmwLightProtocol.RdaLightException {
        final String refDevice = "deviceA";
        final String refProperty = "MyProperty";
        final String refPath = '/' + refDevice + '/' + refProperty;
        final String testAuthority = "server:1337";
        final String testQuery = "ctx=Test.Context.C=5&filterA=int:3&filterB=bool:true&filterC=foo=bar&filterD=long:1234567890987654321&filterE=double:1.5&filterF=float:-3.5";
        final URI testUri1 = new URI("rda3", testAuthority, refPath, testQuery, null);
        final URI testUri2 = new URI("rda3", null, refPath, testQuery, null);

        final CmwLightDataSource.ParsedEndpoint parsedDefaultCtx = new CmwLightDataSource.ParsedEndpoint(QueryParameterParser.removeQueryParameter(testUri1, "ctx"));
        assertEquals(testAuthority, parsedDefaultCtx.authority);
        assertEquals(refDevice, parsedDefaultCtx.device);
        assertEquals(refProperty, parsedDefaultCtx.property);
        assertEquals(CmwLightDataSource.ParsedEndpoint.DEFAULT_SELECTOR, parsedDefaultCtx.ctx);

        final CmwLightDataSource.ParsedEndpoint parsed1 = new CmwLightDataSource.ParsedEndpoint(testUri1);
        assertEquals(testAuthority, parsed1.authority);
        assertEquals(refDevice, parsed1.device);
        assertEquals(refProperty, parsed1.property);
        final CmwLightDataSource.ParsedEndpoint parsed2 = new CmwLightDataSource.ParsedEndpoint(testUri2);
        assertNull(parsed2.authority);
        assertEquals(refDevice, parsed2.device);
        assertEquals(refProperty, parsed2.property);

        assertEquals(parsed1, parsed1); // NOSONAR
        assertNotEquals(parsed1, new Object());
        assertEquals(testUri1, parsed1.toURI());
        assertNotEquals(testUri1, parsed2.toURI()); // since testURI2 has no authority given
        assertEquals(parsed1, parsed2);
        assertEquals(parsed1.hashCode(), parsed2.hashCode());

        parsed2.filters.put("illegalFilterItem", URI.create(""));
        assertThrows(IllegalArgumentException.class, parsed2::toURI);
        // simple test for faulty sub-property definition - fail early
        final URI faultyTestUri = new URI("rda3", "server:1337", "/deviceA/MyProperty/SubProperty", "filterA=short:3", null);
        assertThrows(CmwLightProtocol.RdaLightException.class, () -> new CmwLightDataSource.ParsedEndpoint(faultyTestUri).toURI());
    }
}
