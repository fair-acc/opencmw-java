package de.gsi.microservice.datasource.cmwlight;


import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.zeromq.*;

import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CmwLightClientTest {
    @Test
    void testCmwLightSubscription() throws CmwLightProtocol.RdaLightException {
        // setup zero mq socket to mock cmw server
        ZContext context = new ZContext(1);
        ZMQ.Socket socket = context.createSocket(SocketType.DEALER);
        socket.bind("tcp://localhost:7777");

        final CmwLightClient client = new CmwLightClient(context, "rda3://localhost:7777/testdevice/testprop?ctx=test.selector&nFilter=int:1", Duration.ofSeconds(1), "testClientId", null);

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
            return client.connectionState.get().equals(CmwLightClient.ConnectionState.CONNECTED);
        });

        // request subscription
        client.subscribe(new CmwLightClient.Subscription("testdevice", "testprop", "test.selector", Map.of("nFilter", 1)));

        final CmwLightMessage subMsg = getNextNonHeartbeatMsg(socket, client, false);
        assertEquals(CmwLightProtocol.MessageType.CLIENT_REQ, subMsg.messageType);
        assertEquals(CmwLightProtocol.RequestType.SUBSCRIBE, subMsg.requestType);
        assertEquals(Map.of("nFilter", 1), subMsg.requestContext.filters);

        // acknowledge subscription
        final long sourceId = 1337L;
        CmwLightProtocol.sendMsg(socket, CmwLightMessage.subscribeReply(subMsg.sessionId, subMsg.id, subMsg.deviceName, subMsg.propertyName, Map.of(CmwLightProtocol.FieldName.SOURCE_ID_TAG.value(), sourceId)));

        final CmwLightMessage subMsgRec = getNextNonHeartbeatClientMsg(client, true);
        assertEquals(CmwLightProtocol.MessageType.SERVER_REP, subMsgRec.messageType);
        assertEquals(CmwLightProtocol.RequestType.SUBSCRIBE, subMsgRec.requestType);

        // send 10 updates
        long notificationId = 0;
        for (int i = 0; i < 10; i++) {
            CmwLightProtocol.sendMsg(socket, CmwLightMessage.notificationReply(subMsg.sessionId, sourceId, "", "", new ZFrame("data"), notificationId,
                    new CmwLightMessage.DataContext("test.context", 123456789, 123456788, null), CmwLightProtocol.UpdateType.NORMAL));

            final CmwLightMessage updateMsg = getNextNonHeartbeatClientMsg(client, false);
            assertEquals(CmwLightProtocol.MessageType.SERVER_REP, updateMsg.messageType);
            assertEquals(CmwLightProtocol.RequestType.NOTIFICATION_DATA, updateMsg.requestType);
            assertEquals(sourceId, updateMsg.id);
            assertEquals(notificationId, updateMsg.notificationId);
            assertEquals("data", updateMsg.bodyData.getString(Charset.defaultCharset()));
            notificationId++;
        }
    }

    /*
    / get next message sent from client to server ignoring heartbeats, periodically send heartbeat and perform housekeeping
    */
    private CmwLightMessage getNextNonHeartbeatMsg(final ZMQ.Socket socket, final CmwLightClient client, boolean debug) throws CmwLightProtocol.RdaLightException {
        int i = 0;
        while(true) {
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

    /*
    / get next message ignoring heartbeats, periodically send heartbeat and perform housekeeping
    */
    private CmwLightMessage getNextNonHeartbeatClientMsg(final CmwLightClient client, boolean debug) throws CmwLightProtocol.RdaLightException {
        while(true) {
            final CmwLightMessage result = client.receiveData();
            if (debug) {
                if (result == null) {
                    System.out.print('.');
                } else {
                    System.out.println(result);
                }
            }
            if (result != null && result.messageType != CmwLightProtocol.MessageType.SERVER_HB) {
                return result;
            }
            client.housekeeping();
            LockSupport.parkNanos(100000);
        }
    }
}
