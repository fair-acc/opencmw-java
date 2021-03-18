package io.opencmw;

import static org.junit.jupiter.api.Assertions.*;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.junit.jupiter.api.Test;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

/**
 * basic MDP OpenCMW protocol consistency tests
 */
class OpenCmwProtocolTests {
    private static final OpenCmwProtocol.MdpMessage TEST_MESSAGE = new OpenCmwProtocol.MdpMessage("senderName".getBytes(StandardCharsets.UTF_8), //
            OpenCmwProtocol.MdpSubProtocol.PROT_CLIENT, OpenCmwProtocol.Command.GET_REQUEST, //
            "serviceName".getBytes(StandardCharsets.UTF_8), new byte[] { (byte) 3, (byte) 2, (byte) 1 },
            URI.create("test/topic"), "test data - Hello World!".getBytes(StandardCharsets.UTF_8), "error message", "rbacToken".getBytes(StandardCharsets.UTF_8));

    @Test
    void testCommandEnum() {
        for (OpenCmwProtocol.Command cmd : OpenCmwProtocol.Command.values()) {
            assertEquals(cmd, OpenCmwProtocol.Command.getCommand(cmd.getData()));
            assertNotNull(cmd.toString());
            if (!cmd.equals(OpenCmwProtocol.Command.UNKNOWN)) {
                assertTrue(cmd.isClientCompatible() || cmd.isWorkerCompatible());
            }
        }
        assertFalse(OpenCmwProtocol.Command.UNKNOWN.isWorkerCompatible());
        assertFalse(OpenCmwProtocol.Command.UNKNOWN.isClientCompatible());
    }

    @Test
    void testMdpSubProtocolEnum() {
        for (OpenCmwProtocol.MdpSubProtocol cmd : OpenCmwProtocol.MdpSubProtocol.values()) {
            assertEquals(cmd, OpenCmwProtocol.MdpSubProtocol.getProtocol(cmd.getData()));
            assertNotNull(cmd.toString());
        }
    }

    @Test
    void testMdpIdentity() {
        final OpenCmwProtocol.MdpMessage test = TEST_MESSAGE;
        assertEquals(OpenCmwProtocol.Command.GET_REQUEST, test.command);
        assertEquals(TEST_MESSAGE, test, "object identity");
        assertNotEquals(TEST_MESSAGE, new Object(), "inequality if different class type");
        final OpenCmwProtocol.MdpMessage clone = new OpenCmwProtocol.MdpMessage(TEST_MESSAGE);
        assertEquals(TEST_MESSAGE, clone, "copy constructor");
        assertEquals(TEST_MESSAGE.hashCode(), clone.hashCode(), "hashCode equality");
        final OpenCmwProtocol.MdpMessage modified = new OpenCmwProtocol.MdpMessage(TEST_MESSAGE);
        modified.protocol = OpenCmwProtocol.MdpSubProtocol.PROT_WORKER;
        assertNotEquals(TEST_MESSAGE, modified, "copy constructor");
        assertTrue(TEST_MESSAGE.hasRbackToken(), "non-empty rbac token");
        assertEquals("senderName", TEST_MESSAGE.getSenderName(), "sender name string");
        assertEquals("serviceName", TEST_MESSAGE.getServiceName(), "service name string");
        assertNotNull(TEST_MESSAGE.toString());
    }

    @Test
    void testMdpSendReceiveIdentity() {
        try (ZContext ctx = new ZContext()) {
            {
                final ZMQ.Socket receiveSocket1 = ctx.createSocket(SocketType.ROUTER);
                receiveSocket1.bind("inproc://pair1");
                final ZMQ.Socket receiveSocket2 = ctx.createSocket(SocketType.DEALER);
                receiveSocket2.bind("inproc://pair2");
                final ZMQ.Socket sendSocket = ctx.createSocket(SocketType.DEALER);
                sendSocket.setIdentity(TEST_MESSAGE.senderID);
                sendSocket.connect("inproc://pair1");
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(200));

                TEST_MESSAGE.send(sendSocket);
                final OpenCmwProtocol.MdpMessage reply = OpenCmwProtocol.MdpMessage.receive(receiveSocket1);
                assertEquals(TEST_MESSAGE, reply, "serialisation identity via router");

                sendSocket.disconnect("inproc://pair1");
                sendSocket.connect("inproc://pair2");
                TEST_MESSAGE.send(sendSocket);
                final OpenCmwProtocol.MdpMessage reply2 = OpenCmwProtocol.MdpMessage.receive(receiveSocket2);
                assertEquals(TEST_MESSAGE, reply2, "serialisation identity via dealer");

                OpenCmwProtocol.MdpMessage.send(sendSocket, List.of(TEST_MESSAGE));
                final OpenCmwProtocol.MdpMessage reply3 = OpenCmwProtocol.MdpMessage.receive(receiveSocket2);
                final OpenCmwProtocol.MdpMessage clone = new OpenCmwProtocol.MdpMessage(TEST_MESSAGE);
                clone.command = OpenCmwProtocol.Command.FINAL; // N.B. multiple message exist only for reply type either FINAL, or (PARTIAL, PARTIAL, ..., FINAL)
                assertEquals(clone, reply3, "serialisation identity via dealer");
            }
        }
    }

    @Test
    void testContext() {
        assertDoesNotThrow(() -> new OpenCmwProtocol.Context(TEST_MESSAGE));

        final OpenCmwProtocol.Context testCtx1 = new OpenCmwProtocol.Context(TEST_MESSAGE);
        final OpenCmwProtocol.Context testCtx2 = new OpenCmwProtocol.Context(new OpenCmwProtocol.MdpMessage(TEST_MESSAGE));
        assertNotNull(testCtx2.toString());
        assertEquals(testCtx1, testCtx2);
        assertEquals(testCtx1.hashCode(), testCtx2.hashCode());
        assertNotEquals(testCtx1, new Object(), "object type mismatch"); // NOPMD

        testCtx2.rep.topic = URI.create("otherRepTopic");
        assertNotEquals(testCtx1, testCtx2);
        assertNotEquals(testCtx1.hashCode(), testCtx2.hashCode());

        testCtx2.req.topic = URI.create("otherReqTopic");
        assertNotEquals(testCtx1, testCtx2);
        assertNotEquals(testCtx1.hashCode(), testCtx2.hashCode());
    }
}
