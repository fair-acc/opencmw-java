package io.opencmw.server;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.junit.jupiter.api.Assertions.*;

import static io.opencmw.OpenCmwProtocol.Command.SET_REQUEST;

import java.io.IOException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.zeromq.Utils;
import org.zeromq.util.ZData;

import io.opencmw.OpenCmwProtocol;
import io.opencmw.rbac.BasicRbacRole;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MmiServiceHelperTests {
    private static final String DEFAULT_REQUEST_MESSAGE = "Hello World!";
    private static final byte[] DEFAULT_REQUEST_MESSAGE_BYTES = DEFAULT_REQUEST_MESSAGE.getBytes(UTF_8);
    private MajordomoBroker broker;
    private String brokerAddress;

    @BeforeAll
    void startBroker() throws IOException {
        broker = new MajordomoBroker("TestBroker", "", BasicRbacRole.values());
        // broker.setDaemon(true); // use this if running in another app that controls threads
        brokerAddress = broker.bind("mdp://*:" + Utils.findOpenPort());
        broker.start();
    }

    @AfterAll
    void stopBroker() {
        broker.stopBroker();
    }

    @Test
    void basicEchoTest() {
        MajordomoTestClientSync clientSession = new MajordomoTestClientSync(brokerAddress, "customClientName");

        final OpenCmwProtocol.MdpMessage reply = clientSession.send(SET_REQUEST, "mmi.echo", DEFAULT_REQUEST_MESSAGE_BYTES); // w/o RBAC
        assertNotNull(reply, "reply not being null");
        assertNotNull(reply.data, "user-data not being null");
        assertArrayEquals(DEFAULT_REQUEST_MESSAGE_BYTES, reply.data, "equal data");
    }

    @Test
    void basicEchoHtmlTest() {
        MajordomoTestClientSync clientSession = new MajordomoTestClientSync(brokerAddress, "customClientName");

        final OpenCmwProtocol.MdpMessage reply = clientSession.send(SET_REQUEST, "mmi.echo?contentType=HTML", DEFAULT_REQUEST_MESSAGE_BYTES); // w/o RBAC
        assertNotNull(reply, "reply not being null");
        assertNotNull(reply.data, "user-data not being null");
        assertArrayEquals(DEFAULT_REQUEST_MESSAGE_BYTES, reply.data, "equal data");
    }

    @Test
    void basicDnsTest() {
        MajordomoTestClientSync clientSession = new MajordomoTestClientSync(brokerAddress, "customClientName");

        final OpenCmwProtocol.MdpMessage reply = clientSession.send(SET_REQUEST, "mmi.dns", DEFAULT_REQUEST_MESSAGE_BYTES); // w/o RBAC
        assertNotNull(reply, "reply not being null");
        assertNotNull(reply.data, "user-data not being null");
        assertTrue(ZData.toString(reply.data).startsWith("[TestBroker: mdp://"));
    }

    @Test
    void basicDnsHtmlTest() {
        MajordomoTestClientSync clientSession = new MajordomoTestClientSync(brokerAddress, "customClientName");

        final OpenCmwProtocol.MdpMessage reply = clientSession.send(SET_REQUEST, "mmi.dns?contentType=HTML", DEFAULT_REQUEST_MESSAGE_BYTES); // w/o RBAC
        assertNotNull(reply, "reply not being null");
        assertNotNull(reply.data, "user-data not being null");
        assertTrue(ZData.toString(reply.data).startsWith("[TestBroker: mdp://"));
    }

    @Test
    void basicSerivceTest() {
        MajordomoTestClientSync clientSession = new MajordomoTestClientSync(brokerAddress, "customClientName");

        {
            final OpenCmwProtocol.MdpMessage reply = clientSession.send(SET_REQUEST, "mmi.service", "".getBytes(UTF_8)); // w/o RBAC
            assertNotNull(reply, "reply not being null");
            assertNotNull(reply.data, "user-data not being null");
            assertTrue(ZData.toString(reply.data).startsWith("mmi.dns,mmi.echo,mmi.openapi,mmi.service"));
        }

        {
            final OpenCmwProtocol.MdpMessage reply = clientSession.send(SET_REQUEST, "mmi.service", "mmi.service".getBytes(UTF_8)); // w/o RBAC
            assertNotNull(reply, "reply not being null");
            assertNotNull(reply.data, "user-data not being null");
            assertTrue(ZData.toString(reply.data).startsWith("200"));
        }

        {
            final OpenCmwProtocol.MdpMessage reply = clientSession.send(SET_REQUEST, "mmi.service", "doesNotExist".getBytes(UTF_8)); // w/o RBAC
            assertNotNull(reply, "reply not being null");
            assertNotNull(reply.data, "user-data not being null");
            assertTrue(ZData.toString(reply.data).startsWith("400"));
        }
    }

    @Test
    void basicServiceHtmlTest() {
        MajordomoTestClientSync clientSession = new MajordomoTestClientSync(brokerAddress, "customClientName");

        {
            final OpenCmwProtocol.MdpMessage reply = clientSession.send(SET_REQUEST, "mmi.service?contentType=HTML", "".getBytes(UTF_8)); // w/o RBAC
            assertNotNull(reply, "reply not being null");
            assertNotNull(reply.data, "user-data not being null");
            assertTrue(ZData.toString(reply.data).startsWith("<a href=\"/mmi.dns\">mmi.dns</a>,<a href=\"/mmi.echo\">mmi.echo</a>,<a href=\"/mmi.openapi\">mmi.openapi</a>,<a href=\"/mmi.service\">mmi.service</a>"));
        }

        {
            final OpenCmwProtocol.MdpMessage reply = clientSession.send(SET_REQUEST, "mmi.service?contentType=HTML", "mmi.service".getBytes(UTF_8)); // w/o RBAC
            assertNotNull(reply, "reply not being null");
            assertNotNull(reply.data, "user-data not being null");
            assertTrue(ZData.toString(reply.data).startsWith("200"));
        }

        {
            final OpenCmwProtocol.MdpMessage reply = clientSession.send(SET_REQUEST, "mmi.service?contentType=HTML", "doesNotExist".getBytes(UTF_8)); // w/o RBAC
            assertNotNull(reply, "reply not being null");
            assertNotNull(reply.data, "user-data not being null");
            assertTrue(ZData.toString(reply.data).startsWith("400"));
        }
    }

    @Test
    void basicOpenAPITest() {
        MajordomoTestClientSync clientSession = new MajordomoTestClientSync(brokerAddress, "customClientName");

        final OpenCmwProtocol.MdpMessage reply = clientSession.send(SET_REQUEST, "mmi.openapi", "mmi.echo".getBytes(UTF_8)); // w/o RBAC
        assertNotNull(reply, "reply not being null");
        assertNotNull(reply.data, "user-data not being null");
        assertTrue(ZData.toString(reply.data).startsWith("io.opencmw.server.MmiServiceHelper$MmiEcho"));
    }

    @Test
    void basicOpenAPIExceptionTest() {
        MajordomoTestClientSync clientSession = new MajordomoTestClientSync(brokerAddress, "customClientName");

        final OpenCmwProtocol.MdpMessage reply = clientSession.send(SET_REQUEST, "mmi.openapi?contentType=HTML", "".getBytes(UTF_8)); // w/o RBAC
        assertNotNull(reply, "reply not being null");
        assertNotNull(reply.data, "user-data not being null");
        assertNotNull(reply.errors);
        assertFalse(reply.errors.isBlank());
    }
}
