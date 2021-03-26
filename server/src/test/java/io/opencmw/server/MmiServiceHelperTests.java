package io.opencmw.server;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.junit.jupiter.api.Assertions.*;

import static io.opencmw.OpenCmwProtocol.Command.SET_REQUEST;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.*;
import org.zeromq.Utils;
import org.zeromq.util.ZData;

import io.opencmw.OpenCmwProtocol;
import io.opencmw.rbac.BasicRbacRole;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MmiServiceHelperTests {
    private static final String DEFAULT_REQUEST_MESSAGE = "Hello World!";
    private static final String DEFAULT_BROKER_NAME = "TestBroker";
    private static final byte[] DEFAULT_REQUEST_MESSAGE_BYTES = DEFAULT_REQUEST_MESSAGE.getBytes(UTF_8);
    private MajordomoBroker broker;
    private URI brokerAddress;

    @BeforeAll
    @Timeout(10)
    void startBroker() throws IOException {
        broker = new MajordomoBroker(DEFAULT_BROKER_NAME, null, BasicRbacRole.values());
        // broker.setDaemon(true); // use this if running in another app that controls threads
        brokerAddress = broker.bind(URI.create("mdp://*:" + Utils.findOpenPort()));
        broker.start();
    }

    @AfterAll
    @Timeout(10)
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
        assertTrue(ZData.toString(reply.data).startsWith("[" + DEFAULT_BROKER_NAME + ": mdp://"));

        final OpenCmwProtocol.MdpMessage dnsAll = clientSession.send(SET_REQUEST, "mmi.dns", DEFAULT_REQUEST_MESSAGE_BYTES); // w/o RBAC
        assertNotNull(dnsAll, "dnsAll not being null");
        assertNotNull(dnsAll.data, "dnsAll user-data not being null");
        final Map<String, List<URI>> dnsMapAll = parseDnsReply(dnsAll.data);
        assertFalse(dnsMapAll.isEmpty());
        assertFalse(dnsMapAll.get(DEFAULT_BROKER_NAME).isEmpty());
        // System.err.println("dnsMapAll: '" + dnsMapAll + "'\n") // enable for debugging

        final List<String> queryDevice = Arrays.asList("mdp:/TestBroker/mmi.service", "/TestBroker/mmi.openapi", "mds:/TestBroker/mmi.service", "/TestBroker", "unknown");
        final String query = String.join(",", queryDevice);
        final OpenCmwProtocol.MdpMessage dnsSpecific = clientSession.send(SET_REQUEST, "mmi.dns?" + query, DEFAULT_REQUEST_MESSAGE_BYTES); // w/o RBAC
        assertNotNull(dnsSpecific, "dnsSpecific not being null");
        assertNotNull(dnsSpecific.data, " dnsSpecificuser-data not being null");
        final Map<String, List<URI>> dnsMapSelective = parseDnsReply(dnsSpecific.data);
        System.err.println("dnsMapSelective: '" + dnsMapSelective + "'\n"); // enable for debugging
        assertFalse(dnsMapSelective.isEmpty());
        assertEquals(5, dnsMapSelective.size());
        assertEquals(1, dnsMapSelective.get("mdp:/TestBroker/mmi.service").size(), "match full scheme and path");
        assertEquals(1, dnsMapSelective.get("/TestBroker/mmi.openapi").size(), "match full path w/o scheme");
        assertEquals(4, dnsMapSelective.get("/TestBroker").size(), "list of all properties for a given device");
        assertTrue(dnsMapSelective.get("mds:/TestBroker/mmi.service").isEmpty(), "protocol mismatch");
        assertTrue(dnsMapSelective.get("unknown").isEmpty(), "unknown service/device");
    }

    @Test
    void basicDnsHtmlTest() {
        MajordomoTestClientSync clientSession = new MajordomoTestClientSync(brokerAddress, "customClientName");

        final OpenCmwProtocol.MdpMessage reply = clientSession.send(SET_REQUEST, "mmi.dns?contentType=HTML", DEFAULT_REQUEST_MESSAGE_BYTES); // w/o RBAC
        assertNotNull(reply, "reply not being null");
        assertNotNull(reply.data, "user-data not being null");
        assertTrue(ZData.toString(reply.data).startsWith("[" + DEFAULT_BROKER_NAME + ": mdp://"), " reply data was: " + ZData.toString(reply.data));
    }

    @Test
    void basicServiceTest() {
        MajordomoTestClientSync clientSession = new MajordomoTestClientSync(brokerAddress, "customClientName");

        {
            final OpenCmwProtocol.MdpMessage reply = clientSession.send(SET_REQUEST, "mmi.service", "".getBytes(UTF_8)); // w/o RBAC
            assertNotNull(reply, "reply not being null");
            assertNotNull(reply.data, "user-data not being null");
            assertTrue(ZData.toString(reply.data).contains(DEFAULT_BROKER_NAME + "/mmi.dns"));
            assertTrue(ZData.toString(reply.data).contains(DEFAULT_BROKER_NAME + "/mmi.echo"));
            assertTrue(ZData.toString(reply.data).contains(DEFAULT_BROKER_NAME + "/mmi.openapi"));
            assertTrue(ZData.toString(reply.data).contains(DEFAULT_BROKER_NAME + "/mmi.service"));
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
            assertTrue(ZData.toString(reply.data).contains("<a href=\"/" + DEFAULT_BROKER_NAME + "/mmi.dns\">"));
            assertTrue(ZData.toString(reply.data).contains("<a href=\"/" + DEFAULT_BROKER_NAME + "/mmi.echo\">"));
            assertTrue(ZData.toString(reply.data).contains("<a href=\"/" + DEFAULT_BROKER_NAME + "/mmi.openapi\">"));
            assertTrue(ZData.toString(reply.data).contains("<a href=\"/" + DEFAULT_BROKER_NAME + "/mmi.service\">"));
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

    private static Map<String, List<URI>> parseDnsReply(final byte[] dnsReply) {
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

    private static boolean isUTF8(byte[] array) {
        final CharsetDecoder decoder = UTF_8.newDecoder();
        final ByteBuffer buf = ByteBuffer.wrap(array);
        try {
            decoder.decode(buf);
        } catch (CharacterCodingException e) {
            return false;
        }
        return true;
    }
}
