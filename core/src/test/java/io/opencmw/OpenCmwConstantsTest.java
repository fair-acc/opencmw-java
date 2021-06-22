package io.opencmw;

import static org.junit.jupiter.api.Assertions.*;

import static io.opencmw.OpenCmwConstants.*;
import static io.opencmw.OpenCmwProtocol.MdpSubProtocol.PROT_CLIENT;
import static io.opencmw.utils.SystemProperties.parseOptions;

import java.net.URI;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import io.opencmw.serialiser.spi.Field;
import io.opencmw.utils.SystemProperties;

class OpenCmwConstantsTest {
    @BeforeAll
    static void init() {
        OpenCmwConstants.init(); // needed to execute once -- other class member function invocation work as well
    }

    @Test
    void testCommandLineOptions() {
        final String testOption_heartbeat = OpenCmwConstants.class.getSimpleName() + ".HEARTBEAT_INTERVAL";
        Field.getField(SystemProperties.class, "WITH_EXIT").setBoolean(null, false); // needed only for testing purposes

        assertEquals(1001, Integer.parseInt(parseOptions(new String[] { "--" + testOption_heartbeat + "=1001" }).get(testOption_heartbeat).toString()));

        assertEquals(1001, HEARTBEAT_INTERVAL);
        assertEquals(1001, SystemProperties.getIntValueIgnoreCase(testOption_heartbeat));

        assertEquals(1002, Integer.parseInt(parseOptions(new String[] { "--" + testOption_heartbeat + "=1002" }).get(testOption_heartbeat).toString()));
        assertEquals(1002, HEARTBEAT_INTERVAL);
        assertEquals(1002, SystemProperties.getIntValueIgnoreCase(testOption_heartbeat));

        try {
            parseOptions(new String[] { "--help" });
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("--" + testOption_heartbeat));
        }

        Field.getField(SystemProperties.class, "WITH_EXIT").setBoolean(null, true); // needed only for testing purposes
    }

    @Test
    void testReplaceScheme() {
        assertEquals(URI.create("tcp://host:20"), replaceScheme(URI.create("mdp://host:20"), SCHEME_TCP));
        assertEquals(URI.create("http://host:20"), replaceScheme(URI.create("mdp://host:20"), SCHEME_HTTP));
        assertEquals(URI.create("https://host:20"), replaceScheme(URI.create("mdp://host:20"), SCHEME_HTTPS));
        assertEquals(URI.create("mdp://host:20"), replaceScheme(URI.create("mdp://host:20"), SCHEME_MDP));
        assertEquals(URI.create("mds://host:20"), replaceScheme(URI.create("mdp://host:20"), SCHEME_MDS));

        assertEquals(URI.create("tcp://host:20"), replaceSchemeKeepOnlyAuthority(URI.create("mdp://host:20/device/property?test"), SCHEME_TCP));
        assertThrows(NullPointerException.class, () -> replaceSchemeKeepOnlyAuthority(URI.create("mdp:/device/property"), SCHEME_TCP));

        assertEquals(URI.create("mdp://host:20"), replacePath(URI.create("mdp://host:20/device/property"), ""));
        assertEquals(URI.create("mdp://host:20/device/property"), replacePath(URI.create("mdp://host:20/device/property"), "/device/property"));
        assertEquals(URI.create("mdp://host:20/otherDevice/path"), replacePath(URI.create("mdp://host:20/device/property"), "/otherDevice/path"));

        assertEquals(URI.create("mdp://host:20/device/property?queryA"), replaceQuery(URI.create("mdp://host:20/device/property?queryA"), "queryA"));
        assertEquals(URI.create("mdp://host:20/device/property?queryB"), replaceQuery(URI.create("mdp://host:20/device/property?queryA"), "queryB"));
        assertEquals(URI.create("mdp://host:20/device/property"), replaceQuery(URI.create("mdp://host:20/device/property?queryA"), null));

        assertEquals(URI.create("tcp://host:20/path"), replaceScheme(URI.create("mdp://host:20/path"), SCHEME_TCP));
        assertEquals(URI.create("tcp://host:20/path"), replaceScheme(URI.create("mdp://host:20/path"), SCHEME_TCP));

        assertThrows(IllegalArgumentException.class, () -> replaceScheme(URI.create("mdp:://host:20"), SCHEME_TCP));
        assertEquals(URI.create("inproc://host:20/path"), replaceScheme(URI.create("inproc://host:20/path"), SCHEME_TCP), "do not change inproc scheme");

        assertEquals(URI.create("tcp://host:20/path"), stripPathTrailingSlash(URI.create("tcp://host:20/path/")));
        assertEquals(URI.create("tcp://host:20/path"), stripPathTrailingSlash(URI.create("tcp://host:20/path//")));
    }

    @Test
    void testDeviceAndPropertyNames() {
        assertEquals("device", getDeviceName(URI.create("mdp:/device/property/sub-property")));
        assertEquals("device", getDeviceName(URI.create("mdp://authority/device/property/sub-property")));
        assertEquals("device", getDeviceName(URI.create("mdp://authority//device/property/sub-property")));
        assertEquals("authority", URI.create("mdp://authority//device/property/sub-property").getAuthority());
        assertEquals("property/sub-property", getPropertyName(URI.create("mdp:/device/property/sub-property")));
        assertEquals("property/sub-property", getPropertyName(URI.create("mdp:/device/property/sub-property")));
    }

    @Test
    void testResolveLocalHostName() {
        assertDoesNotThrow(OpenCmwConstants::getLocalHostName);
        assertEquals(URI.create("tcp://localhost:20/path/"), resolveHost(URI.create("tcp://*:20/path/"), "localhost"));
        assertEquals(URI.create("tcp://localhost:20/path/"), resolveHost(URI.create("tcp://localhost:20/path/"), "localhost"));
        assertEquals(URI.create("tcp://localhost/path/"), resolveHost(URI.create("tcp://localhost/path/"), "localhost"));

        assertThrows(IllegalArgumentException.class, () -> resolveHost(URI.create("tcp://*:aa/path/"), ""));
    }

    @Test
    void testMisc() {
        try (ZContext ctx = new ZContext(); ZMQ.Socket socket = ctx.createSocket(SocketType.DEALER)) {
            assertDoesNotThrow(() -> setDefaultSocketParameters(socket));
            assertEquals(HIGH_WATER_MARK, socket.getRcvHWM(), "receive high-water mark");
            assertEquals(HIGH_WATER_MARK, socket.getSndHWM(), "send high-water mark");
            assertArrayEquals(PROT_CLIENT.getData(), socket.getHeartbeatContext(), "heart-beat payload message");
            assertEquals(HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS, socket.getHeartbeatTtl(), "time-out for remote socket [ms]");
            assertEquals(HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS, socket.getHeartbeatTimeout(), "time-out for local socket [ms]");
            assertEquals(HEARTBEAT_INTERVAL, socket.getHeartbeatIvl(), "heart-beat ping period [ms]");
        }
    }
}