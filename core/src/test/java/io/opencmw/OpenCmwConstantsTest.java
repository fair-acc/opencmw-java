package io.opencmw;

import static org.junit.jupiter.api.Assertions.*;

import static io.opencmw.OpenCmwConstants.*;

import java.net.URI;

import org.junit.jupiter.api.Test;

class OpenCmwConstantsTest {
    @Test
    void testReplaceScheme() {
        assertEquals(URI.create("tcp://host:20"), replaceScheme(URI.create("mdp://host:20"), SCHEME_TCP));
        assertEquals(URI.create("http://host:20"), replaceScheme(URI.create("mdp://host:20"), SCHEME_HTTP));
        assertEquals(URI.create("https://host:20"), replaceScheme(URI.create("mdp://host:20"), SCHEME_HTTPS));
        assertEquals(URI.create("mdp://host:20"), replaceScheme(URI.create("mdp://host:20"), SCHEME_MDP));
        assertEquals(URI.create("mds://host:20"), replaceScheme(URI.create("mdp://host:20"), SCHEME_MDS));

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
        assertEquals("device", getDeviceName(URI.create("mdp://authrority/device/property/sub-property")));
        assertEquals("device", getDeviceName(URI.create("mdp://authrority//device/property/sub-property")));
        assertEquals("authrority", URI.create("mdp://authrority//device/property/sub-property").getAuthority());
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
}