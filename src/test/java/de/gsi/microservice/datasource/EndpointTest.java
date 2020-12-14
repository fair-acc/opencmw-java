package de.gsi.microservice.datasource;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class EndpointTest {
    @Test
    void testEndpointParsing() {
        final Endpoint ep = new Endpoint("rda3://server:port/device/property?ctx=test.sel:t=100&filter=asdf&amount=int:1");
        assertEquals("rda3://", ep.getProtocol());
        assertEquals("rda3://server:port", ep.getAddress());
        assertEquals("device", ep.getDevice());
        assertEquals("property", ep.getProperty());
        assertEquals("test.sel:t=100", ep.getSelector());
        assertEquals(Map.of("filter", "asdf", "amount", Integer.valueOf(1)), ep.getFilters());
        assertEquals("rda3://server:port/device/property?ctx=test.sel:t=101:id=1&filter=asdf&amount=int:1", ep.getEndpointForContext("test.sel:t=101:id=1"));
    }
}
