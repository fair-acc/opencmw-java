package io.opencmw.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.Test;

import io.opencmw.serialiser.spi.JsonSerialiser;

public class DataSourceFilterTest {
    @Test
    void testDataSourceFilter() {
        final DataSourceFilter filterA = new DataSourceFilter();
        final DataSourceFilter filterB = new DataSourceFilter();
        assertEquals(filterA, filterB);
        filterA.arrivalTimestamp = 1227L;
        assertNotEquals(filterA, filterB);
        filterA.clear();
        assertEquals(filterA, filterB);

        filterA.endpoint = "test://server:port/hello/world";
        assertNotEquals(filterA, filterB);
        filterA.clear();

        filterA.eventType = DataSourceFilter.ReplyType.SUBSCRIBE;
        assertNotEquals(filterA, filterB);

        filterB.eventType = DataSourceFilter.ReplyType.SUBSCRIBE;
        assertEquals(filterA, filterB);

        filterA.eventType = DataSourceFilter.ReplyType.GET;
        filterA.arrivalTimestamp = 42L;
        filterA.endpoint = "test";
        filterA.protocolType = JsonSerialiser.class;
        assertNotEquals(filterA, filterB);
        filterA.copyTo(filterB);
        assertEquals(filterA, filterB);

        assertEquals(DataSourceFilter.ReplyType.SUBSCRIBE, DataSourceFilter.ReplyType.valueOf(0));
        assertEquals(DataSourceFilter.ReplyType.GET, DataSourceFilter.ReplyType.valueOf(1));
        assertEquals(DataSourceFilter.ReplyType.SET, DataSourceFilter.ReplyType.valueOf(2));
        assertEquals(DataSourceFilter.ReplyType.UNSUBSCRIBE, DataSourceFilter.ReplyType.valueOf(3));
        assertEquals(DataSourceFilter.ReplyType.UNKNOWN, DataSourceFilter.ReplyType.valueOf(-1));
        assertEquals(DataSourceFilter.ReplyType.UNKNOWN, DataSourceFilter.ReplyType.valueOf(42));
    }
}
