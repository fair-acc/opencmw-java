package io.opencmw.filter;

import static org.junit.jupiter.api.Assertions.*;

import java.net.URI;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.ThrowingSupplier;

class EvtTypeFilterTests {
    @Test
    void basicTests() {
        assertDoesNotThrow((ThrowingSupplier<EvtTypeFilter>) EvtTypeFilter::new);
        assertNull(new EvtTypeFilter().get(""));

        final EvtTypeFilter evtTypeFilter = new EvtTypeFilter();
        assertInitialised(evtTypeFilter);
        assertNotNull(new EvtTypeFilter().get(evtTypeFilter.getValue()));
        assertEquals(evtTypeFilter, new EvtTypeFilter().get(evtTypeFilter.getValue()));
        assertEquals(evtTypeFilter, evtTypeFilter);
        evtTypeFilter.clear();
        assertInitialised(evtTypeFilter);

        assertTrue(evtTypeFilter.matches(evtTypeFilter));
        assertTrue(evtTypeFilter.matches(new EvtTypeFilter().get(evtTypeFilter.getValue())));
        assertFalse(evtTypeFilter.matches(null));
        assertNotNull(evtTypeFilter.toString());

        assertNotNull(new EvtTypeFilter().get("EvtTypeFilter:TIMING_EVENT:test"));
        assertNull(new EvtTypeFilter().get("TypeFilter:TIMING_EVENT:test"));
    }

    @Test
    void testEqualsAndHash() {
        final EvtTypeFilter evtTypeFilter1 = new EvtTypeFilter();
        evtTypeFilter1.evtType = EvtTypeFilter.DataType.DEVICE_DATA;
        evtTypeFilter1.property = URI.create("DeviceName");
        // check identity
        assertEquals(evtTypeFilter1, evtTypeFilter1);
        assertEquals(evtTypeFilter1.hashCode(), evtTypeFilter1.hashCode());
        assertTrue(EvtTypeFilter.isDeviceData().test(evtTypeFilter1));
        assertTrue(EvtTypeFilter.isDeviceData("DeviceName").test(evtTypeFilter1));

        assertNotEquals(evtTypeFilter1, new Object());

        final EvtTypeFilter evtTypeFilter2 = new EvtTypeFilter();
        evtTypeFilter2.evtType = EvtTypeFilter.DataType.DEVICE_DATA;
        evtTypeFilter2.property = URI.create("DeviceName");
        assertEquals(evtTypeFilter1, evtTypeFilter2);
        assertEquals(evtTypeFilter1.hashCode(), evtTypeFilter2.hashCode());

        evtTypeFilter2.property = URI.create("DeviceName2");
        assertNotEquals(evtTypeFilter1, evtTypeFilter2);
        evtTypeFilter2.evtType = EvtTypeFilter.DataType.PROCESSED_DATA;

        final EvtTypeFilter evtTypeFilter3 = new EvtTypeFilter();
        assertNotEquals(evtTypeFilter1, evtTypeFilter3);
        assertDoesNotThrow(() -> evtTypeFilter1.copyTo(null));
        assertDoesNotThrow(() -> evtTypeFilter1.copyTo(evtTypeFilter3));
        assertEquals(evtTypeFilter1, evtTypeFilter3);
    }

    @Test
    void predicateTsts() {
        final EvtTypeFilter evtTypeFilter = new EvtTypeFilter();

        evtTypeFilter.evtType = EvtTypeFilter.DataType.TIMING_EVENT;
        evtTypeFilter.property = URI.create("TimingEventName");
        assertTrue(EvtTypeFilter.isTimingData().test(evtTypeFilter));
        assertTrue(EvtTypeFilter.isTimingData("TimingEventName").test(evtTypeFilter));

        evtTypeFilter.evtType = EvtTypeFilter.DataType.DEVICE_DATA;
        evtTypeFilter.property = URI.create("DeviceName");
        assertTrue(EvtTypeFilter.isDeviceData().test(evtTypeFilter));
        assertTrue(EvtTypeFilter.isDeviceData("DeviceName").test(evtTypeFilter));

        evtTypeFilter.evtType = EvtTypeFilter.DataType.SETTING_SUPPLY_DATA;
        evtTypeFilter.property = URI.create("SettingName");
        assertTrue(EvtTypeFilter.isSettingsData().test(evtTypeFilter));
        assertTrue(EvtTypeFilter.isSettingsData("SettingName").test(evtTypeFilter));
    }

    private static void assertInitialised(final EvtTypeFilter evtTypeFilter) {
        assertNotNull(evtTypeFilter.property);
        assertTrue(evtTypeFilter.property.toString().isBlank());
        assertEquals(EvtTypeFilter.DataType.UNKNOWN, evtTypeFilter.evtType);
        assertEquals(0, evtTypeFilter.hashCode);
    }
}
