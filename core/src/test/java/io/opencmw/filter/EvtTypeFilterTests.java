package io.opencmw.filter;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class EvtTypeFilterTests {
    @Test
    void basicTests() {
        assertDoesNotThrow(EvtTypeFilter::new);

        final EvtTypeFilter evtTypeFilter = new EvtTypeFilter();
        assertInitialised(evtTypeFilter);

        evtTypeFilter.clear();
        assertInitialised(evtTypeFilter);

        assertNotNull(evtTypeFilter.toString());
    }

    @Test
    void testEqualsAndHash() {
        final EvtTypeFilter evtTypeFilter1 = new EvtTypeFilter();
        evtTypeFilter1.evtType = EvtTypeFilter.DataType.DEVICE_DATA;
        evtTypeFilter1.typeName = "DeviceName";
        // check identity
        assertEquals(evtTypeFilter1, evtTypeFilter1);
        assertEquals(evtTypeFilter1.hashCode(), evtTypeFilter1.hashCode());
        assertTrue(EvtTypeFilter.isDeviceData().test(evtTypeFilter1));
        assertTrue(EvtTypeFilter.isDeviceData("DeviceName").test(evtTypeFilter1));

        assertNotEquals(evtTypeFilter1, new Object());

        final EvtTypeFilter evtTypeFilter2 = new EvtTypeFilter();
        evtTypeFilter2.evtType = EvtTypeFilter.DataType.DEVICE_DATA;
        evtTypeFilter2.typeName = "DeviceName";
        assertEquals(evtTypeFilter1, evtTypeFilter2);
        assertEquals(evtTypeFilter1.hashCode(), evtTypeFilter2.hashCode());

        evtTypeFilter2.typeName = "DeviceName2";
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
        evtTypeFilter.typeName = "TimingEventName";
        assertTrue(EvtTypeFilter.isTimingData().test(evtTypeFilter));
        assertTrue(EvtTypeFilter.isTimingData("TimingEventName").test(evtTypeFilter));

        evtTypeFilter.evtType = EvtTypeFilter.DataType.DEVICE_DATA;
        evtTypeFilter.typeName = "DeviceName";
        assertTrue(EvtTypeFilter.isDeviceData().test(evtTypeFilter));
        assertTrue(EvtTypeFilter.isDeviceData("DeviceName").test(evtTypeFilter));

        evtTypeFilter.evtType = EvtTypeFilter.DataType.SETTING_SUPPLY_DATA;
        evtTypeFilter.typeName = "SettingName";
        assertTrue(EvtTypeFilter.isSettingsData().test(evtTypeFilter));
        assertTrue(EvtTypeFilter.isSettingsData("SettingName").test(evtTypeFilter));
    }

    private static void assertInitialised(final EvtTypeFilter evtTypeFilter) {
        assertNotNull(evtTypeFilter.typeName);
        assertTrue(evtTypeFilter.typeName.isBlank());
        assertEquals(EvtTypeFilter.DataType.UNKNOWN, evtTypeFilter.evtType);
        assertEquals(0, evtTypeFilter.hashCode);
    }
}
