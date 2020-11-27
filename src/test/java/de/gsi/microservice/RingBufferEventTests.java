package de.gsi.microservice;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import de.gsi.microservice.filter.EvtTypeFilter;
import de.gsi.microservice.filter.TimingCtx;
import de.gsi.microservice.utils.SharedPointer;

class RingBufferEventTests {
    @Test
    void basicTests() {
        assertDoesNotThrow(() -> new RingBufferEvent(TimingCtx.class));
        assertThrows(IllegalArgumentException.class, () -> new RingBufferEvent(TimingCtx.class, BogusFilter.class));

        final RingBufferEvent evt = new RingBufferEvent(TimingCtx.class);
        assertFalse(evt.matches(String.class));
        evt.payload = new SharedPointer<>();
        assertFalse(evt.matches(String.class));
        evt.payload.set("Hello World");
        assertTrue(evt.matches(String.class));
        evt.throwables.add(new Throwable("test"));
        assertNotNull(evt.toString());

        // assert copy/clone interfaces
        assertEquals(evt, evt.clone());
        final RingBufferEvent evt2 = new RingBufferEvent(TimingCtx.class);
        evt.copyTo(evt2);
        assertEquals(evt, evt2);

        assertDoesNotThrow(evt::clear);
        assertEquals(0, evt.throwables.size());
        assertEquals(0, evt.arrivalTimeStamp);

        final long timeNowMicros = System.currentTimeMillis() * 1000;
        final TimingCtx ctxFilter = evt.getFilter(TimingCtx.class);
        assertNotNull(ctxFilter);
        assertThrows(IllegalArgumentException.class, () -> evt.getFilter(BogusFilter.class));

        ctxFilter.setSelector("FAIR.SELECTOR.C=3:S=2", timeNowMicros);

        // assert copy/clone interfaces for cleared evt
        evt.clear();
        assertEquals(evt, evt.clone());
        final RingBufferEvent evt3 = new RingBufferEvent(TimingCtx.class);
        evt.copyTo(evt3);
        assertEquals(evt, evt3);
    }

    @Test
    void basicUsageTests() {
        final RingBufferEvent evt = new RingBufferEvent(TimingCtx.class, EvtTypeFilter.class);
        assertNotNull(evt);
        final long timeNowMicros = System.currentTimeMillis() * 1000;
        evt.arrivalTimeStamp = timeNowMicros;
        evt.getFilter(EvtTypeFilter.class).evtType = EvtTypeFilter.DataType.DEVICE_DATA;
        evt.getFilter(EvtTypeFilter.class).typeName = "MyDevice";
        evt.getFilter(TimingCtx.class).setSelector("FAIR.SELECTOR.C=3:S=2", timeNowMicros);

        evt.matches(TimingCtx.class, ctx -> {
            System.err.println("received ctx = " + ctx);
            return true;
        });

        // fall-back filter: the whole RingBufferEvent, all Filters etc are accessible
        assertTrue(evt.matches(e -> e.arrivalTimeStamp == timeNowMicros));

        // filter only on given filter trait - here TimingCtx
        assertTrue(evt.matches(TimingCtx.class, TimingCtx.matches(3, 2)));
        evt.test(TimingCtx.class, TimingCtx.matches(3, 2));

        // combination of filter traits
        assertTrue(evt.test(TimingCtx.class, TimingCtx.matches(3, 2)) && evt.test(EvtTypeFilter.class, dataType -> dataType.evtType == EvtTypeFilter.DataType.DEVICE_DATA));
        assertTrue(evt.test(TimingCtx.class, TimingCtx.matches(3, 2)) && evt.test(EvtTypeFilter.class, EvtTypeFilter.isDeviceData("MyDevice")));
        assertTrue(evt.test(TimingCtx.class, TimingCtx.matches(3, 2).and(TimingCtx.isNewerBpcts(timeNowMicros - 1L))));
    }

    @Test
    void equalsTests() {
        final RingBufferEvent evt1 = new RingBufferEvent(TimingCtx.class);
        final RingBufferEvent evt2 = new RingBufferEvent(TimingCtx.class);

        assertEquals(evt1, evt1, "equals identity");
        assertNotEquals(null, evt1, "equals null");
        evt1.parentSequenceNumber = 42;
        assertNotEquals(evt1.hashCode(), evt2.hashCode(), "equals hashCode");
        assertNotEquals(evt1, evt2, "equals hashCode");
    }

    @Test
    void testClearEventHandler() {
        final RingBufferEvent evt = new RingBufferEvent(TimingCtx.class, EvtTypeFilter.class);
        assertNotNull(evt);
        final long timeNowMicros = System.currentTimeMillis() * 1000;
        evt.arrivalTimeStamp = timeNowMicros;

        assertEquals(timeNowMicros, evt.arrivalTimeStamp);
        assertDoesNotThrow(RingBufferEvent.ClearEventHandler::new);

        final RingBufferEvent.ClearEventHandler clearHandler = new RingBufferEvent.ClearEventHandler();
        assertNotNull(clearHandler);

        clearHandler.onEvent(evt, 0, false);
        assertEquals(0, evt.arrivalTimeStamp);
    }

    @Test
    void testHelper() {
        assertNotNull(RingBufferEvent.getPrintableStackTrace(new Throwable("pretty print")));
        assertNotNull(RingBufferEvent.getPrintableStackTrace(null));
        StringBuilder builder = new StringBuilder();
        assertDoesNotThrow(() -> RingBufferEvent.printToStringArrayList(builder, "[", "]", 1, 2, 3, 4));
        assertDoesNotThrow(() -> RingBufferEvent.printToStringArrayList(builder, null, "]", 1, 2, 3, 4));
        assertDoesNotThrow(() -> RingBufferEvent.printToStringArrayList(builder, "[", null, 1, 2, 3, 4));
        assertDoesNotThrow(() -> RingBufferEvent.printToStringArrayList(builder, "", "]", 1, 2, 3, 4));
        assertDoesNotThrow(() -> RingBufferEvent.printToStringArrayList(builder, "[", "", 1, 2, 3, 4));
    }

    private class BogusFilter implements Filter {
        public BogusFilter() {
            throw new IllegalStateException("should not call/use this filter");
        }

        @Override
        public void clear() {
            // never called
        }

        @Override
        public void copyTo(final Filter other) {
            // never called
        }
    }
}
