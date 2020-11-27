package de.gsi.microservice.filter;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class TimingCtxTests {
    @Test
    void basicTests() {
        assertDoesNotThrow(TimingCtx::new);

        final long timeNowMicros = System.currentTimeMillis() * 1000;
        final TimingCtx ctx = new TimingCtx();
        assertInitialised(ctx);

        assertDoesNotThrow(() -> ctx.setSelector("FAIR.SELECTOR.C=0:S=1:P=2:T=3", timeNowMicros));
        assertEquals(0, ctx.cid);
        assertEquals(1, ctx.sid);
        assertEquals(2, ctx.pid);
        assertEquals(3, ctx.gid);
        assertEquals(timeNowMicros, ctx.bpcts);
        assertNotNull(ctx.toString());
        ctx.clear();

        assertThrows(IllegalArgumentException.class, () -> ctx.setSelector("FAIR.SELECTOR.C=0:S=1:P=2:T=3", -1));
        assertInitialised(ctx);

        // add unknown/erroneous tag
        assertThrows(IllegalArgumentException.class, () -> ctx.setSelector("FAIR.SELECTOR.C0:S=1:P=2:T=3", timeNowMicros));
        assertInitialised(ctx);
        assertThrows(IllegalArgumentException.class, () -> ctx.setSelector("FAIR.SELECTOR.X=1", timeNowMicros));
        assertInitialised(ctx);

        assertThrows(IllegalArgumentException.class, () -> ctx.setSelector(null, timeNowMicros));
        assertInitialised(ctx);
    }

    @Test
    void basicAllSelectorTests() {
        final long timeNowMicros = System.currentTimeMillis() * 1000;
        final TimingCtx ctx = new TimingCtx();

        // empty selector
        assertDoesNotThrow(() -> ctx.setSelector("", timeNowMicros));
        assertEquals("", ctx.selector.toUpperCase());
        assertAllWildCard(ctx);
        assertEquals(timeNowMicros, ctx.bpcts);

        // "ALL" selector
        assertDoesNotThrow(() -> ctx.setSelector(TimingCtx.WILD_CARD, timeNowMicros));
        assertEquals(TimingCtx.WILD_CARD, ctx.selector.toUpperCase());
        assertAllWildCard(ctx);
        assertEquals(timeNowMicros, ctx.bpcts);

        // "FAIR.SELECTOR.ALL" selector
        assertDoesNotThrow(() -> ctx.setSelector("FAIR.SELECTOR.ALL", timeNowMicros));
        assertEquals(TimingCtx.SELECTOR_PREFIX + TimingCtx.WILD_CARD, ctx.selector.toUpperCase());
        assertAllWildCard(ctx);
        assertEquals(timeNowMicros, ctx.bpcts);
    }

    @Test
    void testHelper() {
        assertTrue(TimingCtx.wildCardMatch(TimingCtx.WILD_CARD_VALUE, 2));
        assertTrue(TimingCtx.wildCardMatch(TimingCtx.WILD_CARD_VALUE, -1));
        assertTrue(TimingCtx.wildCardMatch(1, TimingCtx.WILD_CARD_VALUE));
        assertTrue(TimingCtx.wildCardMatch(-1, TimingCtx.WILD_CARD_VALUE));
        assertFalse(TimingCtx.wildCardMatch(3, 2));
    }

    @Test
    void testEqualsAndHash() {
        final long timeNowMicros = System.currentTimeMillis() * 1000;
        final TimingCtx ctx1 = new TimingCtx();
        assertDoesNotThrow(() -> ctx1.setSelector("FAIR.SELECTOR.C=0:S=1:P=2:T=3", timeNowMicros));
        // check identity
        assertEquals(ctx1, ctx1);
        assertEquals(ctx1.hashCode(), ctx1.hashCode());

        assertNotEquals(ctx1, new Object());

        final TimingCtx ctx2 = new TimingCtx();
        assertDoesNotThrow(() -> ctx2.setSelector("FAIR.SELECTOR.C=0:S=1:P=2:T=3", timeNowMicros));

        assertEquals(ctx1, ctx2);
        assertEquals(ctx1.hashCode(), ctx2.hashCode());

        ctx2.bpcts++;
        assertNotEquals(ctx1, ctx2);
        ctx2.gid = -1;
        assertNotEquals(ctx1, ctx2);
        ctx2.pid = -1;
        assertNotEquals(ctx1, ctx2);
        ctx2.sid = -1;
        assertNotEquals(ctx1, ctx2);
        ctx2.cid = -1;
        assertNotEquals(ctx1, ctx2);

        final TimingCtx ctx3 = new TimingCtx();
        assertNotEquals(ctx1, ctx3);
        assertDoesNotThrow(() -> ctx1.copyTo(null));
        assertDoesNotThrow(() -> ctx1.copyTo(ctx3));
        assertEquals(ctx1, ctx3);
    }

    @Test
    void basicSelectorTests() {
        final long timeNowMicros = System.currentTimeMillis() * 1000;
        final TimingCtx ctx = new TimingCtx();
        assertInitialised(ctx);

        // "FAIR.SELECTOR.C=2" selector
        assertDoesNotThrow(() -> ctx.setSelector("FAIR.SELECTOR.C=2", timeNowMicros));
        assertEquals(2, ctx.cid);
        assertEquals(-1, ctx.sid);
        assertEquals(-1, ctx.pid);
        assertEquals(-1, ctx.gid);
        assertEquals(timeNowMicros, ctx.bpcts);
    }

    @Test
    void matchingTests() { // NOPMD NOSONAR -- number of assertions is OK ... it's a simple unit-test
        final long timeNowMicros = System.currentTimeMillis() * 1000;
        final TimingCtx ctx = new TimingCtx();
        ctx.setSelector("FAIR.SELECTOR.C=0:S=1:P=2:T=3", timeNowMicros);

        assertTrue(ctx.matches(ctx).test(ctx));
        assertTrue(TimingCtx.matches(0, timeNowMicros).test(ctx));
        assertTrue(TimingCtx.matches(0, 1, timeNowMicros).test(ctx));
        assertTrue(TimingCtx.matches(0, 1, 2, timeNowMicros).test(ctx));
        assertTrue(TimingCtx.matches(0, 1, 2).test(ctx));
        assertTrue(TimingCtx.matches(-1, 1, 2).test(ctx));
        assertFalse(TimingCtx.matches(0, 0, 2).test(ctx));
        assertFalse(TimingCtx.matches(0, 1, 0).test(ctx));

        assertTrue(TimingCtx.matchesBpcts(timeNowMicros).test(ctx));
        assertTrue(TimingCtx.isOlderBpcts(timeNowMicros + 1L).test(ctx));
        assertTrue(TimingCtx.isNewerBpcts(timeNowMicros - 1L).test(ctx));

        // test wildcard
        ctx.setSelector("FAIR.SELECTOR.C=0:S=1", timeNowMicros);
        assertEquals(0, ctx.cid);
        assertEquals(1, ctx.sid);
        assertEquals(-1, ctx.pid);
        assertTrue(TimingCtx.matches(0, 1).test(ctx));
        assertTrue(TimingCtx.matches(0, 1, -1).test(ctx));
        assertTrue(TimingCtx.matches(0, timeNowMicros).test(ctx));
    }

    private static void assertAllWildCard(final TimingCtx ctx) {
        assertEquals(-1, ctx.cid);
        assertEquals(-1, ctx.sid);
        assertEquals(-1, ctx.pid);
        assertEquals(-1, ctx.gid);
    }

    private static void assertInitialised(final TimingCtx ctx) {
        assertNull(ctx.selector);
        assertAllWildCard(ctx);
        assertEquals(-1, ctx.bpcts);
        assertEquals(0, ctx.hashCode);
    }
}
