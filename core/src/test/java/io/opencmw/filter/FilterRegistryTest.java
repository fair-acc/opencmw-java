package io.opencmw.filter;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests global FilterRegistry
 * @author rstein
 */
class FilterRegistryTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(FilterRegistryTest.class);
    private static final int TIME_OUT_MILLIS = 1000;
    private static final int MIN_N_WRITES = 1000; // tuned/dependent on TIME_OUT_MILLIS
    private Exception exception;
    private long start;

    @Test
    void basicTest() {
        FilterRegistry.clear();
        assertTrue(FilterRegistry.getKeyFilterMap().isEmpty(), "initial empty filter map");
        assertThrows(Exception.class, () -> FilterRegistry.registerNewFilter(SubscriptionMatcherTest.BogusFilter.class));
        assertTrue(FilterRegistry.getKeyFilterMap().isEmpty(), "initial empty filter map");
        assertDoesNotThrow(() -> assertTrue(FilterRegistry.registerNewFilter(TimingCtx.class)));
        assertDoesNotThrow(() -> assertFalse(FilterRegistry.registerNewFilter(TimingCtx.class)));
        assertEquals("ctx", FilterRegistry.getClassFilterKeyMap().get(TimingCtx.class));
        assertEquals(1, FilterRegistry.getKeyFilterMap().size(), "filter map size = 1");

        FilterRegistry.clear();
        assertTrue(FilterRegistry.getKeyFilterMap().isEmpty(), "initial empty filter map");
        assertDoesNotThrow(() -> assertTrue(FilterRegistry.checkClassForNewFilters(new ContextWithFilter())));
        assertDoesNotThrow(() -> assertFalse(FilterRegistry.checkClassForNewFilters(new ContextWithFilter())));
        assertEquals(1, FilterRegistry.getKeyFilterMap().size(), "filter map size = 1");
    }

    @Test
    @Timeout(value = 2 * TIME_OUT_MILLIS, unit = TimeUnit.MILLISECONDS)
    void concurrentAccessTest() {
        ExecutorService threadPool = Executors.newCachedThreadPool();
        Thread testThread = Thread.currentThread();

        start = System.currentTimeMillis();
        final List<Future<Integer>> writerJobs = new ArrayList<>(10);
        for (int iw = 0; iw < 10; iw++) {
            writerJobs.add(threadPool.submit(() -> writeLoop(testThread)));
        }

        int nWritesTotal = 0;
        try {
            for (Future<Integer> task : writerJobs) {
                final Integer nWrites = task.get(TIME_OUT_MILLIS, TimeUnit.MILLISECONDS);
                assertTrue(nWrites >= MIN_N_WRITES, "starved writer - nWrites = " + nWrites);
                nWritesTotal += nWrites;
            }
        } catch (Exception e) {
            if (exception != null) {
                final long diff = System.currentTimeMillis() - start;
                throw new IllegalStateException("terminated after " + diff + " ms", exception);
            }
        }
        threadPool.shutdown();
        assertNull(exception, "did not finish test without errors");
        LOGGER.atInfo().addArgument(TIME_OUT_MILLIS).addArgument(nWritesTotal).log("qualitative performance over {} ms: nWrites = {}");
    }

    private int writeLoop(Thread testThread) {
        int nWrites = 0;
        try {
            while (!Thread.interrupted() && (TimeUnit.MILLISECONDS.toMillis(System.currentTimeMillis() - start) < TIME_OUT_MILLIS)) {
                FilterRegistry.registerNewFilter(TimingCtx.class);
                nWrites++;
            }
        } catch (Exception e) {
            exception = e; // forward exception to static class context
            testThread.interrupt();
            return -1;
        }
        return nWrites;
    }

    private static class ContextWithFilter {
        TimingCtx ctx;
        String notAFilter;
    }
}