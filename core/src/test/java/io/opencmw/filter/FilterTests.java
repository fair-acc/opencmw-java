package io.opencmw.filter;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import io.opencmw.Filter;

/**
 * tests basic Filter interface API contracts for known implementations
 *
 * @author rstein
 */
class FilterTests {
    @Test
    void testApiConformity() {
        test(TimingCtx.class, TimingCtx.class.getSimpleName());
        test(EvtTypeFilter.class, EvtTypeFilter.class.getSimpleName());
        test(ContentTypeFilter.class, ContentTypeFilter.class.getSimpleName());
    }

    public static void test(final Class<? extends Filter> filterClass, final String filterName) throws AssertionFailedError {
        final Filter filter;
        try {
            filter = filterClass.getDeclaredConstructor().newInstance();
        } catch (Exception e) { // NOPMD
            throw new AssertionFailedError("instantiation assertion for class " + filterName + " - has no public constructor", e);
        }

        assertNotNull(filter.toString(), "filter toString() representation: " + filterName);
        assertFalse(filter.toString().isBlank(), "filter toString() blank: " + filterName);
        assertNotNull(filter.getKey(), "filter key null: " + filterName);
        assertFalse(filter.getKey().isBlank(), "filter key blank: " + filterName);
        assertNotNull(filter.getValue(), "filter value null: " + filterName);
        // filter value may be blank
        assertNotNull(filter.get(filter.getValue()), "initByValue for: " + filterName);

        assertDoesNotThrow(() -> filter.copyTo(null));

        try {
            final Filter filter2 = filterClass.getDeclaredConstructor().newInstance();
            assertDoesNotThrow(() -> filter.copyTo(filter2));
        } catch (Exception e) { // NOPMD
            throw new AssertionFailedError("instantiation assertion for class " + filterName, e);
        }
    }
}
