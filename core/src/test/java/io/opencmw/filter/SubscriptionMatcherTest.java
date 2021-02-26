package io.opencmw.filter;

import static org.junit.jupiter.api.Assertions.*;

import java.net.URI;
import java.util.Objects;
import java.util.function.BiPredicate;

import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import io.opencmw.Filter;

class SubscriptionMatcherTest {
    @Test
    void testMatcherPathOnly() {
        final BiPredicate<URI, URI> matcher = new SubscriptionMatcher();

        test(matcher, true, "property", "property");
        test(matcher, false, "property/A", "property");
        test(matcher, true, "property/A", "property*");
        test(matcher, true, "property/A/B", "property*");
        test(matcher, false, "property", "property2");
        test(matcher, true, "property?testQuery", "property");
        test(matcher, true, "property?testQuery", "property*");

        // no filter configuration -> ignores query and matches only path
        test(matcher, true, "property?testQuery", "property?testQuery");
        test(matcher, true, "property?testQuery", "property*?testQuery");
        test(matcher, true, "property/A?testQuery", "property*?testQuery");
        test(matcher, true, "property", "property?testQuery");
        test(matcher, true, "property", "property*?testQuery");
    }

    @Test
    void testMatcherPathAndQuery() {
        FilterTests.test(TestFilter1.class, TestFilter1.class.getName());
        FilterTests.test(TestFilter2.class, TestFilter2.class.getName());
        assertThrows(IllegalArgumentException.class, () -> new SubscriptionMatcher(BogusFilter.class));

        final BiPredicate<URI, URI> matcher = new SubscriptionMatcher(TestFilter1.class, TestFilter2.class);

        test(matcher, false, "property1?testKey1", "property2?testKey1");
        test(matcher, true, "property?testKey1", "property?testKey1");
        test(matcher, true, "property?testKey1&testKey2", "property?testKey1&testKey2");
        test(matcher, true, "property?testKey1&testKey2", "property?testKey2&testKey1");
        test(matcher, true, "property?testKey1=42&testKey2=24", "property?testKey2=24&testKey1=42");
        test(matcher, false, "property?testKey1=41", "property*?testKey1=4711");
        test(matcher, true, "property/A?testKey1=41", "property*?testKey1=41");
        test(matcher, true, "property", "property?testQuery"); // ignore unknown ctx filter on subscription side
        test(matcher, true, "property", "property*?testQuery"); // ignore unknown ctx filter on subscription side
        test(matcher, true, "property?testKey1", "property*");
        test(matcher, true, "property?testKey1", "property?TestKey1"); // N.B. key is case sensitive
        test(matcher, false, "property?testKey1", "property?testKey1=42");
        test(matcher, false, "property", "property?testKey1=42");
    }

    @Test
    void testMatcherTimingContentContext() {
        FilterTests.test(TimingCtx.class, TimingCtx.class.getName());
        FilterTests.test(ContentTypeFilter.class, ContentTypeFilter.class.getName());

        final BiPredicate<URI, URI> matcher = new SubscriptionMatcher(TimingCtx.class, ContentTypeFilter.class);
        TimingCtx ctx1 = new TimingCtx("FAIR.SELECTOR.ALL");
        TimingCtx ctx2 = new TimingCtx("FAIR.SELECTOR.C=2");
        assertTrue(ctx1.matches(ctx2), "context selector ctx1 " + ctx1 + " matches" + ctx2);
        assertFalse(TimingCtx.getStatic("FAIR.SELECTOR.C=2:P=1").matches(TimingCtx.getStatic("FAIR.SELECTOR.C=2")), "context selector ctx1 FAIR.SELECTOR.C=2:P=1 !matches FAIR.SELECTOR.C=2");

        test(matcher, false, "property?ctx=FAIR.SELECTOR.ALL", "property?ctx=FAIR.SELECTOR.C=2");
        test(matcher, true, "property?ctx=FAIR.SELECTOR.C=2", "property?ctx=FAIR.SELECTOR.ALL");
        test(matcher, true, "property?ctx=FAIR.SELECTOR.C=2", "property?ctx=FAIR.SELECTOR.C=2");
        test(matcher, true, "property?ctx=FAIR.SELECTOR.C=2:P=1", "property?ctx=FAIR.SELECTOR.C=2:P=1"); // notify not specific enough (missing 'P=1')
        test(matcher, false, "property?ctx=FAIR.SELECTOR.C=2", "property?ctx=FAIR.SELECTOR.C=2:P=1"); // notify not specific enough (missing 'P=1')
        test(matcher, false, "property?ctx=FAIR.SELECTOR.ALL&contentType=text/html", "property?ctx=FAIR.SELECTOR.C=2&contentType=text/html");
        test(matcher, false, "property?ctx=FAIR.SELECTOR.ALL", "property?ctx=FAIR.SELECTOR.C=2&contentType=text/html");
    }

    private static void test(final BiPredicate<URI, URI> matcher, final boolean expected, final String notify, final String subscription) throws AssertionFailedError {
        final boolean actual = matcher.test(URI.create(notify), URI.create(subscription));
        if (actual != expected) {
            final String message = "notify: '" + notify + "' vs. subscription: '" + subscription + "'";
            throw new AssertionFailedError(message, expected, actual);
        }
    }

    public static class TestFilter1 implements Filter {
        private static final String KEY = "testKey1";
        private int testValue = 42;

        @Override
        public void clear() {
            testValue = 0;
        }

        @Override
        public void copyTo(final Filter other) {
        }

        @Override
        public String getKey() {
            return KEY;
        }

        @Override
        public String getValue() {
            return Integer.toString(testValue);
        }

        @Override
        public Filter get(final String value) {
            final TestFilter1 filter = new TestFilter1();
            try {
                filter.testValue = Integer.parseInt(value);
            } catch (NumberFormatException e) {
                return null;
            }
            return filter;
        }

        @Override
        public boolean matches(final Filter other) {
            return equals(other);
        }

        @Override
        public boolean equals(final Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof TestFilter1)) {
                return false;
            }
            return testValue == ((TestFilter1) other).testValue;
        }

        @Override
        public int hashCode() {
            return testValue;
        }

        @Override
        public String toString() {
            return "TestFilter1{testValue=" + testValue + '}';
        }
    }

    public static class TestFilter2 implements Filter {
        private static final String KEY = "testKey2";
        private String testString = "";

        @Override
        public void clear() {
        }

        @Override
        public void copyTo(final Filter other) {
        }

        @Override
        public String getKey() {
            return KEY;
        }

        @Override
        public String getValue() {
            return testString;
        }

        @Override
        public Filter get(final String value) {
            final TestFilter2 filter = new TestFilter2();
            filter.testString = value;
            return filter;
        }

        @Override
        public boolean matches(final Filter other) {
            return equals(other);
        }

        @Override
        public boolean equals(final Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof TestFilter2)) {
                return false;
            }
            return Objects.equals(testString, ((TestFilter2) other).testString);
        }

        @Override
        public int hashCode() {
            return testString == null ? 0 : testString.hashCode();
        }

        @Override
        public String toString() {
            return "TestFilter2{testString='" + testString + '\'' + '}';
        }
    }

    /* default */ static class BogusFilter implements Filter { // NOPMD default access
        private BogusFilter() {
            // cannot instantiate this publicly
        }

        @Override
        public void clear() {
            // never called
        }

        @Override
        public void copyTo(final Filter other) {
            // never called
        }

        @Override
        public String getKey() {
            return "bogusFilter";
        }

        @Override
        public String getValue() {
            return "";
        }

        @Override
        public BogusFilter get(final String value) {
            return new BogusFilter();
        }

        @Override
        public boolean matches(final Filter other) {
            return false;
        }
    }
}