package io.opencmw.filter;

import java.net.URI;
import java.util.function.BiPredicate;

import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import io.opencmw.filter.PathSubscriptionMatcher;

class PathSubscriptionMatcherTest {
    @Test
    void testMatcher() {
        final BiPredicate<URI, URI> matcher = new PathSubscriptionMatcher();

        test(matcher, true, "property", "property");
        test(matcher, false, "property/A", "property");
        test(matcher, true, "property/A", "property*");
        test(matcher, true, "property/A/B", "property*");
        test(matcher, false, "property", "property2");
        test(matcher, true, "property?testQuery", "property");
        test(matcher, true, "property?testQuery", "property*");

        // not yet implemented - defaults to 'true' TODO: upgrade
        test(matcher, true, "property?testQuery", "property?testQuery");
        test(matcher, true, "property?testQuery", "property*?testQuery");
        test(matcher, true, "property/A?testQuery", "property*?testQuery");
        test(matcher, true, "property", "property?testQuery");
        test(matcher, true, "property", "property*?testQuery");
    }

    private static void test(final BiPredicate<URI, URI> matcher, final boolean expected, final String notify, final String subscription) throws AssertionFailedError {
        final boolean actual = matcher.test(URI.create(notify), URI.create(subscription));
        if (actual != expected) {
            final String message = "notify: '" + notify + "' vs. subscription: '" + subscription + "'";
            throw new AssertionFailedError(message, expected, actual);
        }
    }
}