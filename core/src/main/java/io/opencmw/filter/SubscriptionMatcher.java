package io.opencmw.filter;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;

import io.opencmw.Filter;
import io.opencmw.QueryParameterParser;

/**
 * basic path-only as well as path-and-context predicate to match notified topics to requested/subscriber topics
 *
 * @author rstein
 */
public class SubscriptionMatcher implements BiPredicate<URI, URI> {
    protected final Map<String, Filter> filterMap = new HashMap<>(); // NOPMD <filter key, filter prototype> - thread-safe use -- only reads after initialisation
    protected final boolean isPathOnly;

    @SafeVarargs
    public SubscriptionMatcher(final Class<? extends Filter>... filterConfig) {
        Objects.requireNonNull(filterConfig, "filterConfig must not be null");
        isPathOnly = filterConfig.length == 0;
        try {
            for (final Class<? extends Filter> aClass : filterConfig) {
                final Constructor<? extends Filter> constructor = aClass.getDeclaredConstructor();
                final Filter prototype = constructor.newInstance(); // needed to have access to non-static 'get("..")' initialsers as Interfaces cannot have statics
                final String key = prototype.getKey(); // instantiated for getting key and to check that the default constructor for the filter is declared
                filterMap.put(key, prototype);
            }
        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new IllegalArgumentException("filter must be public and have a public default constructor", e);
        }
    }

    @Override
    public boolean test(final @NotNull URI notified, final @NotNull URI subscriber) {
        return isPathOnly ? testPathOnly(notified, subscriber) : testPathAndContext(notified, subscriber);
    }

    private boolean testPathAndContext(final URI notified, final URI subscriber) {
        if (!testPathOnly(notified, subscriber)) {
            // paths are not matching so let's not bother parsing any further
            return false;
        }
        final String queryNotification = notified.getQuery();
        final String querySubscriber = subscriber.getQuery();
        if (Objects.equals(querySubscriber, queryNotification)) {
            return true;
        }
        if ((queryNotification != null && querySubscriber == null)) {
            return true;
        }
        final Map<String, String> mapNotify = getReducedMap(queryNotification);
        final Map<String, String> mapSubscribe = getReducedMap(querySubscriber);
        return mapSubscribe.entrySet().stream().filter(e -> {
                                                   // N.B. inverted logic - stop at first mismatch
                                                   final String ctxKey = e.getKey();
                                                   final Filter protoFilter = filterMap.get(ctxKey);
                                                   if (protoFilter == null) {
                                                       // provided subscription filter unknown - continue with next
                                                       return false;
                                                   }
                                                   final String subscriptionQuerySubString = e.getValue();
                                                   final String notifyQuerySubString = mapNotify.get(ctxKey);
                                                   if (notifyQuerySubString == null && !subscriptionQuerySubString.isBlank()) {
                                                       // specific/required subscription topic but not corresponding filter in notification set
                                                       return true;
                                                   }
                                                   final Filter subscriptionFilter = protoFilter.get(subscriptionQuerySubString);
                                                   if (subscriptionFilter == null) {
                                                       // invalid subscription filter - ignore
                                                       return false;
                                                   }
                                                   return !subscriptionFilter.matches(protoFilter.get(notifyQuerySubString));
                                               })
                .findFirst()
                .isEmpty();
    }

    protected Map<String, String> getReducedMap(final String query) {
        return QueryParameterParser.getMap(query).entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> {
            final List<String> values = e.getValue();
            return Objects.requireNonNullElse(values.get(values.size() - 1), ""); // pick last matching query parameter in case of multiple parameters with the same key
        }));
    }

    /**
     *
     * @param notified notification topic's URI
     * @param subscriber subscriber's URI
     * @return true if path matches
     */
    public static boolean testPathOnly(final @NotNull URI notified, final @NotNull URI subscriber) {
        final String pathNotification = notified.getPath();
        final String pathSubscriber = subscriber.getPath();

        final int asterisk = pathSubscriber.indexOf('*');
        if (asterisk == -1) {
            // no asterisk match path only - exact
            return pathNotification.equals(pathSubscriber) || pathSubscriber.isBlank();
        }
        final String pathSubscriberCleaned = StringUtils.removeEnd(pathSubscriber, "*");
        // match path (leading characters) only - assumes trailing asterisk
        return pathNotification.startsWith(pathSubscriberCleaned) || pathSubscriber.isBlank();
    }
}
