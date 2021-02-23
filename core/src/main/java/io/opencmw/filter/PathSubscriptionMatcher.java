package io.opencmw.filter;

import java.net.URI;
import java.util.function.BiPredicate;

import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;

/**
 * basic path-only predicate to match notified topics to requested/subscriber topics
 *
 * @author rstein
 */
public class PathSubscriptionMatcher implements BiPredicate<URI, URI> {
    @Override
    public boolean test(final @NotNull URI notified, final @NotNull URI subscriber) {
        final String notPath = notified.getPath();
        // final String notQuery = notified.getQuery()
        final String subPath = subscriber.getPath();
        final String subPathCleaned = StringUtils.removeEnd(subPath, "*");
        final String subQuery = subscriber.getQuery();

        final int asterisk = subPath.indexOf('*');
        if (asterisk == -1 && subQuery == null) {
            // match path only - exact
            return notPath.equals(subPath) || subPath.isBlank();
        } else if (asterisk >= 0 && subQuery == null) {
            // match path (leading characters) only - assumes trailing asterisk
            return notPath.startsWith(subPathCleaned) || subPath.isBlank();
        }

        // N.B. for the time being, for all other cases only the exact path is matched - TODO: upgrade to full topic matching
        return subPath.isBlank() || (asterisk < 0 ? notPath.equals(subPath) : notPath.startsWith(subPathCleaned));
    }
}
