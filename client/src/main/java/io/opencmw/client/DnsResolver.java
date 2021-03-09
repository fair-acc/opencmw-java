package io.opencmw.client;

import java.net.URI;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;

/**
 * basic interface to resolve a given service URI to connectable URI.
 *
 * N.B. Follows URI syntax, ie. '<pre>scheme:[//authority]path[?query][#fragment]</pre>' see <a href="https://tools.ietf.org/html/rfc3986">documentation</a>
 */
public interface DnsResolver extends AutoCloseable {
    /**
     * @return returns the list of applicable schemes (and protocols this resolver can handle) this resolver can handle
     */
    List<String> getApplicableSchemes();

    /**
     *
     * @param devicesToResolve list of partial (ie. w/o scheme, authority,or only the beginning of the path) URIs that need to get resolved.
     * @return map containing the initial request as key and list of matching fully resolved URI (ie. including scheme, authority and full path). N.B. list may be empty
     */
    Map<URI, List<URI>> resolveNames(List<URI> devicesToResolve) throws UnknownHostException;
}
