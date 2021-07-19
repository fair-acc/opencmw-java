package io.opencmw.server;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import io.opencmw.MimeType;
import io.opencmw.QueryParameterParser;
import io.opencmw.rbac.RbacRole;
import io.opencmw.serialiser.annotations.MetaInfo;

public final class MmiServiceHelper {
    public static final String INTERNAL_SERVICE_NAMES = "mmi.service";
    public static final String INTERNAL_SERVICE_OPENAPI = "mmi.openapi";
    public static final String INTERNAL_SERVICE_DNS = "mmi.dns";
    public static final String INTERNAL_SERVICE_ECHO = "mmi.echo";

    private MmiServiceHelper() {
        // nothing to instantiate
    }

    public static boolean isHtmlRequest(final URI topic) {
        return topic != null && topic.getQuery() != null && MimeType.HTML == QueryParameterParser.getMimeType(topic.getQuery());
    }

    public static String wrapInAnchor(final String text, final URI uri) {
        if (uri.getScheme() == null || uri.getScheme().equalsIgnoreCase("http") || uri.getScheme().equalsIgnoreCase("https")) {
            return "<a href=\"" + uri.toString() + "\">" + text + "</a>";
        }
        return text;
    }

    @MetaInfo(description = "output = input : echo service is that complex :-)", unit = "MMI Echo Service")
    public static class MmiEcho extends BasicMdpWorker {
        public MmiEcho(final MajordomoBroker broker, final RbacRole<?>... rbacRoles) {
            super(broker.getContext(), broker.brokerName + '/' + INTERNAL_SERVICE_ECHO, rbacRoles);
            this.registerHandler(ctx -> ctx.rep.data = ctx.req.data); //  output = input : echo service is complex :-)
        }
    }

    @MetaInfo(description = "Dynamic Name Service (DNS) returning registered internal and external broker endpoints' URIs (protocol, host ip, port etc.)", unit = "MMI DNS Service")
    public static class MmiDns extends BasicMdpWorker {
        private final Map<String, MajordomoBroker.DnsServiceItem> dnsCache;
        private final String brokerName;

        public MmiDns(final MajordomoBroker broker, final RbacRole<?>... rbacRoles) {
            super(broker.getContext(), broker.brokerName + '/' + INTERNAL_SERVICE_DNS, rbacRoles);
            dnsCache = broker.getDnsCache();
            brokerName = broker.brokerName;
            this.registerHandler(ctx -> {
                final boolean isHtml = isHtmlRequest(ctx.req.topic);
                final String delimiter = isHtml ? ", " : ",";
                final String query = ctx.req.topic.getQuery() == null ? null : ctx.req.topic.getQuery().split("&")[0];
                if (query == null || query.isBlank() || !(query.contains(",") || query.contains(":") || query.contains("/"))) {
                    final Function<MajordomoBroker.DnsServiceItem, String> mapper = isHtml ? MajordomoBroker.DnsServiceItem::getDnsEntryHtml : MajordomoBroker.DnsServiceItem::getDnsEntry;
                    ctx.rep.data = dnsCache.values().stream().map(mapper).collect(Collectors.joining(delimiter)).getBytes(UTF_8);
                    return;
                }
                // search for specific service
                ctx.rep.data = Arrays.stream(StringUtils.split(query, ',')).map(StringUtils::strip).map(this::findDnsEntry).collect(Collectors.joining(delimiter)).getBytes(UTF_8);
            });
        }

        /**
         *
         * @param entry scheme - path entry to look for (N.B. any potential authority, user or query parameter will be ignored
         * @return String containing the 'broker : fully resolved device(service)/property address(es)' or 'null'
         */
        public String findDnsEntry(final String entry) {
            var builder = new StringBuilder(64);
            final URI query;
            try {
                query = new URI(entry);
            } catch (URISyntaxException e) {
                builder.append('[').append(entry).append(": null]");
                return builder.toString();
            }

            final var queryPath = StringUtils.stripStart(query.getPath(), "/");
            final boolean providedScheme = query.getScheme() != null && !query.getScheme().isBlank();
            final String stripStartFromSearchPath = queryPath.startsWith("mmi.") ? ('/' + brokerName) : "/"; // crop initial broker name for broker-specific MMI services
            Predicate<URI> matcher = dnsEntry -> {
                if (providedScheme && !dnsEntry.getScheme().equalsIgnoreCase(query.getScheme())) {
                    // require scheme but entry does not provide any
                    return false;
                }
                // scheme matches - check path compatibility
                return StringUtils.stripStart(dnsEntry.getPath(), stripStartFromSearchPath).startsWith(queryPath);
            };
            for (final var brokerEntry : dnsCache.entrySet()) {
                final MajordomoBroker.DnsServiceItem serviceItem = dnsCache.get(brokerEntry.getKey());
                final String matchedItems = serviceItem.uri.stream().filter(matcher).map(URI::toString).collect(Collectors.joining(", "));
                if (matchedItems.isBlank()) {
                    continue;
                }
                builder.append('[').append(query.toString()).append(": ").append(matchedItems).append(']');
            }
            if (builder.length() == 0) {
                builder.append('[').append(query.toString()).append(": null]");
            }
            return builder.toString();
        }
    }

    @MetaInfo(description = "endpoint returning OpenAPI definitions", unit = "MMI OpenAPI Worker Class Definitions")
    public static class MmiOpenApi extends BasicMdpWorker {
        public MmiOpenApi(final MajordomoBroker broker, final RbacRole<?>... rbacRoles) {
            super(broker.getContext(), broker.brokerName + '/' + INTERNAL_SERVICE_OPENAPI, rbacRoles);
            this.registerHandler(context -> {
                final String serviceName = context.req.data == null ? "" : new String(context.req.data, UTF_8);
                var service = broker.services.get(serviceName);
                if (service == null) {
                    service = broker.services.get(broker.brokerName + '/' + serviceName);
                }
                if (service == null) {
                    throw new IllegalArgumentException("requested invalid service name '" + serviceName + "' msg " + context.req);
                }
                context.rep.data = service.serviceDescription;
            });
        }
    }

    @MetaInfo(description = "definition according to <a href = \"http://rfc.zeromq.org/spec:8\">http://rfc.zeromq.org/spec:8</a>", unit = "MMI Service/Property Definitions")
    public static class MmiService extends BasicMdpWorker {
        public MmiService(final MajordomoBroker broker, final RbacRole<?>... rbacRoles) {
            super(broker.getContext(), broker.brokerName + '/' + INTERNAL_SERVICE_NAMES, rbacRoles);
            this.registerHandler(context -> {
                final String serviceName = (context.req.data == null) ? "" : new String(context.req.data, UTF_8);
                if (serviceName.isBlank()) {
                    if (isHtmlRequest(context.req.topic)) {
                        context.rep.data = broker.services.keySet().stream().sorted().map(s -> wrapInAnchor(s, URI.create("/" + s))).collect(Collectors.joining(",")).getBytes(UTF_8);
                    } else {
                        context.rep.data = broker.services.keySet().stream().sorted().collect(Collectors.joining(",")).getBytes(UTF_8);
                    }
                } else {
                    context.rep.data = ((broker.services.containsKey(serviceName) || broker.services.containsKey(broker.brokerName + '/' + serviceName)) ? "200" : "400").getBytes(UTF_8);
                }
            });
        }
    }
}
