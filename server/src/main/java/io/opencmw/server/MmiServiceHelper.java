package io.opencmw.server;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.net.URI;
import java.util.stream.Collectors;

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
            super(broker.getContext(), INTERNAL_SERVICE_ECHO, rbacRoles);
            this.registerHandler(ctx -> ctx.rep.data = ctx.req.data); //  output = input : echo service is complex :-)
        }
    }

    @MetaInfo(description = "Dynamic Name Service (DNS) returning registered internal and external broker endpoints' URIs (protocol, host ip, port etc.)", unit = "MMI DNS Service")
    public static class MmiDns extends BasicMdpWorker {
        public MmiDns(final MajordomoBroker broker, final RbacRole<?>... rbacRoles) {
            super(broker.getContext(), INTERNAL_SERVICE_DNS, rbacRoles);
            this.registerHandler(ctx -> {
                if (isHtmlRequest(ctx.req.topic)) {
                    ctx.rep.data = broker.dnsCache.values().stream().map(MajordomoBroker.DnsServiceItem::getDnsEntryHtml).collect(Collectors.joining(",<br>")).getBytes(UTF_8);
                } else {
                    ctx.rep.data = broker.dnsCache.values().stream().map(MajordomoBroker.DnsServiceItem::getDnsEntry).collect(Collectors.joining(",")).getBytes(UTF_8);
                }
            });
        }
    }

    @MetaInfo(description = "endpoint returning OpenAPI definitions", unit = "MMI OpenAPI Worker Class Definitions")
    public static class MmiOpenApi extends BasicMdpWorker {
        public MmiOpenApi(final MajordomoBroker broker, final RbacRole<?>... rbacRoles) {
            super(broker.getContext(), INTERNAL_SERVICE_OPENAPI, rbacRoles);
            this.registerHandler(context -> {
                final String serviceName = context.req.data == null ? "" : new String(context.req.data, UTF_8);
                final MajordomoBroker.Service service = broker.services.get(serviceName);
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
            super(broker.getContext(), INTERNAL_SERVICE_NAMES, rbacRoles);
            this.registerHandler(context -> {
                final String serviceName = (context.req.data == null) ? "" : new String(context.req.data, UTF_8);
                if (serviceName.isBlank()) {
                    if (isHtmlRequest(context.req.topic)) {
                        context.rep.data = broker.services.keySet().stream().sorted().map(s -> wrapInAnchor(s, URI.create("/" + s))).collect(Collectors.joining(",")).getBytes(UTF_8);
                    } else {
                        context.rep.data = broker.services.keySet().stream().sorted().collect(Collectors.joining(",")).getBytes(UTF_8);
                    }
                } else {
                    context.rep.data = (broker.services.containsKey(serviceName) ? "200" : "400").getBytes(UTF_8);
                }
            });
        }
    }
}
