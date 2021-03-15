package io.opencmw.client;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;

import io.opencmw.OpenCmwConstants;
import io.opencmw.domain.BinaryData;
import io.opencmw.utils.SystemProperties;

public class OpenCmwDnsResolver implements DnsResolver, AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenCmwDnsResolver.class);
    private static final List<String> APPLICABLE_SCHEMES = Arrays.asList("mdp", "mds", "mdr", "http", "https");
    private static final Pattern DNS_PATTERN = Pattern.compile("\\[(.*?)]");
    private final ZContext context;
    private final boolean ownsCtx;
    private final URI dnsServer;
    private final Duration timeOut;
    private final DataSourcePublisher dataSource;

    public OpenCmwDnsResolver(final @NotNull URI dnsServer) {
        this(null, dnsServer, Duration.ofSeconds(1));
    }

    public OpenCmwDnsResolver(final ZContext context, final @NotNull URI dnsServer, final @NotNull Duration timeOut) {
        this.dnsServer = Objects.requireNonNull(dnsServer, "dnsServer may not be null");
        ownsCtx = context == null;
        this.context = Objects.requireNonNullElse(context, new ZContext(1));
        this.timeOut = timeOut;
        dataSource = new DataSourcePublisher(context, null, null, null, OpenCmwDnsResolver.class.getName());
        dataSource.start();
        LockSupport.parkNanos(Duration.ofMillis(1000).toNanos()); // wait until DNS client has been initialised
    }

    @Override
    public List<String> getApplicableSchemes() {
        return APPLICABLE_SCHEMES;
    }

    @Override
    public Map<URI, List<URI>> resolveNames(final List<URI> devicesToResolve) throws UnknownHostException {
        final int liveness = SystemProperties.getValueIgnoreCase(OpenCmwConstants.HEARTBEAT_LIVENESS, OpenCmwConstants.HEARTBEAT_LIVENESS_DEFAULT);
        try (DataSourcePublisher.Client client = dataSource.getClient()) {
            final String query = devicesToResolve.stream().map(URI::toString).collect(Collectors.joining(","));
            final URI queryURI = URI.create(dnsServer + "/mmi.dns?" + query);
            final Future<BinaryData> reply4 = client.get(queryURI, null, BinaryData.class);
            for (int attempt = 0; attempt < liveness; attempt++) {
                try {
                    return parseDnsReply(Objects.requireNonNull(reply4.get(timeOut.toMillis(), TimeUnit.MILLISECONDS).data, "reply data is null"));
                } catch (InterruptedException | ExecutionException | TimeoutException | NullPointerException e) { // NOPMD NOSONAR - fail only after three attempts
                    final String exception = e.getClass().getName() + ": " + e.getMessage();
                    LOGGER.atWarn().addArgument(OpenCmwDnsResolver.class.getSimpleName()).addArgument(attempt).addArgument(dnsServer).addArgument(devicesToResolve).addArgument(exception).log("{} - attempt {}: dns server {} could not resolve '{}'  error: {}");
                }
            }
        }
        throw new UnknownHostException("cannot resolve URI - dnsServer: " + dnsServer + " (timeout reached: " + (timeOut.toMillis() * liveness) + " ms) - URI list: " + devicesToResolve);
    }

    @Override
    public void close() {
        dataSource.close();
        if (ownsCtx) {
            context.close();
        }
    }

    public static Map<URI, List<URI>> parseDnsReply(final byte[] dnsReply) {
        if (dnsReply == null || dnsReply.length == 0 || !isUTF8(dnsReply)) {
            return Collections.emptyMap();
        }
        final String reply = new String(dnsReply, UTF_8);
        if (reply.isBlank()) {
            return Collections.emptyMap();
        }

        // parse reply
        final Matcher matchPattern = DNS_PATTERN.matcher(reply);
        final Map<URI, List<URI>> map = new ConcurrentHashMap<>();
        while (matchPattern.find()) {
            final String device = matchPattern.group(1);
            final String[] message = device.split("(: )", 2);
            assert message.length == 2 : "could not split into 2 segments: " + device;
            try {
                final List<URI> uriList = map.computeIfAbsent(new URI(message[0]), deviceName -> new ArrayList<>()); // NOPMD - in loop allocation OK
                Stream.of(StringUtils.split(message[1], ",")).filter(uriString -> !"null".equalsIgnoreCase(uriString)).forEach(uriString -> {
                    try {
                        uriList.add(new URI(StringUtils.strip(uriString)));
                    } catch (final URISyntaxException e) {
                        LOGGER.atError().setCause(e).addArgument(message[0]).addArgument(uriString).log("could not parse device '{}' uri: '{}}'");
                    }
                });

            } catch (final URISyntaxException e) {
                LOGGER.atError().setCause(e).addArgument(message[1]).log("could not parse device line '{}'");
            }
        }

        return map;
    }

    public static boolean isUTF8(byte[] array) {
        final CharsetDecoder decoder = UTF_8.newDecoder();
        final ByteBuffer buf = ByteBuffer.wrap(array);
        try {
            decoder.decode(buf);
        } catch (CharacterCodingException e) {
            return false;
        }
        return true;
    }
}
