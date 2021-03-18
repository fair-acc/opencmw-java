package io.opencmw.client;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.zeromq.ZMonitor.Event;

import static io.opencmw.OpenCmwConstants.*;
import static io.opencmw.OpenCmwProtocol.*;
import static io.opencmw.OpenCmwProtocol.Command.GET_REQUEST;
import static io.opencmw.OpenCmwProtocol.Command.SET_REQUEST;
import static io.opencmw.OpenCmwProtocol.Command.SUBSCRIBE;
import static io.opencmw.OpenCmwProtocol.Command.UNSUBSCRIBE;
import static io.opencmw.OpenCmwProtocol.MdpSubProtocol.PROT_CLIENT;
import static io.opencmw.utils.AnsiDefs.ANSI_RED;
import static io.opencmw.utils.AnsiDefs.ANSI_RESET;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMonitor;
import org.zeromq.ZMsg;

import io.opencmw.filter.SubscriptionMatcher;
import io.opencmw.serialiser.IoSerialiser;
import io.opencmw.serialiser.spi.BinarySerialiser;
import io.opencmw.utils.NoDuplicatesList;
import io.opencmw.utils.SystemProperties;

/**
 * Client implementation for the OpenCMW protocol.
 *
 * Implements the mdp:// (opencmw tcp get/set/sub), mds:// (opencmw udp multicast subscriptions) and mdr:// (majordomo
 * radio dish for e.g. timing updates) protocols.
 *
 * @author Alexander Krimm
 */
@SuppressWarnings({ "PMD.TooManyFields", "PMD.ExcessiveImports" })
public class OpenCmwDataSource extends DataSource implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenCmwDataSource.class);
    private static final AtomicInteger INSTANCE_COUNT = new AtomicInteger();
    private static final String MDP = "mdp";
    private static final String MDS = "mds";
    private static final String MDR = "mdr";
    private static final List<String> APPLICABLE_SCHEMES = List.of(MDP, MDS, MDR);
    private static final List<DnsResolver> RESOLVERS = Collections.synchronizedList(new NoDuplicatesList<>());
    public static final Factory FACTORY = new Factory() {
        @Override
        public List<String> getApplicableSchemes() {
            return APPLICABLE_SCHEMES;
        }

        @Override
        public Class<? extends IoSerialiser> getMatchingSerialiserType(final @NotNull URI endpoint) {
            return BinarySerialiser.class;
        }

        @Override
        public DataSource newInstance(final ZContext context, final @NotNull URI endpoint, final @NotNull Duration timeout, final @NotNull ExecutorService executorService, final @NotNull String clientId) {
            return new OpenCmwDataSource(context, endpoint, timeout, executorService, clientId);
        }

        @Override
        public List<DnsResolver> getRegisteredDnsResolver() {
            return RESOLVERS;
        }
    };
    private final AtomicReference<Event> connectionState = new AtomicReference<>(Event.CLOSED);
    private final AtomicInteger reconnectAttempt = new AtomicInteger(0);
    private final String sourceName;
    private final Duration timeout;
    private final ExecutorService executorService;
    private final String clientId;
    private final URI endpoint;
    private final ZContext context;
    private final Socket socket;
    private final ZMonitor socketMonitor;
    private final Map<String, URI> subscriptions = new HashMap<>(); // NOPMD: not accessed concurrently
    private final URI serverUri;
    private final BiPredicate<URI, URI> subscriptionMatcher = new SubscriptionMatcher(); // <notify topic, subscribe topic>
    private final long heartbeatInterval;
    private Future<URI> dnsWorkerResult;
    private long nextReconnectAttemptTimeStamp;
    private URI connectedAddress;
    static { // register default data sources
        DataSource.register(OpenCmwDataSource.FACTORY);
    }

    /**
     * @param context zeroMQ context used for internal as well as external communication
     * @param endpoint The endpoint to connect to. Only the server part is used and everything else is discarded.
     * @param timeout time-out for reconnects and DNS queries
     * @param executorService thread-pool used to do parallel DNS queries
     * @param clientId Identification string sent to the OpenCMW server. Should be unique per client and is used by the
     *                 Server to identify clients.
     */
    public OpenCmwDataSource(final @NotNull ZContext context, final @NotNull URI endpoint, final @NotNull Duration timeout, final @NotNull ExecutorService executorService, final String clientId) {
        super(endpoint);
        this.context = context;
        this.endpoint = Objects.requireNonNull(endpoint, "endpoint is null");
        this.timeout = timeout;
        this.executorService = executorService;
        this.clientId = clientId;
        this.sourceName = OpenCmwDataSource.class.getSimpleName() + "(ID: " + INSTANCE_COUNT.getAndIncrement() + ", endpoint: " + endpoint + ", clientId: " + clientId + ")";
        try {
            this.serverUri = new URI(endpoint.getScheme(), endpoint.getAuthority(), "/", null, null);
        } catch (URISyntaxException e) {
            LOGGER.atError().addArgument(endpoint).setCause(e).log("cannot create serverURI from endpoint: {}");
            throw new IllegalArgumentException("Invalid endpoint", e);
        }
        switch (endpoint.getScheme().toLowerCase(Locale.UK)) {
        case MDP:
            this.socket = context.createSocket(SocketType.DEALER);
            break;
        case MDS:
            this.socket = context.createSocket(SocketType.SUB);
            break;
        case MDR:
            throw new UnsupportedOperationException("RADIO-DISH pattern is not yet implemented");
            //this.socket = context.createSocket(SocketType.DISH)
        default:
            throw new UnsupportedOperationException("Unsupported protocol type " + endpoint.getScheme());
        }
        setDefaultSocketParameters(socket);

        socketMonitor = new ZMonitor(context, socket);
        socketMonitor.add(Event.CLOSED, Event.CONNECTED, Event.DISCONNECTED);
        socketMonitor.start();

        final long basicHeartBeat = SystemProperties.getValueIgnoreCase(HEARTBEAT, HEARTBEAT_DEFAULT);
        final long clientTimeOut = SystemProperties.getValueIgnoreCase(CLIENT_TIMEOUT, CLIENT_TIMEOUT_DEFAULT); // [s] N.B. '0' means disabled
        // take the minimum of the (albeit worker) heartbeat, client (if defined) or locally prescribed timeout
        heartbeatInterval = clientTimeOut == 0 ? Math.min(basicHeartBeat, timeout.toMillis()) : Math.min(Math.min(basicHeartBeat, timeout.toMillis()), TimeUnit.SECONDS.toMicros(clientTimeOut));

        nextReconnectAttemptTimeStamp = System.currentTimeMillis() + timeout.toMillis();
        final URI reply = connect();
        if (reply == EMPTY_URI) {
            LOGGER.atWarn().addArgument(endpoint).addArgument(sourceName).log("could not connect URI {} immediately - source {}");
        }
    }

    public final URI connect() {
        if (context.isClosed()) {
            LOGGER.atDebug().addArgument(sourceName).log("ZContext closed for '{}'");
            return EMPTY_URI;
        }
        connectionState.set(Event.CONNECT_RETRIED);
        URI address = endpoint;
        if (address.getAuthority() == null) {
            // need to resolve authority if unknown
            //here: implemented first available DNS resolver, could also be round-robin or rotation if there are several resolver registered
            final Optional<DnsResolver> resolver = getFactory().getRegisteredDnsResolver().stream().findFirst();
            if (resolver.isEmpty()) {
                LOGGER.atWarn().addArgument(endpoint).log("cannot resolve {} without a registered DNS resolver");
                return EMPTY_URI;
            }
            try {
                // resolve address
                address = new URI(address.getScheme(), null, '/' + getDeviceName(address), null, null);
                final Map<URI, List<URI>> candidates = resolver.get().resolveNames(List.of(address));
                if (Objects.requireNonNull(candidates.get(address), "candidates did not contain '" + address + "':" + candidates).isEmpty()) {
                    throw new UnknownHostException("DNS resolver could not resolve " + endpoint + " - unknown service - candidates" + candidates + " - " + address);
                }
                address = candidates.get(address).get(0); // take first matching - may upgrade in the future if there are more than one option
            } catch (URISyntaxException | UnknownHostException e) {
                LOGGER.atWarn().addArgument(address).addArgument(e.getMessage()).log("cannot resolve {} - error message: {}"); // NOPMD the original exception is retained
                return EMPTY_URI;
            }
        }

        switch (endpoint.getScheme().toLowerCase(Locale.UK)) {
        case MDP:
        case MDS:
            address = replaceSchemeKeepOnlyAuthority(address, SCHEME_TCP);
            if (!socket.connect(address.toString())) {
                LOGGER.atError().addArgument(address.toString()).log("could not connect to '{}'");
                connectedAddress = EMPTY_URI;
                return EMPTY_URI;
            }
            connectedAddress = address;
            connectionState.set(Event.CONNECTED);
            return connectedAddress;
        case MDR:
            throw new UnsupportedOperationException("RADIO-DISH pattern is not yet implemented"); // well yes, but not released by the JeroMQ folks
        default:
        }
        throw new UnsupportedOperationException("Unsupported protocol type " + endpoint.getScheme());
    }

    @Override
    public Socket getSocket() {
        return socket;
    }

    public URI reconnect() {
        LOGGER.atDebug().addArgument(this.endpoint).addArgument(sourceName).log("need to reconnect for URI {} - source {} ");
        if (connectedAddress != null) {
            socket.disconnect(connectedAddress.toString());
        }

        final URI result = connect();
        if (result == EMPTY_URI) {
            LOGGER.atDebug().addArgument(endpoint).addArgument(sourceName).log("could not reconnect for URI '{}' - source {} ");
        }
        return result;
    }

    @Override
    public void close() throws IOException {
        socketMonitor.close();
        socket.close();
    }

    @Override
    public ZMsg getMessage() {
        final MdpMessage msg = MdpMessage.receive(socket, false);
        if (msg == null) {
            return null;
        }
        switch (msg.protocol) {
        case PROT_CLIENT:
            return handleRequest(msg);
        case PROT_CLIENT_HTTP:
        case PROT_WORKER:
        case UNKNOWN:
        default:
            LOGGER.atDebug().addArgument(msg).log("Ignoring unexpected message: {}");
            return new ZMsg(); // ignore unknown request
        }
    }

    @Override
    public long housekeeping() {
        final long now = System.currentTimeMillis();
        ZMonitor.ZEvent event;
        while ((event = socketMonitor.nextEvent(false)) != null) {
            switch (event.type) {
            case DISCONNECTED:
            case CLOSED:
                connectionState.set(Event.CLOSED);
                break;
            case CONNECTED:
                connectionState.set(Event.CONNECTED);
                break;
            default:
                LOGGER.atDebug().addArgument(event).log("received unknown event {}");
                break;
            }
        }

        switch (connectionState.get()) {
        case CONNECTED:
            reconnectAttempt.set(0);
            return now + heartbeatInterval;
        case CONNECT_RETRIED:
            // reconnection in process but not yet finished
            if (now < nextReconnectAttemptTimeStamp) {
                // not yet time to give the reconnect another try
                return nextReconnectAttemptTimeStamp;
            }
            if (dnsWorkerResult != null) {
                dnsWorkerResult.cancel(true);
            }
            dnsWorkerResult = executorService.submit(this::reconnect); // <--- actual reconnect
            if (reconnectAttempt.getAndIncrement() < SystemProperties.getValueIgnoreCase(RECONNECT_THRESHOLD1, DEFAULT_RECONNECT_THRESHOLD1)) {
                nextReconnectAttemptTimeStamp = now + timeout.toMillis();
            } else if (reconnectAttempt.getAndIncrement() < SystemProperties.getValueIgnoreCase(RECONNECT_THRESHOLD2, DEFAULT_RECONNECT_THRESHOLD2)) {
                nextReconnectAttemptTimeStamp = now + 10 * timeout.toMillis();
            } else {
                nextReconnectAttemptTimeStamp = now + 100 * timeout.toMillis();
            }
            return nextReconnectAttemptTimeStamp;
        case CLOSED:
            // need to (re-)start connection immediately
            connectionState.compareAndSet(Event.CLOSED, Event.CONNECT_RETRIED);
            dnsWorkerResult = executorService.submit(this::reconnect);
            nextReconnectAttemptTimeStamp = now + timeout.toMillis();
            return nextReconnectAttemptTimeStamp;
        default:
            // other cases
        }

        return now + heartbeatInterval;
    }

    @Override
    public void subscribe(final String reqId, final URI endpoint, final byte[] rbacToken) {
        subscriptions.put(reqId, endpoint);
        final byte[] serviceId = endpoint.getPath().substring(1).getBytes(UTF_8);
        if (socket.getSocketType() == SocketType.DEALER) { // mpd
            // only tcp fallback?
            final MdpMessage msg = new MdpMessage(null, PROT_CLIENT, SUBSCRIBE, serviceId, reqId.getBytes(UTF_8), endpoint, EMPTY_FRAME, "", rbacToken);
            if (!msg.send(socket)) {
                LOGGER.atError().addArgument(reqId).addArgument(endpoint).log("subscription error (reqId: {}) for endpoint: {}");
            }
        } else { // mds
            final String id = endpoint.getPath().substring(1) + '?' + endpoint.getQuery();
            socket.subscribe(id.getBytes(UTF_8));
        }
    }

    @Override
    public void unsubscribe(final String reqId) {
        final URI subscriptionEndpoint = subscriptions.remove(reqId);
        final byte[] serviceId = subscriptionEndpoint.getPath().substring(1).getBytes(UTF_8);
        if (socket.getSocketType() == SocketType.DEALER) { // mdp
            final MdpMessage msg = new MdpMessage(clientId.getBytes(UTF_8), PROT_CLIENT, UNSUBSCRIBE, serviceId, reqId.getBytes(UTF_8), endpoint, EMPTY_FRAME, "", null);
            if (!msg.send(socket)) {
                LOGGER.atError().addArgument(reqId).addArgument(endpoint).log("unsubscribe error (reqId: {}) for endpoint: {}");
            }
        } else { // mds
            socket.unsubscribe(serviceId);
        }
    }

    @Override
    public void get(final String requestId, final URI endpoint, final byte[] data, final byte[] rbacToken) {
        // todo: filters which are not in endpoint
        final byte[] serviceId = endpoint.getPath().substring(1).getBytes(UTF_8);
        final MdpMessage msg = new MdpMessage(null, PROT_CLIENT, GET_REQUEST, serviceId, requestId.getBytes(UTF_8), endpoint, EMPTY_FRAME, "", rbacToken);

        if (!msg.send(socket)) {
            LOGGER.atError().addArgument(requestId).addArgument(endpoint).log("get error (reqId: {}) for endpoint: {}");
        }
    }

    @Override
    public void set(final String requestId, final URI endpoint, final byte[] data, final byte[] rbacToken) {
        // todo: filters which are not in endpoint
        final byte[] serviceId = endpoint.getPath().substring(1).getBytes(UTF_8);
        final MdpMessage msg = new MdpMessage(null, PROT_CLIENT, SET_REQUEST, serviceId, requestId.getBytes(UTF_8), endpoint, data, "", rbacToken);
        if (!msg.send(socket)) {
            LOGGER.atError().addArgument(requestId).addArgument(endpoint).log("set error (reqId: {}) for endpoint: {}");
        }
    }

    @Override
    public String toString() {
        return sourceName;
    }

    @Override
    protected Factory getFactory() {
        return FACTORY;
    }

    private ZMsg handleRequest(final MdpMessage msg) {
        switch (msg.command) {
        case PARTIAL:
        case FINAL:
        case W_NOTIFY:
            if (msg.clientRequestID != null && msg.clientRequestID.length > 0) { // for get/set the request id is provided by the server
                return createInternalMsg(msg.clientRequestID, msg.topic, new ZFrame(msg.data), msg.errors, OpenCmwDataSource.class);
            }
            // for subscriptions the request id is missing and has to be recovered from the endpoint url
            final Optional<String> reqId = subscriptions.entrySet().stream() //
                                                   .filter(e -> subscriptionMatcher.test(serverUri.relativize(msg.topic), serverUri.relativize(e.getValue()))) //
                                                   .map(Map.Entry::getKey)
                                                   .findFirst();
            if (reqId.isPresent()) {
                return createInternalMsg(reqId.get().getBytes(), msg.topic, new ZFrame(msg.data), msg.errors, OpenCmwDataSource.class);
            }
            LOGGER.atWarn().addArgument(msg.topic).log("Could not find subscription for notified request with endpoint: {}");
            return new ZMsg(); // ignore unknown notification
        case W_HEARTBEAT:
        case READY:
        case DISCONNECT:
        case UNSUBSCRIBE:
        case SUBSCRIBE:
        case GET_REQUEST:
        case SET_REQUEST:
        case UNKNOWN:
        default:
            LOGGER.atDebug().addArgument(msg).log("Ignoring unexpected message: {}");
            return new ZMsg(); // ignore unknown request
        }
    }

    public static ZMsg createInternalMsg(final byte[] reqId, final URI endpoint, final ZFrame body, final String exception, final Class<? extends DataSource> dataSource) {
        final ZMsg result = new ZMsg();
        result.add(reqId);
        result.add(endpoint.toString());
        result.add(body == null ? EMPTY_ZFRAME : body);
        if (exception == null || exception.isBlank()) {
            result.add(EMPTY_ZFRAME);
        } else {
            result.add(new ZFrame(ANSI_RED + dataSource.getSimpleName() + " received exception for device " + endpoint + ":\n" + exception + ANSI_RESET));
        }
        return result;
    }
}
