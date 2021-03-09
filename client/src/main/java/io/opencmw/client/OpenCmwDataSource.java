package io.opencmw.client;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.zeromq.ZMonitor.Event;

import static io.opencmw.OpenCmwConstants.*;
import static io.opencmw.OpenCmwProtocol.Command.*;
import static io.opencmw.OpenCmwProtocol.EMPTY_FRAME;
import static io.opencmw.OpenCmwProtocol.MdpMessage;
import static io.opencmw.OpenCmwProtocol.MdpSubProtocol.PROT_CLIENT;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiPredicate;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.*;
import org.zeromq.ZMQ.Socket;

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
        public DataSource newInstance(final ZContext context, final @NotNull URI endpoint, final @NotNull Duration timeout, final @NotNull String clientId) {
            return new OpenCmwDataSource(context, endpoint, timeout, clientId);
        }

        @Override
        public List<DnsResolver> getRegisteredDnsResolver() {
            return RESOLVERS;
        }
    };
    private final AtomicReference<Event> connectionState = new AtomicReference<>(Event.CLOSED);
    private final AtomicInteger reconnectAttempt = new AtomicInteger(0);
    private final String sourceName;
    private final String clientId;
    private final Duration timeout;
    private final URI endpoint;
    private final ZContext context;
    private final Socket socket;
    private final ZMonitor socketMonitor;
    private final Map<String, URI> subscriptions = new HashMap<>(); // NOPMD: not accessed concurrently
    private final URI serverUri;
    private final BiPredicate<URI, URI> subscriptionMatcher = new SubscriptionMatcher(); // <notify topic, subscribe topic>
    private URI connectedAddress;

    /**
     * @param context zeroMQ context used for internal as well as external communication
     * @param endpoint The endpoint to connect to. Only the server part is used and everything else is discarded.
     * @param clientId Identification string sent to the OpenCMW server. Should be unique per client and is used by the
     *                 Server to identify clients.
     */
    public OpenCmwDataSource(final @NotNull ZContext context, final @NotNull URI endpoint, final @NotNull Duration timeout, final String clientId) {
        super(endpoint);
        this.context = context;
        this.endpoint = Objects.requireNonNull(endpoint, "endpoint is null");
        this.timeout = timeout;
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
        socket.setHWM(SystemProperties.getValueIgnoreCase(HIGH_WATER_MARK, HIGH_WATER_MARK_DEFAULT));

        socketMonitor = new ZMonitor(context, socket);
        //socketMonitor.add(Event.CONNECTED, Event.DISCONNECTED);
        socketMonitor.add(Event.ALL);
        socketMonitor.start();

        reconnect(); // NOPMD
    }

    public void connect() throws UnknownHostException {
        if (context.isClosed()) {
            LOGGER.atDebug().addArgument(sourceName).log("ZContext closed for '{}'");
            return;
        }
        URI address = endpoint;
        if (address.getAuthority() == null) {
            // need to resolve authority if unknown
            //here: implemented first available DNS resolver, could also be round-robin or rotation if there are several resolver registered
            final DnsResolver resolver = getFactory().getRegisteredDnsResolver().stream().findFirst().orElseThrow(() -> new UnknownHostException("cannot resolve " + endpoint + " without a registered DNS resolver"));
            try {
                // resolve address
                address = new URI(address.getScheme(), null, '/' + getDeviceName(address), null, null);
                final Map<URI, List<URI>> candidates = resolver.resolveNames(List.of(address));
                if (Objects.requireNonNull(candidates.get(address), "candidates did not contain '" + address + "':" + candidates).isEmpty()) {
                    throw new UnknownHostException("DNS resolver could not resolve " + endpoint + " - unknown service - candidates" + candidates + " - " + address);
                }
                address = candidates.get(address).get(0); // take first matching - may upgrade in the future if there are more than one option
            } catch (URISyntaxException e) {
                throw new UnknownHostException("could not resolve " + address + " - malformed URI: " + e.getMessage()); // NOPMD the original exception is retained
            }
        }

        switch (endpoint.getScheme().toLowerCase(Locale.UK)) {
        case MDP:
        case MDS:
            connectedAddress = replaceSchemeKeepOnlyAuthority(address, SCHEME_TCP);
            if (!socket.connect(connectedAddress.toString())) {
                LOGGER.atError().addArgument(address.toString()).log("could not bind to connect '{}'");
                break;
            }
            connectionState.set(Event.CONNECTED);
            break;
        case MDR:
            throw new UnsupportedOperationException("RADIO-DISH pattern is not yet implemented");
        default:
            throw new UnsupportedOperationException("Unsupported protocol type " + endpoint.getScheme());
        }
    }

    @Override
    public Socket getSocket() {
        return socket;
    }

    public void reconnect() {
        LOGGER.atTrace().addArgument(this.endpoint).addArgument(sourceName).log("need to reconnect for URI {} - source {} ");
        if (connectedAddress != null) {
            socket.disconnect(connectedAddress.toString());
        }
        try {
            connect(); // NOPMD
        } catch (UnknownHostException e) {
            LOGGER.atTrace().setCause(e).addArgument(endpoint).log("malformed endpoint URI or unable to connect to '{}'");
        }
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
        ZMonitor.ZEvent event;
        while ((event = socketMonitor.nextEvent(false)) != null) {
            switch (event.type) {
            case DISCONNECTED:
            case CLOSED:
                // closed or disconnected
                connectionState.set(Event.CLOSED);
                break;
            case CONNECTED:
                reconnectAttempt.set(0);
                connectionState.set(Event.CONNECTED);
                break;
            default:
                LOGGER.atDebug().addArgument(event).log("received unknown event {}");
                break;
            }
        }
        if (connectionState.compareAndSet(Event.CLOSED, Event.CONNECT_RETRIED)) {
            // TODO: replace with worker thread pool
            // need to (re-)start connection
            final Thread reconnectionThread = new Thread(() -> { // TODO: replace with worker thread pool
                while (connectionState.get() != Event.CONNECTED && !context.isClosed() && !Thread.currentThread().isInterrupted()) {
                    // need to (re-)start connection
                    if (reconnectAttempt.getAndIncrement() < SystemProperties.getValueIgnoreCase(RECONNECT_THRESHOLD1, DEFAULT_RECONNECT_THRESHOLD1)) {
                        LockSupport.parkNanos(timeout.toNanos());
                    } else if (reconnectAttempt.getAndIncrement() < SystemProperties.getValueIgnoreCase(RECONNECT_THRESHOLD1, DEFAULT_RECONNECT_THRESHOLD1)) {
                        LockSupport.parkNanos(10 * timeout.toNanos());
                    } else {
                        LockSupport.parkNanos(100 * timeout.toNanos());
                    }
                    reconnect();
                }
            }, "reconnecting-thread for: " + endpoint);
            reconnectionThread.start();
        }
        return System.currentTimeMillis() + 1000;
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
                return createInternalMsg(msg.clientRequestID, msg.topic, new ZFrame(msg.data), msg.errors);
            }
            // for subscriptions the request id is missing and has to be recovered from the endpoint url
            final Optional<String> reqId = subscriptions.entrySet().stream() //
                                                   .filter(e -> subscriptionMatcher.test(serverUri.relativize(msg.topic), serverUri.relativize(e.getValue()))) //
                                                   .map(Map.Entry::getKey)
                                                   .findFirst();
            if (reqId.isPresent()) {
                return createInternalMsg(reqId.get().getBytes(), msg.topic, new ZFrame(msg.data), msg.errors);
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

    public static ZMsg createInternalMsg(final byte[] reqId, final URI endpoint, final ZFrame body, final String exception) {
        final ZMsg result = new ZMsg();
        result.add(reqId);
        result.add(endpoint.toString());
        result.add(body == null ? new ZFrame(new byte[0]) : body);
        result.add(exception == null ? new ZFrame(new byte[0]) : new ZFrame(exception));
        return result;
    }
}
