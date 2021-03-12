package io.opencmw.client.cmwlight;

import static io.opencmw.OpenCmwConstants.*;
import static io.opencmw.client.OpenCmwDataSource.createInternalMsg;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.zeromq.ZMonitor.Event;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.*;

import io.opencmw.OpenCmwProtocol;
import io.opencmw.QueryParameterParser;
import io.opencmw.client.DataSource;
import io.opencmw.client.DnsResolver;
import io.opencmw.serialiser.IoSerialiser;
import io.opencmw.serialiser.spi.CmwLightSerialiser;
import io.opencmw.utils.NoDuplicatesList;
import io.opencmw.utils.SystemProperties;

/**
 * A lightweight implementation of the CMW RDA3 client part.
 * Reads all sockets from a single Thread, which can also be embedded into other event loops.
 * Manages connection state and automatically reconnects broken connections and subscriptions.
 *
 * @author Alexander Krimm
 */
@SuppressWarnings({ "PMD.UseConcurrentHashMap", "PMD.ExcessiveImports" }) // - only accessed from main thread
public class CmwLightDataSource extends DataSource { // NOPMD - class should probably be smaller
    private static final String RDA_3_PROTOCOL = "rda3";
    private static final List<String> APPLICABLE_SCHEMES = List.of(RDA_3_PROTOCOL);
    private static final List<DnsResolver> RESOLVERS = Collections.synchronizedList(new NoDuplicatesList<>());
    public static final Factory FACTORY = new Factory() {
        @Override
        public List<String> getApplicableSchemes() {
            return APPLICABLE_SCHEMES;
        }

        @Override
        public Class<? extends IoSerialiser> getMatchingSerialiserType(final @NotNull URI endpoint) {
            return CmwLightSerialiser.class;
        }

        @Override
        public List<DnsResolver> getRegisteredDnsResolver() {
            return RESOLVERS;
        }

        @Override
        public DataSource newInstance(final ZContext context, final @NotNull URI endpoint, final @NotNull Duration timeout, final @NotNull ExecutorService executorService, final @NotNull String clientId) {
            return new CmwLightDataSource(context, endpoint, executorService, clientId);
        }
    };
    private static final Logger LOGGER = LoggerFactory.getLogger(CmwLightDataSource.class);
    private static final AtomicLong CONNECTION_ID_GENERATOR = new AtomicLong(0); // global counter incremented for each connection
    private static final AtomicInteger REQUEST_ID_GENERATOR = new AtomicInteger(0);
    protected final AtomicInteger channelId = new AtomicInteger(0); // connection local counter incremented for each channel
    protected final ZContext context;
    protected final ZMQ.Socket socket;
    protected final AtomicReference<Event> connectionState = new AtomicReference<>(Event.CLOSED);
    protected final String sessionId;
    protected final long heartbeatInterval = SystemProperties.getValueIgnoreCase(HEARTBEAT, HEARTBEAT_DEFAULT); // [ms] time between to heartbeats in ms
    protected final int heartbeatAllowedMisses = SystemProperties.getValueIgnoreCase(HEARTBEAT_LIVENESS, HEARTBEAT_LIVENESS_DEFAULT); // [counts] 3-5 is reasonable number of heartbeats which can be missed before resetting the connection
    protected final long subscriptionTimeout = SystemProperties.getValueIgnoreCase(SUBSCRIPTION_TIMEOUT, SUBSCRIPTION_TIMEOUT_DEFAULT); // maximum time after which a connection should be reconnected
    protected final Map<Long, Subscription> subscriptions = new HashMap<>(); // all subscriptions added to the server
    protected final Map<String, Subscription> subscriptionsByReqId = new HashMap<>(); // all subscriptions added to the server
    protected final Map<Long, Subscription> replyIdMap = new HashMap<>(); // all acknowledged subscriptions by their reply id
    protected final URI endpoint;
    private final AtomicInteger reconnectAttempt = new AtomicInteger(0);
    private final ZMonitor socketMonitor;
    private final Queue<Request> queuedRequests = new LinkedBlockingQueue<>();
    private final Map<Long, Request> pendingRequests = new HashMap<>();
    private final ExecutorService executorService;
    protected long connectionId;
    protected long lastHeartbeatReceived = -1;
    protected long lastHeartbeatSent = -1;
    protected int backOff = 20;
    private URI connectedAddress = OpenCmwProtocol.EMPTY_URI;
    static { // register default data sources
        DataSource.register(CmwLightDataSource.FACTORY);
    }

    public CmwLightDataSource(final @NotNull ZContext context, final @NotNull URI endpoint, final @NotNull ExecutorService executorService, final String clientId) {
        super(endpoint);
        LOGGER.atTrace().addArgument(endpoint).log("connecting to: {}");
        this.context = context;
        this.executorService = executorService;
        this.socket = context.createSocket(SocketType.DEALER);
        setDefaultSocketParameters(socket);
        this.sessionId = getSessionId(clientId);
        this.endpoint = endpoint;

        socketMonitor = new ZMonitor(context, socket);
        socketMonitor.add(Event.CLOSED, Event.CONNECTED, Event.DISCONNECTED);
        socketMonitor.start();

        connect();
    }

    @Override
    public void close() throws IOException {
        socketMonitor.close();
        socket.close();
    }

    public void connect() {
        if (connectionState.getAndSet(Event.CONNECT_RETRIED) != Event.CLOSED) {
            return; // already connected
        }

        URI resolveAddress = endpoint;
        if (resolveAddress.getAuthority() == null || resolveAddress.getPort() == -1) {
            // need to resolve authority if unknown
            // here: implemented first available DNS resolver, could also be round-robin or rotation if there are several resolver registered
            final Optional<DnsResolver> resolver = getFactory().getRegisteredDnsResolver().stream().findFirst();
            if (resolver.isEmpty()) {
                LOGGER.atWarn().addArgument(endpoint).log("cannot resolve {} without a registered DNS resolver");
                backOff = backOff * 2;
                connectionState.set(Event.CLOSED);
                return;
            }
            try {
                // resolve address
                resolveAddress = new URI(resolveAddress.getScheme(), null, '/' + getDeviceName(endpoint), null, null);
                final Map<URI, List<URI>> candidates = resolver.get().resolveNames(List.of(resolveAddress));
                if (Objects.requireNonNull(candidates.get(resolveAddress), "candidates did not contain '" + resolveAddress + "':" + candidates).isEmpty()) {
                    throw new UnknownHostException("DNS resolver could not resolve " + endpoint + " - unknown service - candidates" + candidates + " - " + resolveAddress);
                }
                resolveAddress = candidates.get(resolveAddress).get(0); // take first matching - may upgrade in the future if there are more than one option
            } catch (URISyntaxException | UnknownHostException e) { // NOPMD - directory client must be refactored anyway
                LOGGER.atError().setCause(e).addArgument(e.getMessage()).log("Error resolving device from nameserver, using address from endpoint. Error was: {}");
                backOff = backOff * 2;
                connectionState.set(Event.CLOSED);
                return;
            }
        }
        lastHeartbeatSent = System.currentTimeMillis();
        if (resolveAddress.getPort() == -1) {
            LOGGER.atError().addArgument(endpoint).log("could not resolve host service address: '{}'");
        }
        try {
            final String identity = getIdentity();
            resolveAddress = replaceSchemeKeepOnlyAuthority(resolveAddress, SCHEME_TCP);
            socket.setIdentity(identity.getBytes()); // hostname/process/id/channel -- seems to be needed by CMW :-|
            LOGGER.atDebug().addArgument(resolveAddress).addArgument(resolveAddress).addArgument(identity).log("connecting to: '{}'->'{}' with identity {}");
            if (!socket.connect(resolveAddress.toString())) {
                LOGGER.atError().addArgument(endpoint).addArgument(resolveAddress.toString()).log("could not connect requested URI '{}' to '{}'");
                connectedAddress = OpenCmwProtocol.EMPTY_URI;
            }
            connectedAddress = resolveAddress;
            CmwLightProtocol.sendMsg(socket, CmwLightMessage.connect(CmwLightProtocol.VERSION));
        } catch (ZMQException | CmwLightProtocol.RdaLightException | NumberFormatException e) {
            LOGGER.atError().setCause(e).addArgument(connectedAddress).addArgument(endpoint).log("failed to connect to '{}' source host address: '{}'");
            backOff = backOff * 2;
            connectionState.set(Event.CLOSED);
        }
    }

    @Override
    public void get(final String requestId, final URI endpoint, final byte[] data, final byte[] rbacToken) {
        final Request request = new Request(CmwLightProtocol.RequestType.GET, requestId, endpoint, data, rbacToken);
        queuedRequests.add(request);
    }

    public Event getConnectionState() {
        return connectionState.get();
    }

    public ZContext getContext() {
        return context;
    }

    @Override
    public ZMsg getMessage() { // return maintenance objects instead of replies
        CmwLightMessage reply = receiveData();
        if (reply == null) {
            return null;
        }
        final long currentTime = System.currentTimeMillis(); // NOPMD
        switch (reply.messageType) {
        case SERVER_CONNECT_ACK:
            if (connectionState.compareAndSet(Event.CONNECT_RETRIED, Event.CONNECTED)) {
                LOGGER.atTrace().addArgument(connectedAddress).log("Connected to server: {}");
                lastHeartbeatReceived = currentTime;
                backOff = 20; // reset back-off time
            } else {
                LOGGER.atWarn().addArgument(reply).log("ignoring unsolicited connection acknowledgement: {}");
            }
            return new ZMsg();
        case SERVER_HB:
            if (connectionState.get() != Event.CONNECTED) {
                LOGGER.atWarn().addArgument(reply).log("ignoring heartbeat received before connection established: {}");
                return new ZMsg();
            }
            lastHeartbeatReceived = currentTime;
            return new ZMsg();
        case SERVER_REP:
            if (connectionState.get() != Event.CONNECTED) {
                LOGGER.atWarn().addArgument(reply).log("ignoring data received before connection established: {}");
                return new ZMsg();
            }
            lastHeartbeatReceived = currentTime;
            return handleServerReply(reply, currentTime);
        case CLIENT_CONNECT:
        case CLIENT_REQ:
        case CLIENT_HB:
        default:
            LOGGER.atWarn().addArgument(reply).log("ignoring client message from server: {}");
            return new ZMsg();
        }
    }

    @Override
    public ZMQ.Socket getSocket() {
        return socket;
    }

    @Override
    public long housekeeping() {
        final long currentTime = System.currentTimeMillis();
        ZMonitor.ZEvent event;
        while ((event = socketMonitor.nextEvent(false)) != null) {
            switch (event.type) {
            case DISCONNECTED:
            case CLOSED:
                // low-level detection that the connection has been separated
                connectionState.set(Event.CLOSED);
                break;
            case CONNECTED:
                // connectionState.set(Event.CONNECTED) - may not be needed
                break;
            default:
                LOGGER.atDebug().addArgument(event).log("unknown socket event: {}");
                break;
            }
        }

        switch (connectionState.get()) {
        case DISCONNECTED:
        case CLOSED: // reconnect after adequate back off - reset authority since we may likely need to resolve the server:port via the DNS
            if (currentTime > lastHeartbeatSent + backOff) {
                executorService.submit(this::reconnect);
            }
            return lastHeartbeatSent + backOff;
        case CONNECT_RETRIED:
            if (currentTime > lastHeartbeatSent + heartbeatInterval * heartbeatAllowedMisses) { // connect timed out -> increase back of and retry
                backOff = backOff * 2;
                lastHeartbeatSent = currentTime;
                LOGGER.atTrace().addArgument(connectedAddress).addArgument(backOff).log("Connection timed out for {}, retrying in {} ms");
                disconnect();
            }
            return lastHeartbeatSent + heartbeatInterval * heartbeatAllowedMisses;
        case CONNECTED:
            reconnectAttempt.set(0);
            Request request;
            while ((request = queuedRequests.poll()) != null) {
                pendingRequests.put(request.id, request);
                if (!sendRequest(request)) {
                    LOGGER.atWarn().addArgument(endpoint).log("could not send request for host {}");
                }
            }
            if (currentTime > lastHeartbeatSent + heartbeatInterval) { // check for heartbeat interval
                // send Heartbeats
                sendHeartBeat();
                lastHeartbeatSent = currentTime;
                // check if heartbeat was received
                if (lastHeartbeatReceived + heartbeatInterval * heartbeatAllowedMisses < currentTime) {
                    LOGGER.atDebug().addArgument(backOff).log("Connection timed out, reconnecting in {} ms");
                    disconnect();
                    return heartbeatInterval;
                }
                // check timeouts of connection/subscription requests
                for (Subscription sub : subscriptions.values()) {
                    updateSubscription(currentTime, sub);
                }
            }
            return lastHeartbeatSent + heartbeatInterval;
        default:
            throw new IllegalStateException("unexpected connection state: " + connectionState.get());
        }
    }

    public void reconnect() {
        LOGGER.atDebug().addArgument(endpoint).log("need to reconnect for URI {}");
        disconnect();
        connect();
    }

    public void registerDnsResolver(final @NotNull DnsResolver resolver) {
        // delegate method primarily for testing
        getFactory().registerDnsResolver(resolver);
    }

    public void sendHeartBeat() {
        try {
            CmwLightProtocol.sendMsg(socket, CmwLightMessage.CLIENT_HB);
        } catch (CmwLightProtocol.RdaLightException e) {
            throw new IllegalStateException("error sending heartbeat", e);
        }
    }

    @Override
    public void set(final String requestId, final URI endpoint, final byte[] data, final byte[] rbacToken) {
        final Request request = new Request(CmwLightProtocol.RequestType.SET, requestId, endpoint, data, rbacToken);
        queuedRequests.add(request);
    }

    @Override
    public void subscribe(final String reqId, final URI endpoint, final byte[] rbacToken) {
        try {
            final ParsedEndpoint ep = new ParsedEndpoint(endpoint);
            final Subscription sub = new Subscription(endpoint, ep.device, ep.property, ep.ctx, ep.filters);
            sub.idString = reqId;
            subscriptions.put(sub.id, sub);
            subscriptionsByReqId.put(reqId, sub);
        } catch (CmwLightProtocol.RdaLightException e) {
            throw new IllegalArgumentException("invalid endpoint: '" + endpoint + "'", e);
        }
    }

    @Override
    public void unsubscribe(final String reqId) {
        subscriptionsByReqId.get(reqId).subscriptionState = SubscriptionState.CANCELED;
    }

    @Override
    protected Factory getFactory() {
        return FACTORY;
    }

    private void disconnect() {
        LOGGER.atDebug().addArgument(connectedAddress).log("disconnecting {}");
        connectionState.set(Event.CLOSED);
        if (connectedAddress != OpenCmwProtocol.EMPTY_URI) {
            try {
                socket.disconnect(connectedAddress.toString());
            } catch (ZMQException e) {
                LOGGER.atError().setCause(e).log("Failed to disconnect socket");
            }
        }
        // disconnect/reset subscriptions
        for (Subscription sub : subscriptions.values()) {
            sub.subscriptionState = SubscriptionState.UNSUBSCRIBED;
        }
    }

    private String getIdentity() {
        String hostname;
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            hostname = "localhost";
        }
        final long processId = ProcessHandle.current().pid();
        connectionId = CONNECTION_ID_GENERATOR.incrementAndGet();
        final int chId = this.channelId.incrementAndGet();
        return hostname + '/' + processId + '/' + connectionId + '/' + chId; // N.B. this scheme is parsed/enforced by CMW
    }

    private String getSessionId(final String clientId) {
        return "CmwLightClient{pid=" + ProcessHandle.current().pid() + ", conn=" + connectionId + ", clientId=" + clientId + '}';
    }

    private ZMsg handleServerReply(final CmwLightMessage reply, final long currentTime) { //NOPMD
        switch (reply.requestType) {
        case REPLY:
            Request requestForReply = pendingRequests.remove(reply.id);
            try {
                return createInternalMsg(requestForReply.requestId.getBytes(UTF_8), new ParsedEndpoint(requestForReply.endpoint, reply.dataContext.cycleName).toURI(), reply.bodyData, null);
            } catch (URISyntaxException | CmwLightProtocol.RdaLightException e) {
                LOGGER.atWarn().addArgument(requestForReply.endpoint).addArgument(reply.dataContext.cycleName).log("Adding reply context to URI results in illegal url {} + {}");
                return new ZMsg();
            }
        case EXCEPTION:
            final Request requestForException = pendingRequests.remove(reply.id);
            return createInternalMsg(requestForException.requestId.getBytes(UTF_8), requestForException.endpoint, null, reply.exceptionMessage.message);
        case SUBSCRIBE:
            final long id = reply.id;
            final Subscription sub = subscriptions.get(id);
            sub.updateId = (long) reply.options.get(CmwLightProtocol.FieldName.SOURCE_ID_TAG.value());
            replyIdMap.put(sub.updateId, sub);
            sub.subscriptionState = SubscriptionState.SUBSCRIBED;
            LOGGER.atDebug().addArgument(sub.device).addArgument(sub.property).log("subscription successful: {}/{}");
            sub.backOff = 20;
            return new ZMsg();
        case UNSUBSCRIBE:
            // successfully removed subscription
            final Subscription subscriptionForUnsub = replyIdMap.remove(reply.id);
            subscriptionsByReqId.remove(subscriptionForUnsub.idString);
            subscriptions.remove(subscriptionForUnsub.id);
            return new ZMsg();
        case NOTIFICATION_DATA:
            final Subscription subscriptionForNotification = replyIdMap.get(reply.id);
            if (subscriptionForNotification == null) {
                LOGGER.atInfo().addArgument(reply.id).addArgument(reply.toString()).log("received unsolicited subscription data for id '{}': {}");
                return new ZMsg();
            }
            final URI endpointForNotificationContext;
            try {
                endpointForNotificationContext = new ParsedEndpoint(subscriptionForNotification.endpoint, reply.dataContext.cycleName).toURI();
            } catch (URISyntaxException | CmwLightProtocol.RdaLightException e) {
                LOGGER.atWarn().setCause(e).log("Error generating reply context URI");
                return new ZMsg();
            }
            return createInternalMsg(subscriptionForNotification.idString.getBytes(UTF_8), endpointForNotificationContext, reply.bodyData, null);
        case NOTIFICATION_EXC:
            final Subscription subscriptionForNotifyExc = replyIdMap.get(reply.id);
            if (subscriptionForNotifyExc == null) {
                LOGGER.atInfo().addArgument(reply.toString()).log("received unsolicited subscription notification error: {}");
                return new ZMsg();
            }
            return createInternalMsg(subscriptionForNotifyExc.idString.getBytes(UTF_8), subscriptionForNotifyExc.endpoint, null, reply.exceptionMessage.message);
        case SUBSCRIBE_EXCEPTION:
            final Subscription subForSubExc = subscriptions.get(reply.id);
            subForSubExc.subscriptionState = SubscriptionState.UNSUBSCRIBED;
            subForSubExc.timeoutValue = currentTime + subForSubExc.backOff;
            subForSubExc.backOff *= 2;
            LOGGER.atDebug().addArgument(subForSubExc.device).addArgument(subForSubExc.property).log("exception during subscription, retrying: {}/{}");
            return createInternalMsg(subForSubExc.idString.getBytes(UTF_8), subForSubExc.endpoint, null, reply.exceptionMessage.message);
        // unsupported or non-actionable replies
        case GET:
        case SET:
        case CONNECT:
        case EVENT:
        case SESSION_CONFIRM:
        default:
            return new ZMsg();
        }
    }

    private CmwLightMessage receiveData() {
        // receive data
        try {
            final ZMsg data = ZMsg.recvMsg(socket, ZMQ.DONTWAIT);
            if (data == null) {
                return null;
            }
            return CmwLightProtocol.parseMsg(data);
        } catch (CmwLightProtocol.RdaLightException e) {
            LOGGER.atDebug().setCause(e).log("error parsing cmw light reply: ");
            return null;
        }
    }

    private boolean sendRequest(final Request request) {
        try {
            final ParsedEndpoint parsedEndpoint = new ParsedEndpoint(request.endpoint);
            switch (request.requestType) {
            case GET:
                return CmwLightProtocol.sendMsg(socket, CmwLightMessage.getRequest(
                                                                sessionId, request.id, parsedEndpoint.device, parsedEndpoint.property,
                                                                new CmwLightMessage.RequestContext(parsedEndpoint.ctx, parsedEndpoint.filters, null)));
            case SET:
                Objects.requireNonNull(request.data, "Data for set cannot be null");
                return CmwLightProtocol.sendMsg(socket, CmwLightMessage.setRequest(
                                                                sessionId, request.id, parsedEndpoint.device, parsedEndpoint.property,
                                                                new ZFrame(request.data),
                                                                new CmwLightMessage.RequestContext(parsedEndpoint.ctx, parsedEndpoint.filters, null)));
            default:
                throw new CmwLightProtocol.RdaLightException("Message of unknown type");
            }
        } catch (CmwLightProtocol.RdaLightException e) {
            LOGGER.atDebug().setCause(e).log("Error sending get request:");
            return false;
        }
    }

    private void sendSubscribe(final Subscription sub) {
        if (!sub.subscriptionState.equals(SubscriptionState.UNSUBSCRIBED)) {
            return; // already subscribed/subscription in progress
        }
        try {
            CmwLightProtocol.sendMsg(socket, CmwLightMessage.subscribeRequest(
                                                     sessionId, sub.id, sub.device, sub.property,
                                                     Map.of(CmwLightProtocol.FieldName.SESSION_BODY_TAG.value(), Collections.<String, Object>emptyMap()),
                                                     new CmwLightMessage.RequestContext(sub.selector, sub.filters, null),
                                                     CmwLightProtocol.UpdateType.IMMEDIATE_UPDATE));
            sub.subscriptionState = SubscriptionState.SUBSCRIBING;
            sub.timeoutValue = System.currentTimeMillis() + subscriptionTimeout;
        } catch (CmwLightProtocol.RdaLightException e) {
            LOGGER.atDebug().setCause(e).log("Error subscribing to property:");
            sub.timeoutValue = System.currentTimeMillis() + sub.backOff;
            sub.backOff *= 2;
        }
    }

    private void sendUnsubscribe(final Subscription sub) {
        try {
            CmwLightProtocol.sendMsg(socket, CmwLightMessage.unsubscribeRequest(
                                                     sessionId, sub.updateId, sub.device, sub.property,
                                                     Map.of(CmwLightProtocol.FieldName.SESSION_BODY_TAG.value(), Collections.<String, Object>emptyMap()),
                                                     CmwLightProtocol.UpdateType.IMMEDIATE_UPDATE));
            sub.subscriptionState = SubscriptionState.UNSUBSCRIBE_SENT;
        } catch (CmwLightProtocol.RdaLightException e) {
            throw new IllegalStateException("failed to unsubscribe from: '" + sub.property + "'", e);
        }
    }

    private void updateSubscription(final long currentTime, final Subscription sub) {
        switch (sub.subscriptionState) {
        case SUBSCRIBING:
            // check timeout
            if (currentTime > sub.timeoutValue) {
                sub.subscriptionState = SubscriptionState.UNSUBSCRIBED;
                sub.timeoutValue = currentTime + sub.backOff;
                sub.backOff = sub.backOff * 2; // exponential back of
                LOGGER.atDebug().addArgument(sub.device).addArgument(sub.property).log("subscription timed out, retrying: {}/{}");
            }
            break;
        case UNSUBSCRIBED:
            if (currentTime > sub.timeoutValue) {
                LOGGER.atDebug().addArgument(sub.device).addArgument(sub.property).log("subscribing {}/{}");
                sendSubscribe(sub);
            }
            break;
        case SUBSCRIBED:
        case UNSUBSCRIBE_SENT:
            // do nothing
            break;
        case CANCELED:
            sendUnsubscribe(sub);
            break;
        default:
            throw new IllegalStateException("unexpected subscription state: " + sub.subscriptionState);
        }
    }

    public enum SubscriptionState {
        UNSUBSCRIBED,
        SUBSCRIBING,
        SUBSCRIBED,
        CANCELED,
        UNSUBSCRIBE_SENT
    }

    public static class Subscription {
        public final String property;
        public final String device;
        public final String selector;
        public final Map<String, Object> filters;
        public final URI endpoint;
        private final long id = REQUEST_ID_GENERATOR.incrementAndGet();
        public SubscriptionState subscriptionState = SubscriptionState.UNSUBSCRIBED;
        public int backOff = 20;
        public long updateId = -1;
        public long timeoutValue = -1;
        public String idString = "";

        public Subscription(final URI endpoint, final String device, final String property, final String selector, final Map<String, Object> filters) {
            this.endpoint = endpoint;
            this.property = property;
            this.device = device;
            this.selector = selector;
            this.filters = filters;
        }

        @Override
        public String toString() {
            return "Subscription{property='" + property + '\'' + ", device='" + device + '\'' + ", selector='" + selector + '\'' + ", filters=" + filters + ", subscriptionState=" + subscriptionState + ", backOff=" + backOff + ", id=" + id + ", updateId=" + updateId + ", timeoutValue=" + timeoutValue + '}';
        }
    }

    public static class Request { // NOPMD - data class
        public final byte[] data;
        public final long id;
        public final CmwLightProtocol.RequestType requestType;
        private final String requestId;
        private final URI endpoint;
        private final byte[] rbacToken;

        public Request(final CmwLightProtocol.RequestType requestType,
                final String requestId,
                final URI endpoint,
                final byte[] data, // NOPMD - zero copy contract
                final byte[] rbacToken) { // NOPMD - zero copy contract
            this.requestType = requestType;
            this.id = REQUEST_ID_GENERATOR.incrementAndGet();
            this.requestId = requestId;
            this.endpoint = endpoint;
            this.data = data;
            this.rbacToken = rbacToken;
        }
    }

    public static class ParsedEndpoint {
        public static final String DEFAULT_SELECTOR = "";
        public static final String FILTER_TYPE_LONG = "long:";
        public static final String FILTER_TYPE_INT = "int:";
        public static final String FILTER_TYPE_BOOL = "bool:";
        public static final String FILTER_TYPE_DOUBLE = "double:";
        public static final String FILTER_TYPE_FLOAT = "float:";

        public final Map<String, Object> filters;
        public final String ctx;
        public final String device;
        public final String property;
        public final String authority;

        public ParsedEndpoint(final URI endpoint) throws CmwLightProtocol.RdaLightException {
            this(endpoint, null);
        }

        public ParsedEndpoint(final URI endpoint, final String ctx) throws CmwLightProtocol.RdaLightException {
            authority = Objects.requireNonNullElse(endpoint.getAuthority(), "").contains(":") ? endpoint.getAuthority() : null; // NOPMD - only accept authorities with a port definition
            device = getDeviceName(endpoint);
            property = getPropertyName(endpoint);
            if (property == null || property.isBlank() || property.contains("/")) {
                throw new CmwLightProtocol.RdaLightException("URI not compatible with rda3://<host>:<port>/<device>/<property> path scheme: " + endpoint + " detected property: " + property);
            }

            final Map<String, String> parsedQuery = QueryParameterParser.getFlatMap(endpoint.getQuery());
            if (ctx == null) {
                if (parsedQuery.containsKey("ctx")) {
                    this.ctx = parsedQuery.remove("ctx");
                } else {
                    this.ctx = DEFAULT_SELECTOR;
                }
            } else {
                this.ctx = ctx;
                parsedQuery.remove("ctx");
            }
            filters = parsedQuery.entrySet().stream().collect(Collectors.toMap(Map.Entry<String, String>::getKey, e -> {
                String val = e.getValue();
                if (val.startsWith(FILTER_TYPE_INT)) {
                    return Integer.valueOf(val.substring(FILTER_TYPE_INT.length()));
                } else if (val.startsWith(FILTER_TYPE_LONG)) {
                    return Long.valueOf(val.substring(FILTER_TYPE_LONG.length()));
                } else if (val.startsWith(FILTER_TYPE_BOOL)) {
                    return Boolean.valueOf(val.substring(FILTER_TYPE_BOOL.length()));
                } else if (val.startsWith(FILTER_TYPE_DOUBLE)) {
                    return Double.valueOf(val.substring(FILTER_TYPE_DOUBLE.length()));
                } else if (val.startsWith(FILTER_TYPE_FLOAT)) {
                    return Float.valueOf(val.substring(FILTER_TYPE_FLOAT.length()));
                } else {
                    return val;
                }
            }));
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ParsedEndpoint)) {
                return false;
            }
            final ParsedEndpoint that = (ParsedEndpoint) o;
            return filters.equals(that.filters) && ctx.equals(that.ctx) && device.equals(that.device) && property.equals(that.property);
        }

        @Override
        public int hashCode() {
            return Objects.hash(filters, ctx, device, property);
        }

        public URI toURI() throws URISyntaxException {
            final String filterString = filters.entrySet().stream() //
                                                .map(e -> {
                                                    final Object value = e.getValue();
                                                    final String val;
                                                    if (value instanceof String) {
                                                        val = (String) value;
                                                    } else if (value instanceof Integer) {
                                                        val = FILTER_TYPE_INT + value;
                                                    } else if (value instanceof Long) {
                                                        val = FILTER_TYPE_LONG + value;
                                                    } else if (value instanceof Boolean) {
                                                        val = FILTER_TYPE_BOOL + value;
                                                    } else if (value instanceof Double) {
                                                        val = FILTER_TYPE_DOUBLE + value;
                                                    } else if (value instanceof Float) {
                                                        val = FILTER_TYPE_FLOAT + value;
                                                    } else {
                                                        // seems to be only thrown for library design errors
                                                        throw new IllegalArgumentException("data type not supported in endpoint filters");
                                                    }
                                                    return e.getKey() + '=' + val;
                                                }) //
                                                .collect(Collectors.joining("&"));
            return new URI(RDA_3_PROTOCOL, authority, '/' + device + '/' + property, "ctx=" + ctx + '&' + filterString, null);
        }
    }
}
