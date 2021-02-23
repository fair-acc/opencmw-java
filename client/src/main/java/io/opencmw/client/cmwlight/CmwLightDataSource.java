package io.opencmw.client.cmwlight;

import static io.opencmw.OpenCmwConstants.*;

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;
import org.zeromq.ZMsg;

import io.opencmw.OpenCmwConstants;
import io.opencmw.client.DataSource;
import io.opencmw.client.Endpoint;
import io.opencmw.serialiser.IoSerialiser;
import io.opencmw.serialiser.spi.CmwLightSerialiser;
import io.opencmw.utils.SystemProperties;

/**
 * A lightweight implementation of the CMW RDA3 client part.
 * Reads all sockets from a single Thread, which can also be embedded into other event loops.
 * Manages connection state and automatically reconnects broken connections and subscriptions.
 *
 * @author Alexander Krimm
 */
@SuppressWarnings({ "PMD.UseConcurrentHashMap" }) // - only accessed from main thread
public class CmwLightDataSource extends DataSource { // NOPMD - class should probably be smaller
    private static final Logger LOGGER = LoggerFactory.getLogger(CmwLightDataSource.class);
    private static final AtomicLong CONNECTION_ID_GENERATOR = new AtomicLong(0); // global counter incremented for each connection
    private static final AtomicInteger REQUEST_ID_GENERATOR = new AtomicInteger(0);
    public static final String RDA_3_PROTOCOL = "rda3";
    public static final Factory FACTORY = new Factory() {
        @Override
        public boolean matches(final URI endpoint) {
            return endpoint.getScheme().equals(RDA_3_PROTOCOL);
        }

        @Override
        public Class<? extends IoSerialiser> getMatchingSerialiserType(final URI endpoint) {
            return CmwLightSerialiser.class;
        }

        @Override
        public DataSource newInstance(final ZContext context, final URI endpoint, final Duration timeout, final String clientId) {
            return new CmwLightDataSource(context, endpoint, clientId);
        }
    };
    private static DirectoryLightClient directoryLightClient;
    protected final AtomicInteger channelId = new AtomicInteger(0); // connection local counter incremented for each channel
    protected final ZContext context;
    protected final ZMQ.Socket socket;
    protected final AtomicReference<ConnectionState> connectionState = new AtomicReference<>(ConnectionState.DISCONNECTED);
    protected final String address;
    protected final String sessionId;
    protected final long heartbeatInterval = SystemProperties.getValueIgnoreCase(HEARTBEAT, HEARTBEAT_DEFAULT); // [ms] time between to heartbeats in ms
    protected final int heartbeatAllowedMisses = SystemProperties.getValueIgnoreCase(HEARTBEAT_LIVENESS, HEARTBEAT_LIVENESS_DEFAULT); // [counts] 3-5 is reasonable number of heartbeats which can be missed before resetting the conection
    protected final long subscriptionTimeout = SystemProperties.getValueIgnoreCase(SUBSCRIPTION_TIMEOUT, SUBSCRIPTION_TIMEOUT_DEFAULT); // maximum time after which a connection should be reconnected
    protected final Map<Long, Subscription> subscriptions = new HashMap<>(); // all subscriptions added to the server
    protected final Map<String, Subscription> subscriptionsByReqId = new HashMap<>(); // all subscriptions added to the server
    protected final Map<Long, Subscription> replyIdMap = new HashMap<>(); // all acknowledged subscriptions by their reply id
    protected long connectionId;
    protected long lastHeartbeatReceived = -1;
    protected long lastHeartbeatSent = -1;
    protected int backOff = 20;
    private final Queue<Request> queuedRequests = new LinkedBlockingQueue<>();
    private final Map<Long, Request> pendingRequests = new HashMap<>();
    private String connectedAddress = "";

    public CmwLightDataSource(final ZContext context, final URI endpoint, final String clientId) {
        super(endpoint);
        LOGGER.atTrace().addArgument(endpoint).log("connecting to: {}");
        this.context = context;
        this.socket = context.createSocket(SocketType.DEALER);
        this.sessionId = getSessionId(clientId);
        this.address = endpoint.getAuthority();
    }

    public static DirectoryLightClient getDirectoryLightClient() {
        return directoryLightClient;
    }

    public static void setDirectoryLightClient(final DirectoryLightClient directoryLightClient) {
        CmwLightDataSource.directoryLightClient = directoryLightClient;
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
    @Override
    public ZMsg getMessage() { // return maintenance objects instead of replies
        CmwLightMessage reply = receiveData();
        if (reply == null) {
            return null;
        }
        final long currentTime = System.currentTimeMillis(); // NOPMD
        switch (reply.messageType) {
        case SERVER_CONNECT_ACK:
            if (connectionState.compareAndSet(ConnectionState.CONNECTING, ConnectionState.CONNECTED)) {
                LOGGER.atTrace().addArgument(connectedAddress).log("Connected to server: {}");
                lastHeartbeatReceived = currentTime;
                backOff = 20; // reset back-off time
            } else {
                LOGGER.atWarn().addArgument(reply).log("ignoring unsolicited connection acknowledgement: {}");
            }
            return new ZMsg();
        case SERVER_HB:
            if (connectionState.get() != ConnectionState.CONNECTED) {
                LOGGER.atWarn().addArgument(reply).log("ignoring heartbeat received before connection established: {}");
                return new ZMsg();
            }
            lastHeartbeatReceived = currentTime;
            return new ZMsg();
        case SERVER_REP:
            if (connectionState.get() != ConnectionState.CONNECTED) {
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

    private ZMsg handleServerReply(final CmwLightMessage reply, final long currentTime) { //NOPMD
        switch (reply.requestType) {
        case REPLY:
            Request requestForReply = pendingRequests.remove(reply.id);
            try {
                return createInternalMsg(requestForReply.requestId,
                        new URI(new Endpoint(requestForReply.endpoint.toString()).getEndpointForContext(reply.dataContext.cycleName)), reply.bodyData, null);
            } catch (URISyntaxException e) {
                LOGGER.atWarn().addArgument(requestForReply.endpoint).addArgument(reply.dataContext.cycleName).log("Adding reply context to URI results in illegal url {} + {}");
                return new ZMsg();
            }
        case EXCEPTION:
            final Request requestForException = pendingRequests.remove(reply.id);
            return createInternalMsg(requestForException.requestId, requestForException.endpoint, null, reply.exceptionMessage.message);
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
                LOGGER.atInfo().addArgument(reply.toString()).log("received unsolicited subscription data: {}");
                return new ZMsg();
            }
            final URI endpointForNotificationContext;
            try {
                endpointForNotificationContext = new URI(new Endpoint(subscriptionForNotification.endpoint.toString()).getEndpointForContext(reply.dataContext.cycleName));
            } catch (URISyntaxException e) {
                LOGGER.atWarn().setCause(e).log("Error generating reply context URI");
                return new ZMsg();
            }
            return createInternalMsg(subscriptionForNotification.idString, endpointForNotificationContext, reply.bodyData, null);
        case NOTIFICATION_EXC:
            final Subscription subscriptionForNotifyExc = replyIdMap.get(reply.id);
            if (subscriptionForNotifyExc == null) {
                LOGGER.atInfo().addArgument(reply.toString()).log("received unsolicited subscription notification error: {}");
                return new ZMsg();
            }
            return createInternalMsg(subscriptionForNotifyExc.idString, subscriptionForNotifyExc.endpoint, null, reply.exceptionMessage.message);
        case SUBSCRIBE_EXCEPTION:
            final Subscription subForSubExc = subscriptions.get(reply.id);
            subForSubExc.subscriptionState = SubscriptionState.UNSUBSCRIBED;
            subForSubExc.timeoutValue = currentTime + subForSubExc.backOff;
            subForSubExc.backOff *= 2;
            LOGGER.atDebug().addArgument(subForSubExc.device).addArgument(subForSubExc.property).log("exception during subscription, retrying: {}/{}");
            return createInternalMsg(subForSubExc.idString, subForSubExc.endpoint, null, reply.exceptionMessage.message);
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

    private ZMsg createInternalMsg(final String reqId, final URI endpoint, final ZFrame body, final String exception) {
        final ZMsg result = new ZMsg();
        result.add(reqId);
        result.add(endpoint.toString());
        result.add(body == null ? new ZFrame(new byte[0]) : body);
        result.add(exception == null ? new ZFrame(new byte[0]) : new ZFrame(exception));
        return result;
    }

    public enum ConnectionState {
        DISCONNECTED,
        CONNECTING,
        CONNECTED
    }

    @Override
    public void get(final String requestId, final URI endpoint, final byte[] data, final byte[] rbacToken) {
        final Request request = new Request(CmwLightProtocol.RequestType.GET, requestId, endpoint, data, rbacToken);
        queuedRequests.add(request);
    }

    @Override
    public void set(final String requestId, final URI endpoint, final byte[] data, final byte[] rbacToken) {
        final Request request = new Request(CmwLightProtocol.RequestType.SET, requestId, endpoint, data, rbacToken);
        queuedRequests.add(request);
    }

    @Override
    public void subscribe(final String reqId, final URI endpoint, final byte[] rbacToken) {
        final Endpoint ep = new Endpoint(endpoint.toString());
        final Subscription sub = new Subscription(endpoint, ep.getDevice(), ep.getProperty(), ep.getSelector(), ep.getFilters());
        sub.idString = reqId;
        subscriptions.put(sub.id, sub);
        subscriptionsByReqId.put(reqId, sub);
    }

    @Override
    public void unsubscribe(final String reqId) {
        subscriptionsByReqId.get(reqId).subscriptionState = SubscriptionState.CANCELED;
    }

    public ConnectionState getConnectionState() {
        return connectionState.get();
    }

    public ZContext getContext() {
        return context;
    }

    @Override
    public ZMQ.Socket getSocket() {
        return socket;
    }

    @Override
    protected Factory getFactory() {
        return FACTORY;
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
        return hostname + '/' + processId + '/' + connectionId + '/' + chId;
    }

    private String getSessionId(final String clientId) {
        return "cmwLightClient{pid=" + ProcessHandle.current().pid() + ", conn=" + connectionId + ", clientId=" + clientId + '}'; // Todo: create identification string, cmw uses string with user/app name, pid, etc
    }

    public void connect() {
        if (connectionState.getAndSet(ConnectionState.CONNECTING) != ConnectionState.DISCONNECTED) {
            return; // already connected
        }
        String address = this.address;
        if (!address.contains(":")) {
            try {
                DirectoryLightClient.Device device = directoryLightClient.getDeviceInfo(Collections.singletonList(address)).get(0);
                LOGGER.atTrace().addArgument(address).addArgument(device).log("resolved address for device {}: {}");
                address = device.servers.stream().findFirst().orElseThrow().get("Address:");
            } catch (NullPointerException | NoSuchElementException | DirectoryLightClient.DirectoryClientException e) { // NOPMD - directory client must be refactored anyway
                LOGGER.atDebug().addArgument(e.getMessage()).log("Error resolving device from nameserver, using address from endpoint. Error was: {}");
                backOff = backOff * 2;
                connectionState.set(ConnectionState.DISCONNECTED);
                return;
            }
        }
        lastHeartbeatSent = System.currentTimeMillis();
        try {
            final String identity = getIdentity();
            connectedAddress = "tcp://" + address;
            LOGGER.atDebug().addArgument(connectedAddress).addArgument(identity).log("connecting to: {} with identity {}");
            socket.setIdentity(identity.getBytes()); // hostname/process/id/channel
            socket.connect(connectedAddress);
            CmwLightProtocol.sendMsg(socket, CmwLightMessage.connect(CmwLightProtocol.VERSION));
        } catch (ZMQException | CmwLightProtocol.RdaLightException e) {
            LOGGER.atDebug().setCause(e).log("failed to connect: ");
            backOff = backOff * 2;
            connectionState.set(ConnectionState.DISCONNECTED);
        }
    }

    private void disconnect() {
        LOGGER.atDebug().addArgument(connectedAddress).log("disconnecting {}");
        connectionState.set(ConnectionState.DISCONNECTED);
        try {
            socket.disconnect(connectedAddress);
        } catch (ZMQException e) {
            LOGGER.atError().setCause(e).log("Failed to disconnect socket");
        }
        // disconnect/reset subscriptions
        for (Subscription sub : subscriptions.values()) {
            sub.subscriptionState = SubscriptionState.UNSUBSCRIBED;
        }
    }

    @Override
    public long housekeeping() {
        final long currentTime = System.currentTimeMillis();
        switch (connectionState.get()) {
        case DISCONNECTED: // reconnect after adequate back off
            if (currentTime > lastHeartbeatSent + backOff) {
                LOGGER.atTrace().addArgument(address).log("Connecting to {}");
                connect();
            }
            return lastHeartbeatSent + backOff;
        case CONNECTING:
            if (currentTime > lastHeartbeatSent + heartbeatInterval * heartbeatAllowedMisses) { // connect timed out -> increase back of and retry
                backOff = backOff * 2;
                lastHeartbeatSent = currentTime;
                LOGGER.atTrace().addArgument(connectedAddress).addArgument(backOff).log("Connection timed out for {}, retrying in {} ms");
                disconnect();
            }
            return lastHeartbeatSent + heartbeatInterval * heartbeatAllowedMisses;
        case CONNECTED:
            Request request;
            while ((request = queuedRequests.poll()) != null) {
                pendingRequests.put(request.id, request);
                sendRequest(request);
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

    private void sendRequest(final Request request) {
        // Filters and data are already serialised but the protocol saves them deserialised :/
        // final ZFrame data = request.data == null ? new ZFrame(new byte[0]) : new ZFrame(request.data);
        // final ZFrame filters = request.filters == null ? new ZFrame(new byte[0]) : new ZFrame(request.filters);
        final Endpoint requestEndpoint = new Endpoint(request.endpoint.toString());

        try {
            switch (request.requestType) {
            case GET:
                CmwLightProtocol.sendMsg(socket, CmwLightMessage.getRequest(
                                                         sessionId, request.id, requestEndpoint.getDevice(), requestEndpoint.getProperty(),
                                                         new CmwLightMessage.RequestContext(requestEndpoint.getSelector(), requestEndpoint.getFilters(), null)));
                break;
            case SET:
                Objects.requireNonNull(request.data, "Data for set cannot be null");
                CmwLightProtocol.sendMsg(socket, CmwLightMessage.setRequest(
                                                         sessionId, request.id, requestEndpoint.getDevice(), requestEndpoint.getProperty(),
                                                         new ZFrame(request.data),
                                                         new CmwLightMessage.RequestContext(requestEndpoint.getSelector(), requestEndpoint.getFilters(), null)));
                break;
            default:
                throw new CmwLightProtocol.RdaLightException("Message of unknown type");
            }
        } catch (CmwLightProtocol.RdaLightException e) {
            LOGGER.atDebug().setCause(e).log("Error sending get request:");
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

    public void sendHeartBeat() {
        try {
            CmwLightProtocol.sendMsg(socket, CmwLightMessage.CLIENT_HB);
        } catch (CmwLightProtocol.RdaLightException e) {
            LOGGER.atDebug().setCause(e).log("Error sending heartbeat");
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
            LOGGER.atError().addArgument(sub.property).log("failed to unsubscribe ");
        }
    }

    public static class Subscription {
        private final long id = REQUEST_ID_GENERATOR.incrementAndGet();
        public final String property;
        public final String device;
        public final String selector;
        public final Map<String, Object> filters;
        public final URI endpoint;
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
        private final String requestId;
        private final URI endpoint;
        private final byte[] rbacToken;
        public final CmwLightProtocol.RequestType requestType;

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

    public enum SubscriptionState {
        UNSUBSCRIBED,
        SUBSCRIBING,
        SUBSCRIBED,
        CANCELED,
        UNSUBSCRIBE_SENT
    }
}
