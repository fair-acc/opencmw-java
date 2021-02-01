package io.opencmw.client.cmwlight;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.*;

import io.opencmw.client.DataSource;
import io.opencmw.client.Endpoint;
import io.opencmw.serialiser.IoSerialiser;
import io.opencmw.serialiser.spi.CmwLightSerialiser;

/**
 * A lightweight implementation of the CMW RDA3 client part.
 * Reads all sockets from a single Thread, which can also be embedded into other event loops.
 * Manages connection state and automatically reconnects broken connections and subscriptions.
 */
public class CmwLightDataSource extends DataSource {
    private static final Logger LOGGER = LoggerFactory.getLogger(CmwLightDataSource.class);
    private static final AtomicLong connectionIdGenerator = new AtomicLong(0); // global counter incremented for each connection
    private static final AtomicInteger requestIdGenerator = new AtomicInteger(0);
    public static final String RDA_3_PROTOCOL = "rda3://";
    public static final Factory FACTORY = new Factory() {
        @Override
        public boolean matches(final String endpoint) {
            return endpoint.startsWith(RDA_3_PROTOCOL);
        }

        @Override
        public Class<? extends IoSerialiser> getMatchingSerialiserType(final String endpoint) {
            return CmwLightSerialiser.class;
        }

        @Override
        public DataSource newInstance(final ZContext context, final String endpoint, final Duration timeout, final String clientId) {
            return new CmwLightDataSource(context, endpoint, timeout, clientId);
        }
    };
    private static DirectoryLightClient directoryLightClient;
    protected final AtomicInteger channelId = new AtomicInteger(0); // connection local counter incremented for each channel
    protected final ZContext context;
    protected final ZMQ.Socket socket;
    protected final AtomicReference<ConnectionState> connectionState = new AtomicReference<>(ConnectionState.DISCONNECTED);
    private String address;
    protected String sessionId;
    protected long connectionId;
    protected final Map<Long, Subscription> subscriptions = new HashMap<>(); // all subscriptions added to the server
    protected final Map<Long, Subscription> replyIdMap = new HashMap<>(); // all acknowledged subscriptions by their reply id
    protected long lastHbReceived = -1;
    protected long lastHbSent = -1;
    protected static final int heartbeatInterval = 1000; // time between to heartbeats in ms
    protected static final int heartbeatAllowedMisses = 3; // number of heartbeats which can be missed before resetting the conection
    protected static final long subscriptionTimeout = 1000; // maximum time after which a connection should be reconnected
    protected int backOff = 20;
    private final Queue<Request<?>> queuedRequests = new LinkedBlockingQueue<>();
    private final Map<Long, Request> pendingRequests = new HashMap<>();
    private String connectedAddress = "";

    public CmwLightDataSource(final ZContext context, final String endpoint, final Duration timeout, final String clientId) {
        super(context, endpoint, timeout, clientId);
        LOGGER.atTrace().addArgument(endpoint).log("connecting to: {}");
        this.context = context;
        this.socket = context.createSocket(SocketType.DEALER);
        this.sessionId = getSessionId(clientId);
        this.address = new Endpoint(endpoint).getAddress();
    }

    public static DirectoryLightClient getDirectoryLightClient() {
        return directoryLightClient;
    }

    public static void setDirectoryLightClient(final DirectoryLightClient directoryLightClient) {
        CmwLightDataSource.directoryLightClient = directoryLightClient;
    }

    @Override
    public ZMsg getMessage() { // return maintenance objects instead of replies
        CmwLightMessage reply = receiveData();
        if (reply == null) {
            return null;
        }
        if (reply.messageType == CmwLightProtocol.MessageType.SERVER_HB || reply.messageType == CmwLightProtocol.MessageType.SERVER_CONNECT_ACK) {
            return new ZMsg();
        }
        if (reply.requestType.equals(CmwLightProtocol.RequestType.NOTIFICATION_DATA)) {
            final Subscription subscription = replyIdMap.get(reply.id);
            if (subscription == null) {
                LOGGER.atInfo().addArgument(reply.toString()).log("Got unsolicited subscription data: {}");
                return new ZMsg();
            }
            final ZMsg result = new ZMsg();
            result.add(subscription.idString);
            result.add(new Endpoint(subscription.endpoint).getEndpointForContext(reply.dataContext.cycleName));
            result.add(new ZFrame(new byte[0])); // header
            result.add(reply.bodyData); // body
            result.add(new ZFrame(new byte[0])); // exception
            return result;
        } else if (reply.requestType.equals(CmwLightProtocol.RequestType.EXCEPTION) || reply.requestType.equals(CmwLightProtocol.RequestType.NOTIFICATION_EXC) || reply.requestType.equals(CmwLightProtocol.RequestType.SUBSCRIBE_EXCEPTION)) {
            // forward errors
            final ZMsg result = new ZMsg();
            if (reply.requestType.equals(CmwLightProtocol.RequestType.NOTIFICATION_EXC)) { // error for this update
                final Subscription subscription = replyIdMap.get(reply.id);
                if (subscription == null) {
                    LOGGER.atInfo().addArgument(reply.toString()).log("Got unsolicited subscription notification error: {}");
                    return new ZMsg();
                }
                result.add(subscription.idString);
                result.add(subscription.endpoint);
            } else if (reply.requestType.equals(CmwLightProtocol.RequestType.SUBSCRIBE_EXCEPTION)) { // error setting up the subscription
                final Subscription subscription = subscriptions.values().stream().filter(s -> s.id == reply.id).findAny().orElseThrow();
                result.add(subscription.idString);
                result.add(subscription.endpoint);
            } else { // error for get or other requests
                final Request request = pendingRequests.remove(reply.id);
                result.add(request.requestId);
                result.add(request.endpoint);
            }
            result.add(new ZFrame(new byte[0])); // header
            result.add(new ZFrame(new byte[0])); // body
            result.add(reply.exceptionMessage.message); // exception
            return result;
        } else if (reply.requestType.equals(CmwLightProtocol.RequestType.REPLY)) {
            final ZMsg result = new ZMsg();
            Request request = pendingRequests.remove(reply.id);
            result.add(request.requestId);
            result.add(new Endpoint(request.endpoint).getEndpointForContext(reply.dataContext.cycleName));
            result.add(new ZFrame(new byte[0])); // header
            result.add(reply.bodyData); // body
            result.add(new ZFrame(new byte[0])); // exception
            return result;
        } else {
            return new ZMsg();
        }
    }

    public enum ConnectionState {
        DISCONNECTED,
        CONNECTING,
        CONNECTED
    }

    @Override
    public void get(final String requestId, final String endpoint, final byte[] filters, final byte[] data, final byte[] rbacToken) {
        final Request request = new Request(CmwLightProtocol.RequestType.GET, requestId, endpoint, filters, data, rbacToken);
        queuedRequests.add(request);
    }

    @Override
    public void set(final String requestId, final String endpoint, final byte[] filters, final byte[] data, final byte[] rbacToken) {
        final Request request = new Request(CmwLightProtocol.RequestType.SET, requestId, endpoint, filters, data, rbacToken);
        queuedRequests.add(request);
    }

    @Override
    public void subscribe(final String reqId, final String endpoint, final byte[] rbacToken) {
        final Endpoint ep = new Endpoint(endpoint);
        final Subscription sub = new Subscription(endpoint, ep.getDevice(), ep.getProperty(), ep.getSelector(), ep.getFilters());
        sub.idString = reqId;
        subscriptions.put(sub.id, sub);
    }

    @Override
    public void unsubscribe(final String reqId) {
        subscriptions.get(reqId).subscriptionState = SubscriptionState.CANCELED;
    }

    public ConnectionState getConnectionState() {
        return connectionState.get();
    }

    public ZContext getContext() {
        return context;
    }

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
        connectionId = connectionIdGenerator.incrementAndGet();
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
        String address = this.address.startsWith(RDA_3_PROTOCOL) ? this.address.substring(RDA_3_PROTOCOL.length()) : this.address;
        if (!address.contains(":")) {
            try {
                DirectoryLightClient.Device device = directoryLightClient.getDeviceInfo(Collections.singletonList(address)).get(0);
                LOGGER.atTrace().addArgument(address).addArgument(device).log("resolved address for device {}: {}");
                address = device.servers.stream().findFirst().orElseThrow().get("Address:");
            } catch (NullPointerException | NoSuchElementException | DirectoryLightClient.DirectoryClientException e) {
                LOGGER.atDebug().addArgument(e.getMessage()).log("Error resolving device from nameserver, using address from endpoint. Error was: {}");
                backOff = backOff * 2;
                connectionState.set(ConnectionState.DISCONNECTED);
                return;
            }
        }
        lastHbSent = System.currentTimeMillis();
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

    public CmwLightMessage receiveData() {
        final long currentTime = System.currentTimeMillis();
        // receive data
        try {
            final ZMsg data = ZMsg.recvMsg(socket, ZMQ.DONTWAIT);
            if (data == null)
                return null;
            final CmwLightMessage reply = CmwLightProtocol.parseMsg(data);
            if (connectionState.get().equals(ConnectionState.CONNECTING) && reply.messageType == CmwLightProtocol.MessageType.SERVER_CONNECT_ACK) {
                LOGGER.atTrace().addArgument(connectedAddress).log("Connected to server: {}");
                connectionState.set(ConnectionState.CONNECTED);
                lastHbReceived = currentTime;
                backOff = 20; // reset back-off time
                return reply;
            }
            if (connectionState.get() != ConnectionState.CONNECTED) {
                LOGGER.atWarn().addArgument(reply).log("received data before connection established: {}");
            }
            if (reply.requestType == CmwLightProtocol.RequestType.SUBSCRIBE_EXCEPTION) { // subscription failed
                final long id = reply.id;
                final Subscription sub = subscriptions.get(id);
                sub.subscriptionState = SubscriptionState.UNSUBSCRIBED;
                sub.timeoutValue = currentTime + sub.backOff;
                sub.backOff *= 2;
                LOGGER.atDebug().addArgument(sub.device).addArgument(sub.property).log("exception during subscription, retrying: {}/{}");
            }
            if (reply.requestType == CmwLightProtocol.RequestType.SUBSCRIBE) { // subscription successful
                final long id = reply.id;
                final Subscription sub = subscriptions.get(id);
                sub.updateId = (long) reply.options.get(CmwLightProtocol.FieldName.SOURCE_ID_TAG.value());
                sub.subscriptionState = SubscriptionState.SUBSCRIBED;
                LOGGER.atDebug().addArgument(sub.device).addArgument(sub.property).log("subscription successful: {}/{}");
                sub.backOff = 20;
            }
            lastHbReceived = currentTime;
            return reply;
        } catch (CmwLightProtocol.RdaLightException e) {
            LOGGER.atDebug().setCause(e).log("error parsing cmw light reply: ");
            return null;
        }
    }

    @Override
    public long housekeeping() {
        final long currentTime = System.currentTimeMillis();
        switch (connectionState.get()) {
        case DISCONNECTED: // reconnect after adequate back off
            if (currentTime > lastHbSent + backOff) {
                LOGGER.atTrace().addArgument(address).log("Connecting to {}");
                connect();
            }
            return lastHbSent + backOff;
        case CONNECTING:
            if (currentTime > lastHbSent + heartbeatInterval * heartbeatAllowedMisses) { // connect timed out -> increase back of and retry
                backOff = backOff * 2;
                lastHbSent = currentTime;
                LOGGER.atTrace().addArgument(connectedAddress).addArgument(backOff).log("Connection timed out for {}, retrying in {} ms");
                disconnect();
            }
            return lastHbSent + heartbeatInterval * heartbeatAllowedMisses;
        case CONNECTED:
            Request<?> request;
            while ((request = queuedRequests.poll()) != null) {
                pendingRequests.put(request.id, request);
                sendRequest(request);
            }
            if (currentTime > lastHbSent + heartbeatInterval) { // check for heartbeat interval
                // send Heartbeats
                sendHeartBeat();
                lastHbSent = currentTime;
                // check if heartbeat was received
                if (lastHbReceived + heartbeatInterval * heartbeatAllowedMisses < currentTime) {
                    LOGGER.atDebug().addArgument(backOff).log("Connection timed out, reconnecting in {} ms");
                    disconnect();
                    return heartbeatInterval;
                }
                // check timeouts of connection/subscription requests
                for (Subscription sub : subscriptions.values()) {
                    updateSubscription(currentTime, sub);
                }
            }
            return lastHbSent + heartbeatInterval;
        default:
            throw new IllegalStateException("unexpected connection state: " + connectionState.get());
        }
    }

    private void sendRequest(final Request<?> request) {
        // Filters and data are already serialised but the protocol saves them deserialised :/
        // final ZFrame data = request.data == null ? new ZFrame(new byte[0]) : new ZFrame(request.data);
        // final ZFrame filters = request.filters == null ? new ZFrame(new byte[0]) : new ZFrame(request.filters);
        final Endpoint requestEndpoint = new Endpoint(request.endpoint);

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
                sub.timeoutValue = currentTime + backOff;
                backOff = backOff * 2; // exponential back of
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
        public final String property;
        public final String device;
        public final String selector;
        public final Map<String, Object> filters;
        public final String endpoint;
        private SubscriptionState subscriptionState = SubscriptionState.UNSUBSCRIBED;
        private int backOff = 20;
        private long id = requestIdGenerator.incrementAndGet();
        private long updateId = -1;
        private long timeoutValue = -1;
        public String idString = "";

        public Subscription(final String endpoint, final String device, final String property, final String selector, final Map<String, Object> filters) {
            this.endpoint = endpoint;
            this.property = property;
            this.device = device;
            this.selector = selector;
            this.filters = filters;
        }

        @Override
        public String toString() {
            return "Subscription{"
                    + "property='" + property + '\'' + ", device='" + device + '\'' + ", selector='" + selector + '\'' + ", filters=" + filters + ", subscriptionState=" + subscriptionState + ", backOff=" + backOff + ", id=" + id + ", updateId=" + updateId + ", timeoutValue=" + timeoutValue + '}';
        }
    }

    public static class Request<T> {
        public final byte[] filters;
        public final byte[] data;
        public final long id;
        private final String requestId;
        private final String endpoint;
        private final byte[] rbacToken;
        public CmwLightProtocol.RequestType requestType;

        public Request(final CmwLightProtocol.RequestType requestType, final String requestId, final String endpoint, final byte[] filters, final byte[] data, final byte[] rbacToken) {
            this.requestType = requestType;
            this.id = requestIdGenerator.incrementAndGet();
            this.requestId = requestId;
            this.endpoint = endpoint;
            this.filters = filters;
            this.data = data;
            this.rbacToken = rbacToken;
        }
    }

    public enum SubscriptionState {
        UNSUBSCRIBED,
        SUBSCRIBING,
        SUBSCRIBED,
        CANCELED,
        UNSUBSCRIBE_SENT;
    }
}
