package de.gsi.microservice.datasource.cmwlight;

import de.gsi.microservice.datasource.DataSource;
import de.gsi.serializer.IoClassSerialiser;
import de.gsi.serializer.IoSerialiser;
import de.gsi.serializer.spi.BinarySerialiser;
import de.gsi.serializer.spi.FastByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A lightweight implementation of the CMW RDA client part.
 * Reads all sockets from a single Thread, which can also be embedded into other event loops.
 * Manages connection state and automatically reconnects broken connections and subscriptions.
 */
public class CmwLightClient extends DataSource {
    private static final Logger LOGGER = LoggerFactory.getLogger(CmwLightClient.class);
    private static final AtomicLong connectionIdGenerator = new AtomicLong(0); // global counter incremented for each connection
    private static final AtomicInteger requestIdGenerator = new AtomicInteger(0);
    public static final Factory FACTORY = new Factory() {
        @Override
        public boolean matches(final String endpoint) {
            return endpoint.startsWith("rda3://");
        }

        @Override
        public Class<? extends IoSerialiser> getMatchingSerialiserType(final String endpoint) {
            return BinarySerialiser.class; // Binarary serialiser, because we unpack it internaly anyway for now
        }

        @Override
        public DataSource newInstance(final ZContext context, final String endpoint, final Duration timeout, final String clientId, final byte[] filters) {
            return new CmwLightClient(context, endpoint, timeout, clientId, filters);
        }
    };
    {
        DataSource.register(FACTORY);
    }
    public static final String DEFAULT_CONTEXT = "FAIR.SELECTOR.ALL";
    protected final AtomicInteger channelId = new AtomicInteger(0); // connection local counter incremented for each channel
    protected final ZContext context;
    protected final ZMQ.Socket socket;
    protected final AtomicReference<ConnectionState> connectionState = new AtomicReference<>(ConnectionState.DISCONNECTED);
    protected final String endpoint;
    private final byte[] filterMap;
    private final String clientId;
    protected String sessionId;
    protected long connectionId;
    protected final Map<Long, Subscription> subscriptions = Collections.synchronizedMap(new HashMap<>()); // all subscriptions added to the server
    protected long lastHbReceived = -1;
    protected long lastHbSent = -1;
    protected static final int heartbeatInterval = 1000; // time between to heartbeats in ms
    protected static final int heartbeatAllowedMisses = 3; // number of heartbeats which can be missed before resetting the conection
    protected static final long subscriptionTimeout = 1000; // maximum time after which a connection should be reconnected
    protected int backOff = 20;

    public void unsubscribe(final Subscription subscription) throws CmwLightProtocol.RdaLightException {
        subscriptions.remove(subscription.id);
        sendUnsubscribe(subscription);
    }

    @Override
    public ZMsg getMessage() { // return maintaninance objects instead of replies
        CmwLightMessage reply = receiveData();
        if (reply == null) {
            return null;
        }
        if (reply.messageType == CmwLightProtocol.MessageType.SERVER_HB || reply.messageType == CmwLightProtocol.MessageType.SERVER_CONNECT_ACK) {
            return new ZMsg();
        }
        if (reply.requestType.equals(CmwLightProtocol.RequestType.NOTIFICATION_DATA)) {
            final ZMsg result = new ZMsg();
            // todo: introduce hashmap for this mapping
            final Subscription subscription = subscriptions.values().stream().filter(s -> s.updateId == reply.id).findAny().orElseThrow();
            result.add(subscription.idString);
            result.add(endpoint);
            result.add(reply.bodyData);
            return result;
        } else if (reply.requestType.equals(CmwLightProtocol.RequestType.REPLY)) {
            return new ZMsg();
        } else {
            return new ZMsg();
        }
    }

    public enum ConnectionState {
        DISCONNECTED,
        CONNECTING,
        CONNECTED
    }

    public CmwLightClient(final ZContext context, final String endpoint, final Duration timeout, final String clientId, final byte[] filters) {
        super(context, endpoint, timeout, clientId, filters);
        LOGGER.atTrace().addArgument(endpoint).log("connecting to: {}");
        this.context = context;
        socket = context.createSocket(SocketType.DEALER);
        socket.setIdentity(getIdentity().getBytes()); // hostname/process/id/channel
        this.endpoint = endpoint;
        this.clientId = clientId;
        this.sessionId = getSessionId();
        this.filterMap = filters;
    }

    @Override
    public String getEndpoint() {
        return endpoint;
    }

    @Override
    public void get(final String requestId, final String filterPattern, final byte[] filters1, final byte[] data, final byte[] rbacToken) {
        final String[] tmp = filterPattern.split("\\?");
        final String[] adp = tmp[0].split("/");
        String device;
        String property;
        if (adp.length == 2) {
            device = adp[0];
            property = adp[1];
        } else if (adp.length == 3) {
            device = adp[1];
            property = adp[2];
        } else {
            throw new IllegalArgumentException("could not parse endpoint " + filterPattern);
        }
        final Map<String, Object> filters = new HashMap<>();
        String context = tmp.length > 1 ? parseUrlParams(tmp[1], filters, DEFAULT_CONTEXT) : DEFAULT_CONTEXT;
        throw new UnsupportedOperationException("not yet implemented");
        // get(device, property, context, filters, requestId);
    }

    @Override
    public void set(final String requestId, final String endpoint, final byte[] filters, final byte[] data, final byte[] rbacToken) {
        throw new UnsupportedOperationException("Set is not implemented for cmw light");
    }

    @Override
    public void subscribe(final String reqId, final byte[] rbacToken) {
        final String[] tmp = endpoint.split("\\?", 2); // split into address/dev/prop and sel+filters part
        final String[] adp = tmp[0].split("/"); // split access point into parts
        String device= adp[adp.length - 2]; // get device name from access point
        String property = adp[adp.length - 1]; // get property name from access point
        Map<String, Object> filters = new HashMap<>();
        if (filterMap != null && filterMap.length > 0) { // read binary filter map if present
            filters.putAll((Map<String, Object>) new IoClassSerialiser(FastByteBuffer.wrap(filterMap)).deserialiseObject(filters));
        }
        final String context = tmp.length > 1 ? parseUrlParams(tmp[1], filters, DEFAULT_CONTEXT) : DEFAULT_CONTEXT;
        final Subscription sub = new Subscription(device, property, context, filters);
        sub.idString = reqId;
        LOGGER.atDebug().addArgument(sub).log("subscribing: {}");
        subscribe(sub);
    }

    private String parseUrlParams(final String paramString, final Map<String, Object> outMap, final String defaultContext) {
        String context = defaultContext;
        final String[] kvpairs = paramString.split("\\&"); // split into individual key/value pairs
        for (final String pair : kvpairs) {
            String[] splitpair = pair.split("=", 2); // split at first equal sign
            if (splitpair.length != 2) {
                continue;
            }
            if (splitpair[0].equals("ctx")) {
                context = splitpair[1];
            } else {
                if (splitpair[1].startsWith("int:")) {
                    outMap.put(splitpair[0], Integer.valueOf(splitpair[1].substring("int:".length())));
                } else if (splitpair[1].startsWith("long:")) {
                    outMap.put(splitpair[0], Long.valueOf(splitpair[1].substring("long:".length())));
                } else {
                    outMap.put(splitpair[0], splitpair[1]);
                }
            }
        }
        return context;
    }

    @Override
    public void unsubscribe() {
        // just unsubscribe the first (should be the only) subscription
        try {
            unsubscribe(subscriptions.values().iterator().next());
        } catch (CmwLightProtocol.RdaLightException e) {
            LOGGER.atError().setCause(e).log("Exception trying to unsubscribe property");
        }
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

    private String getSessionId() {
        return "cmwLightClient" + ProcessHandle.current().pid() + '/' + connectionId; // Todo: create identification string, cmw uses string with user/app name, pid, etc
    }

    public void connect() {
        LOGGER.atDebug().addArgument(endpoint).log("connecting to {}");
        if (connectionState.getAndSet(ConnectionState.CONNECTING) != ConnectionState.DISCONNECTED) {
            return;
        }
        final String address = "tcp://" + endpoint.replace("rda3://", "").split("/")[0];
        socket.connect(address);
        try {
            CmwLightProtocol.serialiseMsg(CmwLightMessage.connect("1.0.0")).send(socket);
            connectionState.set(ConnectionState.CONNECTING);
            lastHbReceived = System.currentTimeMillis();
        } catch (CmwLightProtocol.RdaLightException e) {
            LOGGER.atDebug().setCause(e).log("failed to connect: ");
            backOff = backOff * 2;
            resetConnection();
        }
    }

    private void resetConnection() {
        disconnect();
        connect();
    }

    private void disconnect() {
        LOGGER.atDebug().addArgument(endpoint).log("disconnecting {}");
        connectionState.set(ConnectionState.DISCONNECTED);
        socket.disconnect(endpoint);
        // disconnect/reset subscriptions
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
                connectionState.set(ConnectionState.CONNECTED);
                backOff = 20; // reset back-off time
                return reply;
            }
            if (connectionState.get() != ConnectionState.CONNECTED) {
                LOGGER.atWarn().addArgument(reply).log("received data before connection established: {}");
            }
            if (reply.requestType == CmwLightProtocol.RequestType.SUBSCRIBE_EXCEPTION) {
                final long id = reply.id;
                final Subscription sub = subscriptions.get(id);
                sub.subscriptionState = SubscriptionState.UNSUBSCRIBED;
                sub.timeoutValue = currentTime + sub.backOff;
                sub.backOff *= 2;
                LOGGER.atDebug().addArgument(sub.device).addArgument(sub.property).log("exception during subscription, retrying: {}/{}");
            }
            if (reply.requestType == CmwLightProtocol.RequestType.SUBSCRIBE) {
                final long id = reply.id;
                final Subscription sub = subscriptions.get(id);
                sub.updateId = (long) reply.options.get(CmwLightProtocol.FieldName.SOURCE_ID_TAG.value());
                sub.subscriptionState = SubscriptionState.SUBSCRIBED;
                LOGGER.atDebug().addArgument(sub.device).addArgument(sub.property).log("subscription sucessful: {}/{}");
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
                connect();
            }
            return lastHbSent + backOff;
        case CONNECTING:
            if (currentTime > lastHbReceived + heartbeatInterval * heartbeatAllowedMisses) { // connect timed out -> increase back of and retry
                backOff = backOff * 2;
                resetConnection();
            }
            return lastHbReceived + heartbeatInterval * heartbeatAllowedMisses;
        case CONNECTED:
            if (currentTime > lastHbSent + heartbeatInterval) { // check for heartbeat interval
                // send Heartbeats
                sendHeartBeat();
                lastHbSent = currentTime;
                // check if heartbeat was received
                if (lastHbReceived + heartbeatInterval * heartbeatAllowedMisses < currentTime) {
                    LOGGER.atInfo().log("Connection timed out, reconnecting");
                    resetConnection();
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
            // do nothing
            break;
        default:
            throw new IllegalStateException("unexpected subscription state: " + sub.subscriptionState);
        }
    }

    public void subscribe(final Subscription sub) {
        subscriptions.put(sub.id, sub);
    }

    public void sendHeartBeat() {
        try {
            CmwLightProtocol.sendMsg(socket, CmwLightMessage.CLIENT_HB);
        } catch (CmwLightProtocol.RdaLightException e) {
            LOGGER.atDebug().setCause(e).log("Error sending heartbeat");
        }
    }

    private void sendSubscribe(final Subscription sub){
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

    private void sendUnsubscribe(final Subscription sub) throws CmwLightProtocol.RdaLightException {
        if (sub.subscriptionState.equals(SubscriptionState.UNSUBSCRIBED)) {
            return; // not currently subscribed to this property
        }
        CmwLightProtocol.sendMsg(socket, CmwLightMessage.unsubscribeRequest(
                                                         sessionId, sub.updateId, sub.device, sub.property,
                                                         Map.of(CmwLightProtocol.FieldName.SESSION_BODY_TAG.value(), Collections.<String, Object>emptyMap()),
                                                         CmwLightProtocol.UpdateType.IMMEDIATE_UPDATE));
    }

    public static class Subscription {
        public final String property;
        public final String device;
        public final String selector;
        public final Map<String, Object> filters;
        private SubscriptionState subscriptionState = SubscriptionState.UNSUBSCRIBED;
        private int backOff = 20;
        private long id = requestIdGenerator.incrementAndGet();
        private long updateId = -1;
        private long timeoutValue = -1;
        public String idString = "";

        public Subscription(final String device, final String property, final String selector, final Map<String, Object> filters) {
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

    public enum SubscriptionState {
        UNSUBSCRIBED,
        SUBSCRIBING,
        SUBSCRIBED
    }
}
