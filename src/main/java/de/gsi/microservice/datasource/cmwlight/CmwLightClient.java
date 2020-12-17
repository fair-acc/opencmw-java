package de.gsi.microservice.datasource.cmwlight;

import de.gsi.microservice.datasource.DataSource;
import de.gsi.microservice.datasource.Endpoint;
import de.gsi.serializer.IoSerialiser;
import de.gsi.serializer.spi.BinarySerialiser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.*;

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
            return BinarySerialiser.class; // Binary serialiser, because we unpack it internally anyway for now
        }

        @Override
        public DataSource newInstance(final ZContext context, final String endpoint, final Duration timeout, final String clientId, final byte[] filters) {
            return new CmwLightClient(context, endpoint, timeout, clientId, filters);
        }
    };
    protected final AtomicInteger channelId = new AtomicInteger(0); // connection local counter incremented for each channel
    protected final ZContext context;
    protected final ZMQ.Socket socket;
    protected final AtomicReference<ConnectionState> connectionState = new AtomicReference<>(ConnectionState.DISCONNECTED);
    protected final Endpoint endpoint;
    private final byte[] filterMap;
    protected String sessionId;
    protected long connectionId;
    protected final Map<Long, Subscription> subscriptions = Collections.synchronizedMap(new HashMap<>()); // all subscriptions added to the server
    protected long lastHbReceived = -1;
    protected long lastHbSent = -1;
    protected static final int heartbeatInterval = 1000; // time between to heartbeats in ms
    protected static final int heartbeatAllowedMisses = 3; // number of heartbeats which can be missed before resetting the conection
    protected static final long subscriptionTimeout = 1000; // maximum time after which a connection should be reconnected
    protected int backOff = 20;

    public CmwLightClient(final ZContext context, final String endpoint, final Duration timeout, final String clientId, final byte[] filters) {
        super(context, endpoint, timeout, clientId, filters);
        LOGGER.atTrace().addArgument(endpoint).log("connecting to: {}");
        this.context = context;
        socket = context.createSocket(SocketType.DEALER);
        socket.setIdentity(getIdentity().getBytes()); // hostname/process/id/channel
        this.sessionId = getSessionId(clientId);
        this.endpoint = new Endpoint(endpoint);
        this.filterMap = filters; // todo correctly handle preserialised filters
    }

    public void unsubscribe(final Subscription subscription) throws CmwLightProtocol.RdaLightException {
        subscriptions.remove(subscription.id);
        sendUnsubscribe(subscription);
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
            final ZMsg result = new ZMsg();
            final Subscription subscription = subscriptions.values().stream().filter(s -> s.updateId == reply.id).findAny().orElseThrow();
            result.add(subscription.idString);
            result.add(endpoint.getEndpointForContext(reply.dataContext.cycleName));
            result.add(new ZFrame(new byte[0])); // header
            result.add(reply.bodyData); // body
            result.add(new ZFrame(new byte[0])); // exception
            return result;
        } else if (reply.requestType.equals(CmwLightProtocol.RequestType.EXCEPTION) || reply.requestType.equals(CmwLightProtocol.RequestType.NOTIFICATION_EXC) || reply.requestType.equals(CmwLightProtocol.RequestType.SUBSCRIBE_EXCEPTION)) {
            // forward errors
            final ZMsg result = new ZMsg();
            if (reply.requestType.equals(CmwLightProtocol.RequestType.NOTIFICATION_EXC)) { // error for this update
                final Subscription subscription = subscriptions.values().stream().filter(s -> s.updateId == reply.id).findAny().orElseThrow();
                result.add(subscription.idString);
            } else if (reply.requestType.equals(CmwLightProtocol.RequestType.SUBSCRIBE_EXCEPTION)) { // error setting up the subscription
                final Subscription subscription = subscriptions.values().stream().filter(s -> s.id == reply.id).findAny().orElseThrow();
                result.add(subscription.idString);
            } else { // error for get or other requests
                result.add(Long.toString(reply.id)); // todo: get correct id from list of running requests
            }
            result.add(endpoint.toString());
            result.add(new ZFrame(new byte[0])); // header
            result.add(new ZFrame(new byte[0])); // body
            result.add(reply.exceptionMessage.message); // exception
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

    @Override
    public String getEndpoint() {
        return endpoint.toString();
    }

    @Override
    public void get(final String requestId, final String filterPattern, final byte[] filters1, final byte[] data, final byte[] rbacToken) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public void set(final String requestId, final String endpoint, final byte[] filters, final byte[] data, final byte[] rbacToken) {
        throw new UnsupportedOperationException("Set is not implemented for cmw light");
    }

    @Override
    public void subscribe(final String reqId, final byte[] rbacToken) {
        final Subscription sub = new Subscription(endpoint.getDevice(), endpoint.getProperty(), endpoint.getSelector(), endpoint.getFilters());
        sub.idString = reqId;
        LOGGER.atDebug().addArgument(sub).log("subscribing: {}");
        subscribe(sub);
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

    private String getSessionId(final String clientId) {
        return "cmwLightClient{pid=" + ProcessHandle.current().pid() + ", conn=" + connectionId + ", clientId=" + clientId + '}'; // Todo: create identification string, cmw uses string with user/app name, pid, etc
    }

    public void connect() {
        LOGGER.atDebug().addArgument(endpoint).log("connecting to {}");
        if (connectionState.getAndSet(ConnectionState.CONNECTING) != ConnectionState.DISCONNECTED) {
            return;
        }
        final String address = endpoint.getAddress().replace("rda3://", "tcp://");
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
    }

    private void disconnect() {
        LOGGER.atDebug().addArgument(endpoint).log("disconnecting {}");
        connectionState.set(ConnectionState.DISCONNECTED);
        socket.disconnect(endpoint.getAddress().replace("rda3://", "tcp://"));
        // disconnect/reset subscriptions
        for (Subscription sub: subscriptions.values()) {
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
                connectionState.set(ConnectionState.CONNECTED);
                lastHbReceived = currentTime;
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
