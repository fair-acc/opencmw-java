package io.opencmw.client;

import static io.opencmw.OpenCmwProtocol.Command.*;
import static io.opencmw.OpenCmwProtocol.EMPTY_FRAME;
import static io.opencmw.OpenCmwProtocol.MdpSubProtocol.PROT_CLIENT;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiPredicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.*;
import org.zeromq.ZMQ.Socket;

import io.opencmw.OpenCmwProtocol;
import io.opencmw.filter.SubscriptionMatcher;
import io.opencmw.serialiser.IoSerialiser;
import io.opencmw.serialiser.spi.BinarySerialiser;

/**
 * Client implementation for the OpenCMW protocol.
 *
 * Implements the mdp:// (opencmw tcp get/set/sub), mds:// (opencmw udp multicast subscriptions) and mdr:// (majordomo
 * radio dish for e.g. timing updates) protocols.
 *
 * @author Alexander Krimm
 */
public class OpenCmwDataSource extends DataSource {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenCmwDataSource.class);
    private static final String MDP = "mdp";
    private static final String MDS = "mds";
    private static final String MDR = "mdr";
    public static final Factory FACTORY = new Factory() {
        @Override
        public boolean matches(final URI endpoint) {
            return endpoint.getScheme().equals(MDP) || endpoint.getScheme().equals(MDS) || endpoint.getScheme().equals(MDR);
        }

        @Override
        public Class<? extends IoSerialiser> getMatchingSerialiserType(final URI endpoint) {
            return BinarySerialiser.class;
        }

        @Override
        public DataSource newInstance(final ZContext context, final URI endpoint, final Duration timeout, final String clientId) {
            return new OpenCmwDataSource(context, endpoint, clientId);
        }
    };
    private final String clientId;
    private final URI endpoint;
    private final Socket socket;
    private final Map<String, URI> subscriptions = new HashMap<>(); // NOPMD: not accessed concurrently
    private final URI serverUri;
    private final BiPredicate<URI, URI> subscriptionMatcher = new SubscriptionMatcher(); // <notify topic, subscribe topic>

    /**
     * @param context zeroMQ context used for internal as well as external communication
     * @param endpoint The endpoint to connect to. Only the server part is used and everything else is discarded.
     * @param clientId Identification string sent to the OpenCMW server. Should be unique per client and is used by the
     *                 Server to identify clients.
     */
    public OpenCmwDataSource(final ZContext context, final URI endpoint, final String clientId) {
        super(endpoint);
        this.endpoint = endpoint; // todo: Strip unneeded parts?
        this.clientId = clientId;
        try {
            this.serverUri = new URI(endpoint.getScheme(), endpoint.getAuthority(), "/", null, null);
        } catch (URISyntaxException e) {
            LOGGER.atError().addArgument(endpoint).setCause(e).log("cannot create serverURI from endpoint: {}");
            throw new IllegalArgumentException("Invalid endpoint", e);
        }
        if (MDP.equals(endpoint.getScheme())) {
            this.socket = context.createSocket(SocketType.DEALER);
            socket.setHWM(0);
            socket.setIdentity(clientId.getBytes(StandardCharsets.UTF_8));
            socket.connect("tcp://" + this.endpoint.getAuthority());
        } else if (MDS.equals(endpoint.getScheme())) {
            this.socket = context.createSocket(SocketType.SUB);
            socket.setHWM(0);
            socket.setIdentity(clientId.getBytes(StandardCharsets.UTF_8));
            socket.connect("tcp://" + this.endpoint.getAuthority()); // determine whether udp or tcp should be used
        } else if (MDR.equals(endpoint.getScheme())) {
            throw new UnsupportedOperationException("RADIO-DISH pattern is not yet implemented");
        } else {
            throw new UnsupportedOperationException("Unsupported protocol type " + endpoint.getScheme());
        }
    }

    @Override
    public Socket getSocket() {
        return socket;
    }

    @Override
    protected Factory getFactory() {
        return FACTORY;
    }

    @Override
    public ZMsg getMessage() {
        final OpenCmwProtocol.MdpMessage msg = OpenCmwProtocol.MdpMessage.receive(socket, false);
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

    private ZMsg handleRequest(final OpenCmwProtocol.MdpMessage msg) {
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

    private ZMsg createInternalMsg(final byte[] reqId, final URI endpoint, final ZFrame body, final String exception) {
        final ZMsg result = new ZMsg();
        result.add(reqId);
        result.add(endpoint.toString());
        result.add(body == null ? new ZFrame(new byte[0]) : body);
        result.add(exception == null ? new ZFrame(new byte[0]) : new ZFrame(exception));
        return result;
    }

    @Override
    public long housekeeping() {
        return System.currentTimeMillis() + 1000;
    }

    @Override
    public void subscribe(final String reqId, final URI endpoint, final byte[] rbacToken) {
        subscriptions.put(reqId, endpoint);
        final byte[] serviceId = endpoint.getPath().substring(1).getBytes(StandardCharsets.UTF_8);
        if (socket.getSocketType() == SocketType.DEALER) { // mpd
            // only tcp fallback?
            final boolean sent = new OpenCmwProtocol.MdpMessage(null,
                                                            PROT_CLIENT,
                                                            SUBSCRIBE,
                                                            serviceId,
                                                            reqId.getBytes(StandardCharsets.UTF_8),
                                                            endpoint,
                                                            EMPTY_FRAME,
                                                            "",
                                                            rbacToken)
                                         .send(socket);
            if (!sent) {
                LOGGER.atError().addArgument(reqId).addArgument(endpoint).log("subscription error (reqId: {}) for endpoint: {}");
            }
        } else { // mds
            final String id = endpoint.getPath().substring(1) + '?' + endpoint.getQuery();
            socket.subscribe(id.getBytes(StandardCharsets.UTF_8));
        }
    }

    @Override
    public void unsubscribe(final String reqId) {
        final URI subscriptionEndpoint = subscriptions.remove(reqId);
        final byte[] serviceId = subscriptionEndpoint.getPath().substring(1).getBytes(StandardCharsets.UTF_8);
        if (socket.getSocketType() == SocketType.DEALER) { // mdp
            final boolean sent = new OpenCmwProtocol.MdpMessage(clientId.getBytes(StandardCharsets.UTF_8),
                                                            PROT_CLIENT,
                                                            UNSUBSCRIBE,
                                                            serviceId,
                                                            reqId.getBytes(StandardCharsets.UTF_8),
                                                            endpoint,
                                                            EMPTY_FRAME,
                                                            "",
                                                            null)
                                         .send(socket);
            if (!sent) {
                LOGGER.atError().addArgument(reqId).addArgument(endpoint).log("unsubscribe error (reqId: {}) for endpoint: {}");
            }
        } else { // mds
            socket.unsubscribe(serviceId);
        }
    }

    @Override
    public void get(final String requestId, final URI endpoint, final byte[] data, final byte[] rbacToken) {
        // todo: filters which are not in endpoint
        final byte[] serviceId = endpoint.getPath().substring(1).getBytes(StandardCharsets.UTF_8);
        final boolean sent = new OpenCmwProtocol.MdpMessage(null,
                                                        PROT_CLIENT,
                                                        GET_REQUEST,
                                                        serviceId,
                                                        requestId.getBytes(StandardCharsets.UTF_8),
                                                        endpoint,
                                                        EMPTY_FRAME,
                                                        "",
                                                        rbacToken)
                                     .send(socket);
        if (!sent) {
            LOGGER.atError().addArgument(requestId).addArgument(endpoint).log("get error (reqId: {}) for endpoint: {}");
        }
    }

    @Override
    public void set(final String requestId, final URI endpoint, final byte[] data, final byte[] rbacToken) {
        // todo: filters which are not in endpoint
        final byte[] serviceId = endpoint.getPath().substring(1).getBytes(StandardCharsets.UTF_8);
        final boolean sent = new OpenCmwProtocol.MdpMessage(null,
                                                        PROT_CLIENT,
                                                        SET_REQUEST,
                                                        serviceId,
                                                        requestId.getBytes(StandardCharsets.UTF_8),
                                                        endpoint,
                                                        data,
                                                        "",
                                                        rbacToken)
                                     .send(socket);
        if (!sent) {
            LOGGER.atError().addArgument(requestId).addArgument(endpoint).log("set error (reqId: {}) for endpoint: {}");
        }
    }
}
