package io.opencmw.server;

import static io.opencmw.OpenCmwProtocol.*;
import static io.opencmw.OpenCmwProtocol.Command.*;
import static io.opencmw.OpenCmwProtocol.MdpMessage.receive;
import static io.opencmw.OpenCmwProtocol.MdpSubProtocol.PROT_WORKER;
import static io.opencmw.utils.AnsiDefs.ANSI_RED;
import static io.opencmw.utils.AnsiDefs.ANSI_RESET;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import javax.validation.constraints.NotNull;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import io.opencmw.rbac.RbacRole;
import io.opencmw.serialiser.annotations.MetaInfo;
import io.opencmw.utils.NoDuplicatesList;
import io.opencmw.utils.SystemProperties;

/**
 * Majordomo Protocol Client API, Java version Implements the OpenCmwProtocol/Worker spec at
 * http://rfc.zeromq.org/spec:7.
 *
 * <p>
 * The worder is controlled by the following environment variables (see also MajordomoBroker definitions):
 * <ul>
 * <li> 'OpenCMW.heartBeat' [ms]: default (2500 ms) heart-beat time-out [ms]</li>
 * <li> 'OpenCMW.heartBeatLiveness' []: default (3) heart-beat liveness - 3-5 is reasonable
 * <small>N.B. heartbeat expires when last heartbeat message is more than HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS ms ago.
 * this implies also, that worker must either return their message within 'HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS ms'
 * or decouple their secondary handler interface into another thread.</small></li>
 * </ul>
 *
 */
@MetaInfo(description = "default BasicMdpWorker implementation")
@SuppressWarnings({ "PMD.GodClass", "PMD.ExcessiveImports", "PMD.TooManyStaticImports", "PMD.DoNotUseThreads", "PMD.TooManyFields", "PMD.TooManyMethods" }) // makes the code more readable/shorter lines
public class BasicMdpWorker extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(BasicMdpWorker.class);
    protected static final byte[] RBAC = {}; //TODO: implement RBAC between Majordomo and Worker
    protected static final String WILDCARD = "*";
    protected static final int HEARTBEAT_LIVENESS = SystemProperties.getValueIgnoreCase("OpenCMW.heartBeatLiveness", 3); // [counts] 3-5 is reasonable
    protected static final int HEARTBEAT_INTERVAL = SystemProperties.getValueIgnoreCase("OpenCMW.heartBeat", 2500); // [ms]
    protected static final AtomicInteger WORKER_COUNTER = new AtomicInteger();

    // ---------------------------------------------------------------------
    protected final String uniqueID;
    protected final transient ZContext ctx;
    protected final String brokerAddress;
    protected final String serviceName;
    protected final byte[] serviceBytes;

    protected final AtomicBoolean runSocketHandlerLoop = new AtomicBoolean(true);
    protected final SortedSet<RbacRole> rbacRoles; // NOSONAR NOPMD
    protected transient final ZMQ.Socket notifySocket; // Socket to listener -- needed for thread-decoupling
    protected transient final ZMQ.Socket notifyListenerSocket; // Socket to notifier -- needed for thread-decoupling
    protected final ConcurrentHashMap<URI, List<byte[]>> activeSubscriptions = new ConcurrentHashMap<>(); // <URI, subscriber list>, '*' being wildcard
    protected transient ZMQ.Socket workerSocket; // Socket to broker
    protected final boolean isExternal; // used to skip heart-beating and disconnect checks
    protected long heartbeatAt; // When to send HEARTBEAT
    protected int liveness; // How many attempts left
    protected long reconnect = 2500L; // Reconnect delay, msecs
    protected RequestHandler requestHandler;
    protected transient ZMQ.Poller poller;

    public BasicMdpWorker(String brokerAddress, String serviceName, final RbacRole<?>... rbacRoles) {
        this(null, brokerAddress, serviceName, rbacRoles);
    }

    public BasicMdpWorker(ZContext ctx, String serviceName, final RbacRole<?>... rbacRoles) {
        this(ctx, "inproc://broker", serviceName, rbacRoles);
    }

    protected BasicMdpWorker(ZContext ctx, String brokerAddress, String serviceName, final RbacRole<?>... rbacRoles) {
        super();
        assert (brokerAddress != null);
        assert (serviceName != null);
        this.brokerAddress = brokerAddress;
        this.serviceName = StringUtils.stripStart(serviceName, "/");
        this.serviceBytes = this.serviceName.getBytes(StandardCharsets.UTF_8);
        this.isExternal = !brokerAddress.toLowerCase(Locale.UK).contains("inproc://");

        // initialise RBAC role-based priority queues
        this.rbacRoles = Collections.unmodifiableSortedSet(new TreeSet<>(Set.of(rbacRoles)));

        this.ctx = Objects.requireNonNullElseGet(ctx, ZContext::new);
        if (ctx != null) {
            this.setDaemon(true);
        }
        this.setName(BasicMdpWorker.class.getSimpleName() + "#" + WORKER_COUNTER.getAndIncrement());
        this.uniqueID = this.serviceName + "-PID=" + ManagementFactory.getRuntimeMXBean().getName() + "-TID=" + this.getId();
        this.setName(this.getClass().getSimpleName() + "-" + uniqueID);

        notifyListenerSocket = this.ctx.createSocket(SocketType.PAIR);
        notifyListenerSocket.bind("inproc://notifyListener" + uniqueID);
        notifyListenerSocket.setHWM(0);
        notifySocket = this.ctx.createSocket(SocketType.PAIR);
        notifySocket.connect("inproc://notifyListener" + uniqueID);
        notifySocket.setHWM(0);

        LOGGER.atTrace().addArgument(serviceName).addArgument(uniqueID).log("created new service '{}' worker - uniqueID: {}");
    }

    public ConcurrentMap<URI, List<byte[]>> getActiveSubscriptions() {
        return activeSubscriptions;
    }

    public int getHeartbeat() {
        return HEARTBEAT_INTERVAL;
    }

    public SortedSet<RbacRole> getRbacRoles() { // NOSONAR NOPMD
        return rbacRoles;
    }

    public Duration getReconnectDelay() {
        return Duration.ofMillis(reconnect);
    }

    public void setReconnectDelay(final int reconnect, @NotNull final TimeUnit timeUnit) {
        this.reconnect = timeUnit.toMillis(reconnect);
    }

    public RequestHandler getRequestHandler() {
        return requestHandler;
    }

    public String getServiceName() {
        return serviceName;
    }

    public String getUniqueID() {
        return uniqueID;
    }

    protected List<MdpMessage> handleRequestsFromBroker(final MdpMessage request) {
        if (request == null) {
            return Collections.emptyList();
        }
        if (LOGGER.isTraceEnabled()) {
            LOGGER.atTrace().addArgument(uniqueID).addArgument(request).log("worker '{}' received request: '{}'");
        }
        final byte[] clientID = request.serviceNameBytes;

        switch (request.command) {
        case GET_REQUEST:
        case SET_REQUEST:
        case W_NOTIFY:
        case PARTIAL:
        case FINAL:
            return processRequest(request);
        case W_HEARTBEAT:
            // Do nothing for heartbeats
            return Collections.emptyList();
        case DISCONNECT:
            // TODO: check whether to reconnect or to connect permanently
            reconnectToBroker();
            return Collections.emptyList();
        case UNKNOWN:
            // N.B. not too verbose logging since we do not want that sloppy clients
            // can bring down the broker through warning or info messages
            if (LOGGER.isDebugEnabled()) {
                LOGGER.atDebug().addArgument(uniqueID).addArgument(request).log("worker '{}' received invalid message: '{}'");
            }
            return Collections.emptyList();
        case SUBSCRIBE:
            subscribe(request.topic, clientID);
            return Collections.emptyList();
        case UNSUBSCRIBE:
            unsubscribe(request.topic, clientID);
            return Collections.emptyList();

        case READY:
        default:
        }
        throw new IllegalStateException("should not reach here - request message = " + request);
    }

    /**
   * Sends pre-defined message to subscriber (provided there is any that matches the published topic)
   * @param notifyMessage the message that is supposed to be broadcast
   */
    public void notify(@NotNull final MdpMessage notifyMessage) {
        // send only if there are matching topics and duplicate messages based on topics as necessary
        final HashMap<URI, List<byte[]>> subTopics = new HashMap<>(activeSubscriptions);
        final URI originalTopic = notifyMessage.topic;
        subTopics.forEach((uri, subscribers) -> {
            if (subscribers.isEmpty()) {
                return;
            }
            final String path = uri.getPath();
            for (byte[] clientID : subscribers) {
                if (path.isBlank() || WILDCARD.equals(path) || serviceName.matches(path)) {
                    notifyMessage.senderID = EMPTY_FRAME;
                    notifyMessage.protocol = PROT_WORKER;
                    notifyMessage.command = W_NOTIFY;
                    notifyMessage.serviceNameBytes = clientID;
                    notifyMessage.clientRequestID = EMPTY_FRAME;
                    notifyMessage.topic = originalTopic;
                    notifyRaw(notifyMessage);
                }
            }
        });
    }

    protected List<MdpMessage> processRequest(final MdpMessage request) {
        if (requestHandler == null) {
            return Collections.emptyList();
        }
        // de-serialise byte[] -> PropertyMap() (+ getObject(Class<?>))
        try {
            final Context mdpCtx = new Context(request);
            requestHandler.handle(mdpCtx);
            return mdpCtx.rep == null ? Collections.emptyList() : List.of(mdpCtx.rep);
        } catch (Throwable e) { // NOPMD on purpose since we want to catch exceptions and courteously return this to the user
            final StringWriter sw = new StringWriter();
            final PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            final String exceptionMsg = ANSI_RED + getClass().getName() + " caught exception for service '" + getServiceName()
                                        + "'\nrequest msg: " + request + "\nexception: " + sw.toString() + ANSI_RESET;
            LOGGER.atError().addArgument(exceptionMsg).log("could not processRequest(MdpMessage) - exception thrown:\n{}");
            return List.of(new MdpMessage(request.senderID, request.protocol, FINAL, request.serviceNameBytes, request.clientRequestID, request.topic, null, exceptionMsg, RBAC));
        }
    }

    public BasicMdpWorker registerHandler(final RequestHandler requestHandler) {
        this.requestHandler = requestHandler;
        return this;
    }

    /**
   * primary run loop
   * Send reply, if any, to broker and wait for next request.
   */
    @Override
    public void run() { // NOPMD -- single serial function .. easier to read
        reconnectToBroker();
        // Poll socket for a reply, with timeout and/or until the process is stopped or interrupted
        // N.B. poll(..) returns '-1' when thread is interrupted
        final MdpMessage heartbeatMsg = new MdpMessage(null, PROT_WORKER, W_HEARTBEAT, serviceBytes, EMPTY_FRAME, EMPTY_URI, EMPTY_FRAME, "", RBAC);
        while (runSocketHandlerLoop.get() && !Thread.currentThread().isInterrupted() && poller.poll(HEARTBEAT_INTERVAL) != -1) {
            boolean dataReceived = true;
            while (dataReceived) {
                dataReceived = false;
                // handle message from or to broker
                final MdpMessage brokerMsg = receive(workerSocket, false);
                if (brokerMsg != null) {
                    dataReceived = true;
                    liveness = HEARTBEAT_LIVENESS;
                    MdpMessage.send(workerSocket, handleRequestsFromBroker(brokerMsg));
                }

                // handle message from or to notify thread
                final MdpMessage notifyMsg = receive(notifyListenerSocket, false);
                if (notifyMsg != null) {
                    dataReceived = true;
                    // forward notify message to MDP broker
                    notifyMsg.send(workerSocket);
                }
            }

            if (--liveness == 0) {
                LOGGER.atDebug().addArgument(uniqueID).log("worker '{}' disconnected from broker - retrying");
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(reconnect));
                reconnectToBroker();
            }

            // Send HEARTBEAT if it's time
            if (System.currentTimeMillis() > heartbeatAt) {
                heartbeatMsg.send(workerSocket);
                heartbeatAt = System.currentTimeMillis() + HEARTBEAT_INTERVAL;
            }
        }
        if (Thread.currentThread().isInterrupted()) {
            LOGGER.atWarn().addArgument(uniqueID).log("worker '{}' interrupt received, killing worker");
        }
        if (isExternal) {
            ctx.destroy();
        }
    }

    @Override
    public synchronized void start() { // NOPMD 'synchronized' comes from JDK class definition
        runSocketHandlerLoop.set(true);
        super.start();
    }

    public void stopWorker() {
        runSocketHandlerLoop.set(false);
    }

    public void subscribe(final URI topic, final byte[]... clientName) {
        if (clientName == null) {
            throw new IllegalArgumentException("varargs clientName must not be null");
        }
        final List<byte[]> list = activeSubscriptions.computeIfAbsent(topic, sub -> Collections.synchronizedList(new NoDuplicatesList<>()));
        if (clientName.length > 0) {
            list.add(clientName[0]);
        }
    }

    public void unsubscribe(final URI topic, final byte[]... clientName) {
        if (clientName == null) {
            throw new IllegalArgumentException("varargs clientName must not be null");
        }

        final List<byte[]> list = activeSubscriptions.computeIfAbsent(topic, sub -> Collections.synchronizedList(new NoDuplicatesList<>()));
        if (clientName.length > 0) {
            list.remove(clientName[0]);
        }
        if (list.isEmpty()) {
            activeSubscriptions.remove(topic);
        }
    }

    protected void notifyRaw(@NotNull final MdpMessage notifyMessage) {
        assert notifyMessage != null : "notify message must not be null";
        notifyMessage.send(notifySocket);
    }

    /**
   * Connect or reconnect to broker
   */
    protected void reconnectToBroker() {
        if (workerSocket != null) {
            workerSocket.close();
        }
        workerSocket = ctx.createSocket(SocketType.DEALER);
        assert workerSocket != null : "worker socket is null";
        workerSocket.setHWM(0);
        workerSocket.connect(brokerAddress);

        // Register service with broker
        LOGGER.atInfo().addArgument(brokerAddress).log("register service with broker '{}");
        final byte[] classNameByte = this.getClass().getName().getBytes(StandardCharsets.UTF_8); // used for OpenAPI purposes
        new MdpMessage(null, PROT_WORKER, READY, serviceBytes, EMPTY_FRAME, URI.create(serviceName), classNameByte, "", RBAC).send(workerSocket);

        if (poller != null) {
            poller.unregister(workerSocket);
            poller.close();
        }
        poller = ctx.createPoller(2);
        poller.register(workerSocket, ZMQ.Poller.POLLIN);
        poller.register(notifyListenerSocket, ZMQ.Poller.POLLIN);

        // If liveness hits zero, queue is considered disconnected
        liveness = HEARTBEAT_LIVENESS;
        heartbeatAt = System.currentTimeMillis() + HEARTBEAT_INTERVAL;
    }

    public interface RequestHandler {
        void handle(Context ctx) throws Throwable; // NOPMD NOSONAR - should allow to throw any/generic exceptions
    }
}