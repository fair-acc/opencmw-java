package io.opencmw.server;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.zeromq.ZMQ.Socket;
import static org.zeromq.util.ZData.strhex;

import static io.opencmw.OpenCmwConstants.*;
import static io.opencmw.OpenCmwProtocol.BROKER_SHUTDOWN;
import static io.opencmw.OpenCmwProtocol.Command.*;
import static io.opencmw.OpenCmwProtocol.EMPTY_FRAME;
import static io.opencmw.OpenCmwProtocol.EMPTY_URI;
import static io.opencmw.OpenCmwProtocol.MdpMessage;
import static io.opencmw.OpenCmwProtocol.MdpMessage.receive;
import static io.opencmw.OpenCmwProtocol.MdpSubProtocol.PROT_CLIENT;
import static io.opencmw.OpenCmwProtocol.MdpSubProtocol.PROT_WORKER;
import static io.opencmw.server.MmiServiceHelper.*;

import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import org.zeromq.util.ZData;

import io.opencmw.filter.SubscriptionMatcher;
import io.opencmw.rbac.BasicRbacRole;
import io.opencmw.rbac.RbacRole;
import io.opencmw.rbac.RbacToken;
import io.opencmw.utils.NoDuplicatesList;
import io.opencmw.utils.SystemProperties;

/**
 * Majordomo Protocol broker -- a minimal implementation of http://rfc.zeromq.org/spec:7 and spec:8 and following the OpenCMW specification
 *
 * <p>
 * The broker is controlled by the following environment variables:
 * <ul>
 * <li> 'OpenCMW.heartBeat' [ms]: default (2500 ms) heart-beat time-out [ms]</li>
 * <li> 'OpenCMW.heartBeatLiveness' []: default (3) heart-beat liveness - 3-5 is reasonable
 * <small>N.B. heartbeat expires when last heartbeat message is more than HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS ms ago.
 * this implies also, that worker must either return their message within 'HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS ms'
 * or decouple their secondary handler interface into another thread.</small></li>
 *
 * <li>'OpenCMW.clientTimeOut' [s]: default (3600, i.e. 1h) time-out after which unanswered client messages/infos are being deleted</li>
 * <li>'OpenCMW.nIoThreads' []: default (2) IO threads dedicated to network IO (ZeroMQ recommendation 1 thread per 1 GBit/s)</li>
 * <li>'OpenCMW.dnsTimeOut' [s]: default (60) DNS time-out after which an unresponsive client is dropped from the DNS table
 * <small>N.B. if registered, a HEARTBEAT challenge will be send that needs to be replied with a READY command/re-registering</small></li>
 * </ul>
 * @see io.opencmw.OpenCmwConstants for more details
 */
@SuppressWarnings({ "PMD.GodClass", "PMD.DefaultPackage", "PMD.UseConcurrentHashMap", "PMD.TooManyFields", "PMD.TooManyMethods", "PMD.ExcessiveImports", "PMD.CommentSize", "PMD.UseConcurrentHashMap" })
// package private explicitly needed for MmiServiceHelper, thread-safe/performance use of HashMap
public class MajordomoBroker extends Thread implements AutoCloseable {
    public static final byte[] RBAC = {}; // TODO: implement RBAC between Majordomo and Worker
    private static final Logger LOGGER = LoggerFactory.getLogger(MajordomoBroker.class);
    // ----------------- default service names -----------------------------
    public static final String SUFFIX_ROUTER = "/router";
    public static final String SUFFIX_PUBLISHER = "/publisher";
    public static final String SUFFIX_SUBSCRIBE = "/subscribe";
    public static final String INPROC_BROKER = "inproc://broker";
    public static final String INTERNAL_ADDRESS_BROKER = INPROC_BROKER + SUFFIX_ROUTER;
    public static final String INTERNAL_ADDRESS_PUBLISHER = INPROC_BROKER + SUFFIX_PUBLISHER;
    public static final String INTERNAL_ADDRESS_SUBSCRIBE = INPROC_BROKER + SUFFIX_SUBSCRIBE;
    private static final AtomicInteger BROKER_COUNTER = new AtomicInteger();
    // ---------------------------------------------------------------------
    protected final ZContext ctx;
    protected final Socket routerSocket;
    protected final Socket pubSocket;
    protected final Socket subSocket;
    protected final Socket dnsSocket;
    protected final String brokerName;
    protected final URI dnsAddress;
    protected final List<String> routerSockets = new NoDuplicatesList<>(); // Sockets for clients & public external workers
    protected final SortedSet<RbacRole<?>> rbacRoles;
    /* default */ final Map<String, Service> services = new HashMap<>(); // known services Map<'service name', Service>
    protected final Map<String, Worker> workers = new HashMap<>(); // known workers Map<addressHex, Worker
    protected final Map<String, Client> clients = new HashMap<>();
    protected final BiPredicate<URI, URI> subscriptionMatcher = new SubscriptionMatcher(); // <notify topic, subscribe topic>
    protected final Map<URI, AtomicInteger> activeSubscriptions = new HashMap<>(); // Map<ServiceName,List<SubscriptionTopic>>
    protected final Map<URI, List<byte[]>> routerBasedSubscriptions = new HashMap<>(); // Map<ServiceName,List<SubscriptionTopic>>
    private final AtomicBoolean run = new AtomicBoolean(false); // NOPMD - nomen est omen
    private final AtomicBoolean running = new AtomicBoolean(false); // NOPMD - nomen est omen
    protected final Deque<Worker> waiting = new ArrayDeque<>(); // idle workers
    protected final int heartBeatLiveness = SystemProperties.getValueIgnoreCase(HEARTBEAT_LIVENESS, HEARTBEAT_LIVENESS_DEFAULT); // [counts] 3-5 is reasonable
    protected final long heartBeatInterval = SystemProperties.getValueIgnoreCase(HEARTBEAT, HEARTBEAT_DEFAULT); // [ms]
    protected final long heartBeatExpiry = heartBeatInterval * heartBeatLiveness;
    private final Map<String, DnsServiceItem> dnsCache = new ConcurrentHashMap<>(); // <server name, DnsServiceItem>
    protected final long clientTimeOut = TimeUnit.SECONDS.toMillis(SystemProperties.getValueIgnoreCase(CLIENT_TIMEOUT, CLIENT_TIMEOUT_DEFAULT)); // [s]
    protected final long dnsTimeOut = TimeUnit.SECONDS.toMillis(SystemProperties.getValueIgnoreCase("OpenCMW.dnsTimeOut", 10)); // [ms] time when
    protected long heartbeatAt = System.currentTimeMillis() + heartBeatInterval; // When to send HEARTBEAT
    protected long dnsHeartbeatAt = System.currentTimeMillis() + dnsTimeOut; // When to send a DNS HEARTBEAT

    /**
     * Initialize broker state.
     *
     * @param brokerName specific Majordomo Broker name this instance is known for in the world
     * @param dnsAddress specific of other Majordomo broker that acts as primary DNS
     * @param rbacRoles  RBAC-based roles (used for IO prioritisation and service access control
     */
    public MajordomoBroker(@NotNull final String brokerName, final URI dnsAddress, final RbacRole<?>... rbacRoles) {
        super();
        this.brokerName = brokerName;
        this.dnsAddress = dnsAddress == null || dnsAddress.toString().isBlank() ? null : replaceScheme(dnsAddress, SCHEME_TCP); // NOPMD - uninitialised/unknown dns defaults to null
        this.setName(MajordomoBroker.class.getSimpleName() + "(" + brokerName + ")#" + BROKER_COUNTER.getAndIncrement());

        ctx = new ZContext(SystemProperties.getValueIgnoreCase(N_IO_THREADS, N_IO_THREADS_DEFAULT));

        // initialise RBAC role-based priority queues
        this.rbacRoles = Collections.unmodifiableSortedSet(new TreeSet<>(Set.of(rbacRoles)));

        // generate and register internal default inproc socket
        routerSocket = ctx.createSocket(SocketType.ROUTER);
        routerSocket.setHWM(SystemProperties.getValueIgnoreCase(HIGH_WATER_MARK, HIGH_WATER_MARK_DEFAULT));
        routerSocket.bind(INTERNAL_ADDRESS_BROKER); // NOPMD
        pubSocket = ctx.createSocket(SocketType.XPUB);
        pubSocket.setHWM(SystemProperties.getValueIgnoreCase(HIGH_WATER_MARK, HIGH_WATER_MARK_DEFAULT));
        pubSocket.setXpubVerbose(true);
        pubSocket.bind(INTERNAL_ADDRESS_PUBLISHER); // NOPMD
        subSocket = ctx.createSocket(SocketType.SUB);
        subSocket.setHWM(SystemProperties.getValueIgnoreCase(HIGH_WATER_MARK, HIGH_WATER_MARK_DEFAULT));
        subSocket.bind(INTERNAL_ADDRESS_SUBSCRIBE); // NOPMD

        registerDefaultServices(rbacRoles); // NOPMD

        dnsSocket = ctx.createSocket(SocketType.DEALER);
        dnsSocket.setHWM(SystemProperties.getValueIgnoreCase(HIGH_WATER_MARK, HIGH_WATER_MARK_DEFAULT));
        if (this.dnsAddress == null) {
            dnsSocket.connect(INTERNAL_ADDRESS_BROKER);
        } else {
            dnsSocket.connect(this.dnsAddress.toString());
        }

        LOGGER.atInfo().addArgument(getName()).addArgument(this.dnsAddress).log("register new '{}' broker with DNS: '{}'");
    }

    /**
     * Add internal service.
     *
     * @param worker the worker
     */
    public void addInternalService(final BasicMdpWorker worker) {
        assert worker != null : "worker must not be null";
        requireService(worker.getServiceName(), worker);
    }

    /**
     * Bind broker to endpoint, can call this multiple times. We use a single
     * socket for both clients and workers.
     * <p>
     *
     * @param endpoint the URI-based 'scheme://ip:port' endpoint definition the server should listen to <p> The protocol definition <ul> <li>'mdp://' corresponds to a SocketType.ROUTER socket</li> <li>'mds://' corresponds to a SocketType.XPUB socket</li> <li>'tcp://' internally falls back to 'mdp://' and ROUTER socket</li> </ul>
     * @return the string
     */
    public URI bind(URI endpoint) {
        final String requestedScheme = Objects.requireNonNull(endpoint, "endpoint is null").getScheme().toLowerCase(Locale.UK);
        final boolean isRouterSocket = requestedScheme.startsWith(SCHEME_MDP) || requestedScheme.startsWith(SCHEME_TCP);
        (isRouterSocket ? routerSocket : pubSocket).bind(replaceScheme(endpoint, SCHEME_TCP).toString());
        final URI endpointAdjusted = replaceScheme(endpoint, isRouterSocket ? SCHEME_MDP : SCHEME_MDS);
        final URI adjustedAddressPublic = resolveHost(endpointAdjusted, getLocalHostName());
        routerSockets.add(adjustedAddressPublic.toString());
        LOGGER.atDebug().addArgument(adjustedAddressPublic).log("Majordomo broker/0.1 is active at '{}'");
        return adjustedAddressPublic;
    }

    public ZContext getContext() {
        return ctx;
    }

    public Socket getInternalRouterSocket() {
        return routerSocket;
    }

    /**
     * Gets router sockets.
     *
     * @return unmodifiable list of registered external sockets
     */
    public List<String> getRouterSockets() {
        return Collections.unmodifiableList(routerSockets);
    }

    public Collection<Service> getServices() {
        return services.values();
    }

    /**
     *
     * @return Map containing known brokers as keys and service items
     */
    public Map<String, DnsServiceItem> getDnsCache() {
        return dnsCache;
    }

    public boolean isRunning() {
        return running.get();
    }

    public void removeService(final String serviceName) {
        final Service ret = services.remove(serviceName);
        ret.mdpWorker.forEach(BasicMdpWorker::stopWorker);
        ret.waiting.forEach(worker -> new MdpMessage(worker.address, PROT_WORKER, DISCONNECT, worker.service.nameBytes, EMPTY_FRAME, URI.create(worker.service.name), BROKER_SHUTDOWN, "", RBAC).send(worker.socket));
    }

    /**
     * main broker work happens here
     */
    @Override
    public void run() {
        run.set(true);
        running.set(true);
        services.forEach((serviceName, service) -> service.internalWorkers.forEach(Thread::start));
        sendDnsHeartbeats(true); // initial register of default routes
        try (ZMQ.Poller items = ctx.createPoller(4)) { // 4 -> four sockets defined below
            items.register(routerSocket, ZMQ.Poller.POLLIN);
            items.register(dnsSocket, ZMQ.Poller.POLLIN);
            items.register(pubSocket, ZMQ.Poller.POLLIN);
            items.register(subSocket, ZMQ.Poller.POLLIN);
            while (run.get() && !Thread.currentThread().isInterrupted() && !ctx.isClosed() && items.poll(heartBeatInterval) != -1) {
                if (ctx.isClosed()) {
                    break;
                }
                int loopCount = 0;
                boolean receivedMsg = true;
                while (run.get() && !Thread.currentThread().isInterrupted() && receivedMsg) {
                    final MdpMessage routerMsg = receive(routerSocket, false);
                    receivedMsg = handleReceivedMessage(routerSocket, routerMsg);

                    final MdpMessage subMsg = receive(subSocket, false);
                    receivedMsg |= handleReceivedMessage(subSocket, subMsg);

                    final MdpMessage dnsMsg = receive(dnsSocket, false);
                    receivedMsg |= handleReceivedMessage(dnsSocket, dnsMsg);

                    final ZMsg pubMsg = ZMsg.recvMsg(pubSocket, false);
                    receivedMsg |= handleSubscriptionMsg(pubMsg);

                    processClients();
                    if (loopCount % 10 == 0) {
                        // perform maintenance tasks during the first and every tenth
                        // iteration
                        purgeWorkers();
                        purgeClients();
                        purgeDnsServices();
                        sendHeartbeats();
                        sendDnsHeartbeats(false);
                    }
                    loopCount++;
                }
            }
        }
        Worker[] deleteList = workers.values().toArray(new Worker[0]);
        for (Worker worker : deleteList) {
            deleteWorker(worker, true);
        }

        // wait for workers to shut-down
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
        running.set(false);
    }

    /**
     * Stop broker.
     */
    public void stopBroker() {
        run.set(false);
        if (running.get()) {
            try {
                this.join(heartBeatInterval);
            } catch (InterruptedException e) { // NOPMD NOSONAR -- re-throwing with different type
                throw new IllegalStateException(this.getName() + " did not shut down in " + heartBeatInterval + " ms", e);
            }
        }
        close();
    }

    /**
     * Deletes worker from all data structures, and destroys worker.
     *
     * @param worker     internal reference to worker
     * @param disconnect true: send a disconnect message to worker
     */
    protected void deleteWorker(Worker worker, boolean disconnect) {
        assert (worker != null);
        if (disconnect && !ctx.isClosed()) {
            final MdpMessage disconnectMsg = new MdpMessage(worker.address, PROT_WORKER, DISCONNECT, worker.serviceName, EMPTY_FRAME, URI.create(new String(worker.serviceName, UTF_8)), BROKER_SHUTDOWN, "", RBAC);
            disconnectMsg.send(worker.socket);
        }
        if (worker.service != null) {
            worker.service.waiting.remove(worker);
        }
        workers.remove(worker.addressHex);
    }

    /**
     * Disconnect all workers, destroy context.
     */
    @Override
    public void close() {
        routerSocket.close();
        pubSocket.close();
        subSocket.close();
        dnsSocket.close();
        ctx.close();
    }

    /**
     * Dispatch requests to waiting workers as possible
     *
     * @param service dispatch message for this service
     */
    protected void dispatch(Service service) {
        assert (service != null);
        purgeWorkers();
        while (!service.waiting.isEmpty() && service.requestsPending()) {
            final MdpMessage msg = service.getNextPrioritisedMessage();
            if (msg == null) {
                // should be thrown only with VM '-ea' enabled -- assert noisily since
                // this a (rare|design) library error
                assert false : "getNextPrioritisedMessage should not be null";
                continue;
            }
            Worker worker = service.waiting.pop();
            waiting.remove(worker);
            msg.serviceNameBytes = msg.senderID;
            msg.senderID = worker.address; // replace sourceID with worker destinationID
            msg.protocol = PROT_WORKER; // CLIENT protocol -> WORKER -> protocol
            msg.send(worker.socket);
        }
    }

    /**
     * Handle received message boolean.
     *
     * @param receiveSocket the receive socket
     * @param msg           the to be processed msg
     * @return true if request was implemented and has been processed
     */
    protected boolean handleReceivedMessage(final Socket receiveSocket, final MdpMessage msg) {
        if (msg == null) {
            return false;
        }
        final String topic = msg.topic.toString();
        switch (msg.protocol) {
        case PROT_CLIENT:
        case PROT_CLIENT_HTTP:
            // Set reply return address to client sender
            switch (msg.command) {
            case READY:
                if (msg.topic.getScheme() != null) {
                    // register potentially new service
                    registerNewService(msg.getServiceName());
                    DnsServiceItem ret = dnsCache.computeIfAbsent(msg.getServiceName(), s -> new DnsServiceItem(msg.senderID, msg.getServiceName()));
                    ret.uri.add(msg.topic);
                    ret.updateExpiryTimeStamp();
                }
                return true;
            case SUBSCRIBE:
                if (activeSubscriptions.computeIfAbsent(msg.topic, s -> new AtomicInteger()).incrementAndGet() == 1) {
                    subSocket.subscribe(topic);
                }
                routerBasedSubscriptions.computeIfAbsent(msg.topic, s -> new ArrayList<>()).add(msg.senderID);
                return true;
            case UNSUBSCRIBE:
                if (activeSubscriptions.computeIfAbsent(msg.topic, s -> new AtomicInteger()).decrementAndGet() <= 0) {
                    subSocket.unsubscribe(topic);
                }
                routerBasedSubscriptions.computeIfAbsent(msg.topic, s -> new ArrayList<>()).remove(msg.senderID);
                if (Objects.requireNonNullElse(routerBasedSubscriptions.get(msg.topic), "").toString().isEmpty()) {
                    routerBasedSubscriptions.remove(msg.topic);
                }
                return true;
            case W_HEARTBEAT:
                sendDnsHeartbeats(true);
                return true;
            default:
            }

            final String senderName = msg.getSenderName();
            final Client client = clients.computeIfAbsent(senderName, s -> new Client(receiveSocket, senderName, msg.senderID));
            client.offerToQueue(msg);
            return true;
        case PROT_WORKER:
            processWorker(receiveSocket, msg);
            return true;
        default:
            // N.B. not too verbose logging since we do not want that sloppy clients
            // can bring down the broker through warning or info messages
            if (LOGGER.isDebugEnabled()) {
                LOGGER.atDebug().addArgument(msg).log("Majordomo broker invalid message: '{}'");
            }
            return false;
        }
    }

    /**
     * Process a request coming from a client.
     */
    protected void processClients() {
        // round-robin
        clients.forEach((name, client) -> {
            final MdpMessage clientMessage = client.pop();
            if (clientMessage == null) {
                return;
            }

            // dispatch client message to worker queue
            // old : final Service service = services.get(clientMessage.getServiceName())
            final Service service = getBestMatchingService(StringUtils.strip(URI.create(clientMessage.getServiceName()).getPath(), "/"));
            if (service == null) {
                // not implemented -- reply according to Majordomo Management Interface (MMI) as defined in http://rfc.zeromq.org/spec:8
                final MdpMessage msg = new MdpMessage(clientMessage.senderID, PROT_CLIENT, FINAL, clientMessage.serviceNameBytes, clientMessage.clientRequestID, URI.create(INTERNAL_SERVICE_NAMES),
                        "501".getBytes(UTF_8), "unknown service (error 501): '" + clientMessage.getServiceName() + '\'', RBAC);
                msg.send(client.socket);
                return;
            }
            // queue new client message RBAC-priority-based
            service.putPrioritisedMessage(clientMessage);

            // dispatch service
            dispatch(service);
        });
    }

    /**
     * Process message sent to us by a worker.
     *
     * @param receiveSocket the socket the message was received at
     * @param msg           the received and to be processed message
     */
    protected void processWorker(final Socket receiveSocket, final MdpMessage msg) { //NOPMD
        final String senderIdHex = strhex(msg.senderID);
        final String serviceName = msg.getServiceName();
        final boolean workerReady = workers.containsKey(senderIdHex);
        final Worker worker = requireWorker(receiveSocket, msg.senderID, senderIdHex, msg.serviceNameBytes);

        switch (msg.command) {
        case READY:
            LOGGER.atTrace().addArgument(serviceName).log("log new local/external worker for service {} - " + msg);
            // Attach worker to service and mark as idle
            worker.service = requireService(serviceName);
            workerWaiting(worker);
            worker.service.serviceDescription = Arrays.copyOf(msg.data, msg.data.length);

            if (!msg.topic.toString().isBlank() && msg.topic.getScheme() != null) {
                registerNewService(brokerName);
                routerSockets.add(msg.topic.toString());
                DnsServiceItem ret = dnsCache.computeIfAbsent(brokerName, s -> new DnsServiceItem(msg.senderID, brokerName));
                ret.uri.add(msg.topic);
            }

            // notify potential listener
            msg.data = msg.serviceNameBytes;
            msg.serviceNameBytes = INTERNAL_SERVICE_NAMES.getBytes(UTF_8);
            msg.command = W_NOTIFY;
            msg.clientRequestID = this.getName().getBytes(UTF_8);
            msg.topic = URI.create(INTERNAL_SERVICE_NAMES);
            msg.errors = "";
            if (!pubSocket.sendMore(INTERNAL_SERVICE_NAMES) || !msg.send(pubSocket)) {
                LOGGER.atWarn().addArgument(msg.getServiceName()).log("could not notify service change for '{}'");
            }
            break;
        case W_HEARTBEAT:
            if (workerReady) {
                worker.updateExpiryTimeStamp();
            } else {
                deleteWorker(worker, true);
            }
            break;
        case DISCONNECT:
            //deleteWorker(worker, false);
            break;
        case PARTIAL:
        case FINAL:
            if (workerReady) {
                final Client client = clients.get(msg.getServiceName());
                if (client == null || client.socket == null) {
                    break;
                }
                // need to replace clientID with service name
                final byte[] serviceID = worker.service.nameBytes;
                msg.senderID = msg.serviceNameBytes;
                msg.protocol = PROT_CLIENT;
                msg.serviceNameBytes = serviceID;
                msg.send(client.socket);
                workerWaiting(worker);
            } else {
                deleteWorker(worker, true);
            }
            break;
        case W_NOTIFY:
            // need to replace clientID with service name
            final byte[] serviceID = worker.service.nameBytes;
            msg.senderID = msg.serviceNameBytes;
            msg.serviceNameBytes = serviceID;
            msg.protocol = PROT_CLIENT;
            msg.command = FINAL;

            dispatchMessageToMatchingSubscriber(msg);

            break;
        default:
            // N.B. not too verbose logging since we do not want that sloppy clients
            // can bring down the broker through warning or info messages
            if (LOGGER.isDebugEnabled()) {
                LOGGER.atDebug().addArgument(msg).log("Majordomo broker invalid message: '{}'");
            }
            break;
        }
    }

    /**
     * Look for &amp; kill expired clients.
     */
    protected void purgeClients() {
        if (clientTimeOut <= 0) {
            return;
        }
        for (String clientName : clients.keySet()) { // NOSONAR NOPMD copy because
            // we are going to remove keys
            Client client = clients.get(clientName);
            if (client == null || client.expiry < System.currentTimeMillis()) {
                clients.remove(clientName);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.atDebug().addArgument(client).log("Majordomo broker deleting expired client: '{}'");
                }
            }
        }
    }

    /**
     * Look for &amp; kill expired workers. Workers are oldest to most recent, so
     * we stop at the first alive worker.
     */
    protected void purgeDnsServices() {
        if (System.currentTimeMillis() >= dnsHeartbeatAt) {
            List<DnsServiceItem> cachedList = new ArrayList<>(dnsCache.values());
            final MdpMessage challengeMessage = new MdpMessage(null, PROT_CLIENT, W_HEARTBEAT, EMPTY_FRAME, "dnsChallenge".getBytes(UTF_8), EMPTY_URI, EMPTY_FRAME, "", RBAC);
            for (DnsServiceItem registeredService : cachedList) {
                if (registeredService.serviceName.equalsIgnoreCase(brokerName)) {
                    registeredService.updateExpiryTimeStamp();
                }
                // challenge remote broker with a HEARTBEAT
                challengeMessage.senderID = registeredService.address;
                challengeMessage.serviceNameBytes = registeredService.serviceName.getBytes(UTF_8);
                challengeMessage.send(routerSocket); // NOPMD
                if (System.currentTimeMillis() > registeredService.expiry) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.atInfo().addArgument(registeredService).log("Majordomo broker deleting expired dns service: '{}'");
                    }
                    dnsCache.remove(registeredService.serviceName);
                }
            }
            dnsHeartbeatAt = System.currentTimeMillis() + dnsTimeOut;
        }
    }

    /**
     * Look for &amp; kill expired workers. Workers are oldest to most recent, so we stop at the first alive worker.
     */
    protected void purgeWorkers() {
        for (Worker w = waiting.peekFirst(); w != null && w.expiry < System.currentTimeMillis(); w = waiting.peekFirst()) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.atInfo().addArgument(w.addressHex).addArgument(w.service == null ? "(unknown)" : w.service.name).log("Majordomo broker deleting expired worker: '{}' - service: '{}'");
            }
            deleteWorker(waiting.pollFirst(), false);
        }
    }

    protected void registerDefaultServices(final RbacRole<?>[] rbacRoles) {
        // add simple internal Majordomo worker
        final int nServiceThreads = 3;

        addInternalService(new MmiService(this, rbacRoles));
        addInternalService(new MmiOpenApi(this, rbacRoles));
        addInternalService(new MmiDns(this, rbacRoles));
        for (int i = 0; i < nServiceThreads; i++) {
            addInternalService(new MmiEcho(this, rbacRoles)); // NOPMD valid instantiation inside loop
        }
    }

    /**
     * Locates the service (creates if necessary).
     *
     * @param serviceName service name
     * @param worker      optional worker implementation (may be null)
     * @return the existing (or new if absent) service this worker is responsible for
     */
    protected Service requireService(final String serviceName, final BasicMdpWorker... worker) {
        assert (serviceName != null);
        final BasicMdpWorker w = worker.length > 0 ? worker[0] : null;
        final Service service = services.computeIfAbsent(serviceName, s -> new Service(serviceName, serviceName.getBytes(UTF_8), w));
        if (w != null) {
            w.start();
        }
        return service;
    }

    /**
     * Finds the worker (creates if necessary).
     *
     * @param socket      the socket
     * @param address     the address
     * @param addressHex  the address hex
     * @param serviceName the service name
     * @return the worker
     */
    protected @NotNull Worker requireWorker(final Socket socket, final byte[] address, final String addressHex, final byte[] serviceName) {
        assert (addressHex != null);
        return workers.computeIfAbsent(addressHex, identity -> {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.atInfo().addArgument(addressHex).addArgument(ZData.toString(serviceName)).log("registering new worker: '{}' - '{}'");
            }
            return new Worker(socket, address, addressHex, serviceName);
        });
    }

    /**
     * Send heartbeats to the DNS server if necessary.
     *
     * @param force sending regardless of time-out
     */
    protected void sendDnsHeartbeats(boolean force) {
        // Send heartbeats to idle workers if it's time
        if (System.currentTimeMillis() >= dnsHeartbeatAt || force) {
            final MdpMessage readyMsg = new MdpMessage(null, PROT_CLIENT, READY, brokerName.getBytes(UTF_8), "clientID".getBytes(UTF_8), EMPTY_URI, EMPTY_FRAME, "", RBAC);
            final ArrayList<String> localCopy = new ArrayList<>(getRouterSockets());
            for (String routerAddress : localCopy) {
                readyMsg.topic = URI.create(routerAddress);
                registerWithDnsServices(readyMsg);
            }
            services.keySet().forEach(this::registerNewService);
        }
    }

    private void registerNewService(final String serviceName) {
        final MdpMessage readyMsg = new MdpMessage(null, PROT_CLIENT, READY, brokerName.getBytes(UTF_8), "clientID".getBytes(UTF_8), EMPTY_URI, EMPTY_FRAME, "", RBAC);
        final ArrayList<String> localCopy = new ArrayList<>(getRouterSockets());
        for (String routerAddress : localCopy) {
            readyMsg.topic = URI.create(routerAddress + '/' + serviceName);
            registerWithDnsServices(readyMsg);
        }
    }

    private void registerWithDnsServices(final MdpMessage readyMsg) {
        if (dnsAddress != null) {
            readyMsg.send(dnsSocket); // register with external DNS
        }
        // register with internal DNS
        DnsServiceItem ret = dnsCache.computeIfAbsent(brokerName, s -> new DnsServiceItem(readyMsg.senderID, brokerName)); // NOPMD instantiation in loop necessary
        ret.uri.add(readyMsg.topic);
        ret.updateExpiryTimeStamp();
    }

    /**
     * Send heartbeats to idle workers if it's time.
     */
    protected void sendHeartbeats() {
        // Send heartbeats to idle workers if it's time
        if (System.currentTimeMillis() >= heartbeatAt) {
            final MdpMessage heartbeatMsg = new MdpMessage(null, PROT_WORKER, W_HEARTBEAT, EMPTY_FRAME, EMPTY_FRAME, EMPTY_URI, EMPTY_FRAME, "", RBAC);
            for (Worker worker : waiting) {
                heartbeatMsg.senderID = worker.address;
                heartbeatMsg.serviceNameBytes = worker.service.nameBytes;
                heartbeatMsg.send(worker.socket);
            }
            heartbeatAt = System.currentTimeMillis() + heartBeatInterval;
        }
    }

    /**
     * This worker is now waiting for work.
     *
     * @param worker the worker
     */
    protected void workerWaiting(Worker worker) {
        // Queue to broker and service waiting lists
        waiting.addLast(worker);
        worker.service.waiting.push(worker);
        worker.updateExpiryTimeStamp();
        dispatch(worker.service);
    }

    private void dispatchMessageToMatchingSubscriber(final MdpMessage msg) {
        activeSubscriptions.keySet().stream().filter(s -> subscriptionMatcher.test(msg.topic, s)).forEach(s -> {
            // sends notification with the topic that is expected by the client for its subscription
            pubSocket.sendMore(s.toString());
            msg.send(pubSocket);
        });

        // publish also via router socket directly to known and previously subscribed clients
        final List<byte[]> tClients = routerBasedSubscriptions.get(msg.topic);
        if (tClients == null) {
            return;
        }
        for (final byte[] clientID : tClients) {
            msg.senderID = clientID;
            msg.send(routerSocket);
        }
    }

    private boolean handleSubscriptionMsg(final ZMsg subMsg) {
        if (subMsg == null || subMsg.isEmpty()) {
            return false;
        }
        final byte[] topicBytes = subMsg.getFirst().getData();
        if (topicBytes.length == 0) {
            return false;
        }
        final URI subscriptionTopic = URI.create(new String(topicBytes, 1, topicBytes.length - 1, UTF_8));
        LOGGER.atDebug().addArgument(topicBytes[0]).addArgument(subscriptionTopic).log("received subscription request: {} to '{}'");

        switch (topicBytes[0]) {
        case 0: // '0' being the default ZeroMQ un-subscribe command
            if (activeSubscriptions.computeIfAbsent(subscriptionTopic, s -> new AtomicInteger()).decrementAndGet() <= 0) {
                subSocket.unsubscribe(subscriptionTopic.toString());
            }
            return true;
        case 1: // '1' being the default ZeroMQ subscribe command
            if (activeSubscriptions.computeIfAbsent(subscriptionTopic, s -> new AtomicInteger()).incrementAndGet() == 1) {
                subSocket.subscribe(subscriptionTopic.toString());
            }
            return true;
        default:
            throw new IllegalStateException("received invalid subscription ID " + subMsg);
        }
    }

    /* default */ Service getBestMatchingService(final String serviceName) { // NOPMD package private OK
        final List<String> sortedList = services.keySet().stream().filter(serviceName::startsWith).sorted(Comparator.comparingInt(String::length)).collect(Collectors.toList());
        if (!sortedList.isEmpty()) {
            return services.get(sortedList.get(0));
        }
        // assume serviceName is a shorten (e.g. 'mmi.*') form
        final String longServiceName = brokerName + '/' + serviceName;
        final List<String> sortedList2nd = services.keySet().stream().filter(longServiceName::startsWith).sorted(Comparator.comparingInt(String::length)).collect(Collectors.toList());
        return sortedList2nd.isEmpty() ? null : services.get(sortedList2nd.get(0));
    }

    /**
     * This defines a single service.
     */
    protected class Service {
        protected final String name; // Service name
        protected final byte[] nameBytes; // Service name as byte array
        protected final List<BasicMdpWorker> mdpWorker = new ArrayList<>();
        protected final Map<RbacRole<?>, Queue<MdpMessage>> requests = new HashMap<>(); // RBAC-based queuing
        protected final Deque<Worker> waiting = new ArrayDeque<>(); // List of waiting workers
        protected final List<Thread> internalWorkers = new ArrayList<>();
        protected byte[] serviceDescription; // service OpenAPI description

        private Service(final String name, final byte[] nameBytes, final BasicMdpWorker mdpWorker) {
            this.name = name;
            this.nameBytes = nameBytes == null ? name.getBytes(UTF_8) : nameBytes;
            if (mdpWorker != null) {
                this.mdpWorker.add(mdpWorker);
            }
            rbacRoles.forEach(role -> requests.put(role, new ArrayDeque<>()));
            requests.put(BasicRbacRole.NULL, new ArrayDeque<>()); // add default queue
        }

        private MdpMessage getNextPrioritisedMessage() {
            for (RbacRole<?> role : rbacRoles) {
                final Queue<MdpMessage> queue = requests.get(role); // matched non-empty queue
                if (!queue.isEmpty()) {
                    return queue.poll();
                }
            }
            final Queue<MdpMessage> queue = requests.get(BasicRbacRole.NULL); // default queue
            return queue.isEmpty() ? null : queue.poll();
        }

        private void putPrioritisedMessage(final MdpMessage queuedMessage) {
            if (queuedMessage.hasRbackToken()) {
                // find proper RBAC queue
                final RbacToken rbacToken = RbacToken.from(queuedMessage.rbacToken);
                final Queue<MdpMessage> roleBasedQueue = requests.get(rbacToken.getRole());
                if (roleBasedQueue != null) {
                    roleBasedQueue.offer(queuedMessage);
                }
            } else {
                requests.get(BasicRbacRole.NULL).offer(queuedMessage);
            }
        }

        private boolean requestsPending() {
            return requests.entrySet().stream().anyMatch(
                    map -> !map.getValue().isEmpty());
        }

        @Override
        public String toString() {
            return "Service{name='" + name + '\'' + '}';
        }
    }

    /**
     * This defines a client service.
     */
    protected class Client {
        protected final Socket socket; // Socket client is connected to
        protected final String name; // client name
        protected final byte[] nameBytes; // client name as byte array
        protected final String nameHex; // client name as hex String
        private final Deque<MdpMessage> requests = new ArrayDeque<>(); // List of client requests
        protected long expiry = System.currentTimeMillis() + clientTimeOut; // Expires at unless heartbeat

        protected Client(final Socket socket, final String name, final byte[] nameBytes) {
            this.socket = socket;
            this.name = name;
            this.nameBytes = nameBytes == null ? name.getBytes(UTF_8) : nameBytes;
            this.nameHex = strhex(nameBytes);
        }

        protected void offerToQueue(final MdpMessage msg) {
            expiry = System.currentTimeMillis() + clientTimeOut;
            requests.offer(msg);
        }

        protected MdpMessage pop() {
            return requests.isEmpty() ? null : requests.poll();
        }
    }

    /**
     * This defines one worker, idle or active.
     */
    protected class Worker {
        protected final Socket socket; // Socket worker is connected to
        protected final byte[] address; // Address ID frame to route to
        protected final String addressHex; // Address ID frame of worker expressed as hex-String
        protected final byte[] serviceName; // service name of worker

        protected Service service; // Owning service, if known
        protected long expiry; // Expires at unless heartbeat

        private Worker(final Socket socket, final byte[] address, final String addressHex, final byte[] serviceName) { // NOPMD direct storage of address OK
            this.socket = socket;
            this.address = address;
            this.addressHex = addressHex;
            this.serviceName = serviceName;
            updateExpiryTimeStamp();
        }

        private void updateExpiryTimeStamp() {
            expiry = System.currentTimeMillis() + heartBeatExpiry;
        }
    }

    /**
     * This defines one DNS service item, idle or active.
     */
    @SuppressWarnings("PMD.CommentDefaultAccessModifier") // needed for utility classes in the same package
    public class DnsServiceItem {
        protected final byte[] address; // Address ID frame to route to
        protected final String serviceName;
        protected final List<URI> uri = new NoDuplicatesList<>();
        protected long expiry; // Expires at unless heartbeat

        private DnsServiceItem(final byte[] address, final String serviceName) { // NOPMD direct storage of address OK
            this.address = address;
            this.serviceName = serviceName;
            updateExpiryTimeStamp();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            } else if (!(o instanceof DnsServiceItem)) {
                return false;
            }
            DnsServiceItem that = (DnsServiceItem) o;
            return serviceName.equals(that.serviceName);
        }

        public String getDnsEntry() {
            return '[' + serviceName + ": " + uri.stream().map(URI::toString).collect(Collectors.joining(",")) + ']';
        }

        public String getDnsEntryHtml() {
            Optional<URI> webHandler = uri.stream().filter(u -> "https".equalsIgnoreCase(u.getScheme())).findFirst();
            if (webHandler.isEmpty()) {
                webHandler = uri.stream().filter(u -> "http".equalsIgnoreCase(u.getScheme())).findFirst();
            }
            final String wrappedService = webHandler.isEmpty() ? serviceName : wrapInAnchor(serviceName, webHandler.get());
            return '[' + wrappedService + ": " + uri.stream().map(u -> wrapInAnchor(u.toString(), u)).collect(Collectors.joining(", ")) + "]";
        }

        public List<URI> getUri() {
            return uri;
        }

        @Override
        public int hashCode() {
            return serviceName.hashCode();
        }

        @Override
        public String toString() {
            @SuppressWarnings("SpellCheckingInspection")
            final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX", Locale.UK);
            return "DnsServiceItem{address=" + ZData.toString(address) + ", serviceName='" + serviceName + "', uri= '" + uri + "',expiry=" + expiry + " - " + sdf.format(expiry) + '}';
        }

        private void updateExpiryTimeStamp() {
            expiry = System.currentTimeMillis() + dnsTimeOut * heartBeatLiveness;
        }
    }
}
