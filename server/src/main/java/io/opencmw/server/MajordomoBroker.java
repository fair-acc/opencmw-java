package io.opencmw.server;

import static org.zeromq.ZMQ.Socket;
import static org.zeromq.util.ZData.strhex;

import static io.opencmw.OpenCmwProtocol.Command.DISCONNECT;
import static io.opencmw.OpenCmwProtocol.Command.FINAL;
import static io.opencmw.OpenCmwProtocol.Command.W_HEARTBEAT;
import static io.opencmw.OpenCmwProtocol.EMPTY_FRAME;
import static io.opencmw.OpenCmwProtocol.EMPTY_URI;
import static io.opencmw.OpenCmwProtocol.MdpMessage;
import static io.opencmw.OpenCmwProtocol.MdpMessage.receive;
import static io.opencmw.OpenCmwProtocol.MdpSubProtocol;
import static io.opencmw.OpenCmwProtocol.MdpSubProtocol.PROT_WORKER;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import io.opencmw.utils.SystemProperties;
import io.opencmw.rbac.BasicRbacRole;
import io.opencmw.rbac.RbacRole;
import io.opencmw.rbac.RbacToken;

/**
 * Majordomo Protocol broker -- a minimal implementation of http://rfc.zeromq.org/spec:7 and spec:8 and following the OpenCMW specification
 * <p>
 * default heart-beat time-out [ms] is set by system property: 'OpenCMW.heartBeat' // default: 2500 [ms]
 * default heart-beat liveness is set by system property: 'OpenCMW.heartBeatLiveness' // [counts] 3-5 is reasonable
 * N.B. heartbeat expires when last heartbeat message is more than HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS ms ago.
 * this implies also, that worker must either return their message within 'HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS ms' or decouple their secondary handler interface into another thread.
 * <p>
 * default client time-out [s] is set by system property: 'OpenCMW.clientTimeOut' // default: 3600 [s] -- after which unanswered client messages and infos are being deleted
 */
public class MajordomoBroker extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(MajordomoBroker.class);
    public static final byte[] RBAC = new byte[] {}; // TODO: implement RBAC between Majordomo and Worker
    private static final String INTERNAL_SERVICE_PREFIX = "mmi.";
    private static final byte[] INTERNAL_SERVICE_PREFIX_BYTES = INTERNAL_SERVICE_PREFIX.getBytes(StandardCharsets.UTF_8);
    private static final long HEARTBEAT_LIVENESS = SystemProperties.getValueIgnoreCase("OpenCMW.heartBeatLiveness", 3); // [counts] 3-5 is reasonable
    private static final long HEARTBEAT_INTERVAL = SystemProperties.getValueIgnoreCase("OpenCMW.heartBeat", 2500); // [ms]
    private static final long HEARTBEAT_EXPIRY = HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS;
    private static final int CLIENT_TIMEOUT = SystemProperties.getValueIgnoreCase("OpenCMW.clientTimeOut", 0); // [s]
    private static final AtomicInteger BROKER_COUNTER = new AtomicInteger();

    // ---------------------------------------------------------------------
    private final ZContext ctx;
    private final Socket routerSocket;
    private final Socket pubSocket;
    private final List<String> routerSockets = new ArrayList<>(); // Sockets for clients & public external workers
    private final List<String> pubSockets = new ArrayList<>(); // Sockets for client subscriptions
    private final AtomicBoolean run = new AtomicBoolean(false);
    private final SortedSet<RbacRole<?>> rbacRoles;
    private final Map<String, Service> services = new HashMap<>(); // known services Map<'service name', Service>
    private final Map<String, Worker> workers = new HashMap<>(); // known workers Map<addressHex, Worker>
    private final Map<String, Client> clients = new HashMap<>();

    private final Deque<Worker> waiting = new ArrayDeque<>(); // idle workers
    private long heartbeatAt = System.currentTimeMillis() + HEARTBEAT_INTERVAL; // When to send HEARTBEAT

    /**
   * Initialize broker state.
   *
   * @param ioThreads number of threads dedicated to network IO (recommendation
   *     1 thread per 1 GBit/s)
   * @param rbacRoles RBAC-based roles (used for IO prioritisation and service
   *     access control
   */
    public MajordomoBroker(final int ioThreads, final RbacRole<?>... rbacRoles) {
        this.setName(MajordomoBroker.class.getSimpleName() + "#" + BROKER_COUNTER.getAndIncrement());

        ctx = new ZContext(ioThreads);

        // initialise RBAC role-based priority queues
        this.rbacRoles = Collections.unmodifiableSortedSet(new TreeSet<>(Set.of(rbacRoles)));

        // generate and register internal default inproc socket
        routerSocket = ctx.createSocket(SocketType.ROUTER);
        routerSocket.setHWM(0);
        routerSocket.bind("inproc://broker"); // NOPMD
        pubSocket = ctx.createSocket(SocketType.XPUB);
        pubSocket.setHWM(0);
        pubSocket.bind("inproc://publish"); // NOPMD

        registerDefaultServices(rbacRoles); // NOPMD
    }

    public void
    addInternalService(final MajordomoWorker worker) {
        assert worker != null : "worker must not be null";
        services.computeIfAbsent(worker.getServiceName(), s -> new Service(s, s.getBytes(StandardCharsets.UTF_8), worker));
        worker.start();
    }

    /**
   * Bind broker to endpoint, can call this multiple times. We use a single
   * socket for both clients and workers.
   */
    public Socket bind(String endpoint) {
        routerSocket.bind(endpoint);
        routerSockets.add(endpoint);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.atDebug().addArgument(endpoint).log(
                    "Majordomo broker/0.1 is active at '{}'");
        }
        return routerSocket;
    }

    /**
   * Bind broker to endpoint, can call this multiple times. We use a single
   * socket for both clients and workers.
   */
    public Socket bindPub(String endpoint) {
        pubSocket.bind(endpoint);
        pubSockets.add(endpoint);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.atDebug().addArgument(endpoint).log(
                    "Majordomo broker/0.1 publication is active at '{}'");
        }
        return pubSocket;
    }

    public ZContext getContext() {
        return ctx;
    }

    public Socket getInternalRouterSocket() {
        return routerSocket;
    }

    /**
   * @return unmodifiable list of registered external sockets
   */
    public List<String> getRouterSockets() {
        return Collections.unmodifiableList(routerSockets);
    }

    public List<String> getPubSockets() {
        return Collections.unmodifiableList(pubSockets);
    }

    public Collection<Service> getServices() {
        return services.values();
    }

    public boolean isRunning() {
        return run.get();
    }

    public void removeService(final String serviceName) {
        final Service ret = services.remove(serviceName);
        ret.mdpWorker.forEach(MajordomoWorker::stopWorker);
        ret.waiting.forEach(worker
                -> new MdpMessage(worker.address, PROT_WORKER,
                        DISCONNECT, worker.service.nameBytes,
                        EMPTY_FRAME,
                        URI.create(worker.service.name),
                        EMPTY_FRAME, "", RBAC)
                           .send(worker.socket));
    }

    /**
   * main broker work happens here
   */
    @Override
    public void run() {
        try (final ZMQ.Poller items = ctx.createPoller(routerSockets.size())) {
            items.register(routerSocket, ZMQ.Poller.POLLIN);
            items.register(pubSocket, ZMQ.Poller.POLLIN);
            while (run.get() && !Thread.currentThread().isInterrupted() && items.poll(HEARTBEAT_INTERVAL) != -1) {
                int loopCount = 0;
                boolean receivedMsg = true;
                while (run.get() && !Thread.currentThread().isInterrupted() && receivedMsg) {
                    receivedMsg = false;
                    final MdpMessage routerMsg = receive(routerSocket, false);
                    if (routerMsg != null) {
                        if (LOGGER.isTraceEnabled()) {
                            LOGGER.atTrace().addArgument(routerMsg).log(
                                    "Majordomo broker received new message: '{}'");
                        }
                        receivedMsg |= handleReceivedMessage(routerSocket, routerMsg);
                    }

                    final ZMsg subMsg = ZMsg.recvMsg(pubSocket, false);
                    if (subMsg != null) {
                        if (LOGGER.isTraceEnabled()) {
                            LOGGER.atTrace().addArgument(subMsg).log(
                                    "Majordomo broker received new message: '{}'");
                        }
                        final byte[] subMessage = !subMsg.isEmpty() ? subMsg.getFirst().getData()
                                                                    : new byte[] { 1 }; // TODO: refactor into an
                        // enum/class representation
                        final String subType = subMessage[0] == 1 ? "subscribe" : "unsubscribe";
                        final String subTopic = subMessage.length > 1
                                                        ? new String(subMessage, 1, subMessage.length - 1,
                                                                StandardCharsets.UTF_8)
                                                        : "*";
                        LOGGER.atDebug().addArgument(subType).addArgument(subTopic).log(
                                "received subscription request: {} to '{}'");
                        receivedMsg = true;
                    }

                    processClients();
                    if (loopCount % 10 == 0) {
                        // perform maintenance tasks during the first and every tenth
                        // iteration
                        purgeWorkers();
                        purgeClients();
                        sendHeartbeats();
                    }
                    loopCount++;
                }
            }
        }
        destroy(); // interrupted
    }

    @Override
    public synchronized void start() {
        run.set(true);
        services.forEach(
                (serviceName,
                        service) -> service.internalWorkers.forEach(Thread::start));
        super.start();
    }

    public void stopBroker() {
        run.set(false);
    }

    /**
   * Deletes worker from all data structures, and destroys worker.
   */
    protected void deleteWorker(Worker worker, boolean disconnect) {
        assert (worker != null);
        if (disconnect) {
            new MdpMessage(worker.address, PROT_WORKER, DISCONNECT,
                    worker.service.nameBytes, EMPTY_FRAME,
                    URI.create(worker.service.name), EMPTY_FRAME, "", RBAC)
                    .send(worker.socket);
        }
        if (worker.service != null) {
            worker.service.waiting.remove(worker);
        }
        workers.remove(worker.addressHex);
    }

    /**
   * Disconnect all workers, destroy context.
   */
    protected void destroy() {
        Worker[] deleteList = workers.values().toArray(new Worker[0]);
        for (Worker worker : deleteList) {
            deleteWorker(worker, true);
        }
        ctx.destroy();
    }

    /**
   * Dispatch requests to waiting workers as possible
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

    protected boolean handleReceivedMessage(final Socket receiveSocket,
            final MdpMessage msg) {
        switch (msg.protocol) {
        case PROT_CLIENT:
        case PROT_CLIENT_HTTP:
            // Set reply return address to client sender
            final String senderName = msg.getSenderName();
            final Client client = clients.computeIfAbsent(
                    senderName,
                    s
                    -> new Client(receiveSocket, msg.protocol, senderName, msg.senderID));
            client.offerToQueue(msg);
            return true;
        case PROT_WORKER:
            processWorker(receiveSocket, msg);
            return true;
        default:
            // N.B. not too verbose logging since we do not want that sloppy clients
            // can bring down the broker through warning or info messages
            if (LOGGER.isDebugEnabled()) {
                LOGGER.atDebug().addArgument(msg).log(
                        "Majordomo broker invalid message: '{}'");
            }
            return false;
        }
    }

    /**
   * Process a request coming from a client.
   */
    protected void processClients() {
        // round-robbin
        clients.forEach((name, client) -> {
            final MdpMessage clientMessage = client.pop();
            if (clientMessage == null) {
                return;
            }
            // dispatch client message to worker queue
            final Service service = services.get(clientMessage.getServiceName());
            if (service == null) {
                // not implemented -- according to Majordomo Management Interface (MMI)
                // as defined in http://rfc.zeromq.org/spec:8
                new MdpMessage(clientMessage.senderID, client.protocol, FINAL,
                        clientMessage.serviceNameBytes,
                        clientMessage.clientRequestID,
                        URI.create("mdp/UnknownService"),
                        "501".getBytes(StandardCharsets.UTF_8), "", RBAC)
                        .send(client.socket);
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
   */
    protected void processWorker(final Socket receiveSocket,
            final MdpMessage msg) {
        final String senderIdHex = strhex(msg.senderID);
        final String serviceName = msg.getServiceName();
        final boolean workerReady = workers.containsKey(senderIdHex);
        final Worker worker = requireWorker(receiveSocket, msg.senderID, senderIdHex);

        switch (msg.command) {
        case READY:
            // Attach worker to service and mark as idle
            worker.service = requireService(serviceName, msg.serviceNameBytes);
            workerWaiting(worker);
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
                msg.protocol = client.protocol;
                msg.serviceNameBytes = serviceID;
                msg.send(client.socket);
                workerWaiting(worker);
            } else {
                deleteWorker(worker, true);
            }
            break;
        case W_NOTIFY:
            msg.send(pubSocket);

            final Client client = clients.get(msg.getServiceName());
            if (client == null || client.socket == null) {
                break;
            }
            // need to replace clientID with service name
            final byte[] serviceID = worker.service.nameBytes;
            msg.senderID = msg.serviceNameBytes;
            msg.protocol = client.protocol;
            msg.command = FINAL;
            msg.serviceNameBytes = serviceID;
            msg.send(client.socket);
            break;
        case W_HEARTBEAT:
            if (workerReady && worker != null) {
                worker.expiry = System.currentTimeMillis() + HEARTBEAT_EXPIRY;
            } else {
                deleteWorker(worker, true);
            }
            break;
        case DISCONNECT:
            deleteWorker(worker, false);
            break;
        default:
            // N.B. not too verbose logging since we do not want that sloppy clients
            // can bring down the broker through warning or info messages
            if (LOGGER.isDebugEnabled()) {
                LOGGER.atDebug().addArgument(msg).log(
                        "Majordomo broker invalid message: '{}'");
            }
            break;
        }
    }

    /**
   * Look for &amp; kill expired clients.
   */
    protected /*synchronized*/ void purgeClients() {
        if (CLIENT_TIMEOUT <= 0) {
            return;
        }
        for (String clientName : clients.keySet()) { // NOSONAR NOPMD copy because
            // we are going to remove keys
            Client client = clients.get(clientName);
            if (client == null || client.expiry < System.currentTimeMillis()) {
                clients.remove(clientName);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.atDebug().addArgument(client).log(
                            "Majordomo broker deleting expired client: '{}'");
                }
            }
        }
    }

    /**
   * Look for &amp; kill expired workers. Workers are oldest to most recent, so
   * we stop at the first alive worker.
   */
    protected /*synchronized*/ void purgeWorkers() {
        for (Worker w = waiting.peekFirst();
                w != null && w.expiry < System.currentTimeMillis();
                w = waiting.peekFirst()) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.atInfo()
                        .addArgument(w.addressHex)
                        .addArgument(w.service == null ? "(unknown)" : w.service.name)
                        .log(
                                "Majordomo broker deleting expired worker: '{}' - service: '{}'");
            }
            deleteWorker(waiting.pollFirst(), false);
        }
    }

    protected void registerDefaultServices(final RbacRole<?>[] rbacRoles) {
        // add simple internal Majordomo worker
        final int nServiceThreads = 3;

        // Majordomo Management Interface (MMI) as defined in http://rfc.zeromq.org/spec:8
        MajordomoWorker mmiService = new MajordomoWorker(ctx, "mmi.service", rbacRoles);
        mmiService.registerHandler(context -> {
            final String serviceName = (context.req.data != null) ? new String(context.req.data, StandardCharsets.UTF_8) : "";
            final String returnCode;
            if (!serviceName.isBlank()) {
                returnCode = services.containsKey(serviceName) ? "200" : "400";
            } else {
                returnCode = services.keySet().stream().sorted().collect(
                        Collectors.joining(","));
            }
            context.rep.data = returnCode.getBytes(StandardCharsets.UTF_8);
        });
        addInternalService(mmiService);

        MajordomoWorker mmiDns = new MajordomoWorker(ctx, "mmi.dns", rbacRoles);
        mmiDns.registerHandler(ctx -> {
            final String returnCode = routerSockets.stream()
                                              .map(s -> s.toLowerCase(Locale.UK).replace("tcp", "mdp"))
                                              .collect(Collectors.joining(","));
            ctx.rep.data = returnCode.getBytes(StandardCharsets.UTF_8);
        });
        addInternalService(mmiDns);

        // echo service
        for (int i = 0; i < nServiceThreads; i++) {
            MajordomoWorker echoService = new MajordomoWorker(ctx, "mmi.echo", rbacRoles);
            echoService.registerHandler(
                    ctx
                    -> ctx.rep.data = ctx.req.data); //  output = input : echo service is complex :-)
            addInternalService(echoService);
        }

        // Hello World service
        MajordomoWorker httpDemoService = new MajordomoWorker(ctx, "mmi.hello", rbacRoles);
        httpDemoService.registerHandler(
                ctx -> ctx.rep.data = "Hello World!".getBytes(StandardCharsets.UTF_8));
        addInternalService(httpDemoService);
    }

    /**
   * Locates the service (creates if necessary).
   *
   * @param serviceName      service name
   * @param serviceNameBytes UTF-8 encoded service name
   */
    protected Service requireService(final String serviceName,
            final byte[] serviceNameBytes) {
        assert (serviceNameBytes != null);
        return services.computeIfAbsent(
                serviceName, s -> new Service(serviceName, serviceNameBytes, null));
    }

    /**
     * Finds the worker (creates if necessary).
     */
    protected Worker requireWorker(final Socket socket, final byte[] address,
            final String addressHex) {
        assert (addressHex != null);
        return workers.computeIfAbsent(addressHex, identity -> {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.atInfo().addArgument(addressHex).log("registering new worker: '{}'");
            }
            return new Worker(socket, address, addressHex);
        });
    }

    /**
   * Send heartbeats to idle workers if it's time
   */
    protected /*synchronized*/ void sendHeartbeats() {
        // Send heartbeats to idle workers if it's time
        if (System.currentTimeMillis() >= heartbeatAt) {
            for (Worker worker : waiting) {
                final byte[] clientRequestID = new byte[0];
                new MdpMessage(worker.address, PROT_WORKER, W_HEARTBEAT, worker.service.nameBytes, clientRequestID, EMPTY_URI, EMPTY_FRAME, "", RBAC).send(worker.socket);
            }
            heartbeatAt = System.currentTimeMillis() + HEARTBEAT_INTERVAL;
        }
    }

    /**
   * This worker is now waiting for work.
   */
    protected /*synchronized*/ void workerWaiting(Worker worker) {
        // Queue to broker and service waiting lists
        waiting.addLast(worker);
        // TODO: evaluate addLast vs. push (addFirst) - latter should be more
        // beneficial w.r.t. CPU context switches (reuses the same thread/context
        // frequently
        // do not know why original implementation wanted to spread across different
        // workers (load balancing across different machines perhaps?!=)
        // worker.service.waiting.addLast(worker);
        worker.service.waiting.push(worker);
        worker.expiry = System.currentTimeMillis() + HEARTBEAT_EXPIRY;
        dispatch(worker.service);
    }

    /**
   * Main method - create and start new broker.
   *
   * @param args use '-v' for putting worker in verbose mode
   */
    public static void main(String[] args) {
        MajordomoBroker broker = new MajordomoBroker(1, BasicRbacRole.values());
        LOGGER.atInfo().log("broker initialised");
        // broker.setDaemon(true); // use this if running in another app that
        // controls threads Can be called multiple times with different endpoints
        broker.bind("tcp://*:5555");
        broker.bind("tcp://*:5556");
        LOGGER.atInfo().log("broker bound");

        for (int i = 0; i < 10; i++) {
            // simple internalSock echo
            MajordomoWorker workerSession = new MajordomoWorker(broker.getContext(), "inproc.echo", BasicRbacRole.ADMIN);
            workerSession.registerHandler(ctx -> ctx.rep.data = ctx.req.data); //  output = input : echo service is complex :-)
            workerSession.start();
        }
        LOGGER.atInfo().log("added services");

        broker.start();
        LOGGER.atInfo().log("broker started");
    }

    /**
   * This defines a client service.
   */
    protected static class Client {
        protected final Socket socket; // Socket client is connected to
        protected final MdpSubProtocol protocol;
        protected final String name; // client name
        protected final byte[] nameBytes; // client name as byte array
        protected final String nameHex; // client name as hex String
        private final Deque<MdpMessage> requests = new ArrayDeque<>(); // List of client requests
        protected long expiry = System.currentTimeMillis() + CLIENT_TIMEOUT * 1000L; // Expires at unless heartbeat

        public Client(final Socket socket, final MdpSubProtocol protocol,
                final String name, final byte[] nameBytes) {
            this.socket = socket;
            this.protocol = protocol;
            this.name = name;
            this.nameBytes = nameBytes == null ? name.getBytes(StandardCharsets.UTF_8) : nameBytes;
            this.nameHex = strhex(nameBytes);
        }

        public void offerToQueue(final MdpMessage msg) {
            expiry = System.currentTimeMillis() + CLIENT_TIMEOUT * 1000L;
            requests.offer(msg);
        }

        public MdpMessage pop() {
            return requests.isEmpty() ? null : requests.poll();
        }
    }

    /**
   * This defines a single service.
   */
    protected class Service {
        protected final String name; // Service name
        protected final byte[] nameBytes; // Service name as byte array
        protected final List<MajordomoWorker> mdpWorker = new ArrayList<>();
        protected final Map<RbacRole<?>, Queue<MdpMessage>> requests = new HashMap<>(); // RBAC-based queuing
        protected final Deque<Worker> waiting = new ArrayDeque<>(); // List of waiting workers
        protected final List<Thread> internalWorkers = new ArrayList<>();

        public Service(final String name, final byte[] nameBytes, final MajordomoWorker mdpWorker) {
            this.name = name;
            this.nameBytes = nameBytes == null ? name.getBytes(StandardCharsets.UTF_8) : nameBytes;
            if (mdpWorker != null) {
                this.mdpWorker.add(mdpWorker);
            }
            rbacRoles.forEach(role -> requests.put(role, new ArrayDeque<>()));
            requests.put(BasicRbacRole.NULL, new ArrayDeque<>()); // add default queue
        }

        public boolean requestsPending() {
            return requests.entrySet().stream().anyMatch(
                    map -> !map.getValue().isEmpty());
        }

        /**
     * @return next RBAC prioritised message or 'null' if there aren't any
     */
        protected final MdpMessage getNextPrioritisedMessage() {
            for (RbacRole<?> role : rbacRoles) {
                final Queue<MdpMessage> queue = requests.get(role); // matched non-empty queue
                if (!queue.isEmpty()) {
                    return queue.poll();
                }
            }
            final Queue<MdpMessage> queue = requests.get(BasicRbacRole.NULL); // default queue
            return queue.isEmpty() ? null : queue.poll();
        }

        protected void putPrioritisedMessage(final MdpMessage queuedMessage) {
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
    }

    /**
   * This defines one worker, idle or active.
   */
    protected class Worker {
        protected final Socket socket; // Socket worker is connected to
        protected final byte[] address; // Address ID frame to route to
        protected final String addressHex; // Address ID frame of worker expressed as hex-String

        protected final boolean external;
        protected Service service; // Owning service, if known
        protected long expiry = System.currentTimeMillis() + HEARTBEAT_LIVENESS; // Expires at unless heartbeat

        public Worker(final Socket socket, final byte[] address, final String addressHex) {
            this.socket = socket;
            this.external = true;
            this.address = address;
            this.addressHex = addressHex;
        }
    }
}
