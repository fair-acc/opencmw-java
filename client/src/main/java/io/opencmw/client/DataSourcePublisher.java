package io.opencmw.client;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

import static io.opencmw.OpenCmwConstants.*;
import static io.opencmw.OpenCmwProtocol.EMPTY_FRAME;
import static io.opencmw.OpenCmwProtocol.EMPTY_ZFRAME;
import static io.opencmw.utils.AnsiDefs.ANSI_RED;
import static io.opencmw.utils.AnsiDefs.ANSI_RESET;

import java.io.Closeable;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.net.ProtocolException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import io.micrometer.core.instrument.Metrics;
import io.opencmw.EventStore;
import io.opencmw.Filter;
import io.opencmw.MimeType;
import io.opencmw.OpenCmwProtocol.Command;
import io.opencmw.QueryParameterParser;
import io.opencmw.RingBufferEvent;
import io.opencmw.client.cmwlight.CmwLightDataSource;
import io.opencmw.client.rest.RestDataSource;
import io.opencmw.domain.BinaryData;
import io.opencmw.filter.EvtTypeFilter;
import io.opencmw.filter.FilterRegistry;
import io.opencmw.rbac.RbacProvider;
import io.opencmw.serialiser.IoBuffer;
import io.opencmw.serialiser.IoClassSerialiser;
import io.opencmw.serialiser.IoSerialiser;
import io.opencmw.serialiser.spi.FastByteBuffer;
import io.opencmw.utils.CustomFuture;
import io.opencmw.utils.SharedPointer;

/**
 * Publishes events from different sources into a common {@link EventStore} and takes care of setting the appropriate
 * filters and deserialisation of the domain objects.
 *
 * The subscribe/unsubscribe/set/get methods can be called from any thread and are decoupled from the actual io thread.
 * The results can be either processed using the disruptor based event processing, using Future&lt;&gt;, or notification
 * callback functions.
 *
 * Internally the Publisher uses two disruptor ring buffers. An internal ring buffer just stores the raw input messages
 * to prevent messages from getting lost, only acknowledging receipt. An internal EventHandler thread then processes
 * these, deserialises the domain objects and context information and then republishes them to a user provided
 * eventStore and/or notifies futures/event-listeners.
 *
 * All requests to the DataSource publisher follow a standard URI syntax (as commonly used also in web/http requests),
 * ie. '<pre>scheme:[//authority]path[?query][#fragment]</pre>'
 * see <a href="https://tools.ietf.org/html/rfc3986">documentation</a> for details.
 *
 * The scheme and authority part are used to resolve a suitable implementation and retrieve/instantiate a client instance.
 * The client uses the path and query part to perform the actual request.
 * As a second argument, they take the class type of the requested domain object and the result context.
 * Get and Set requests return a Future, which will be notified once the request is completed.
 * Subscription requests take an optional RequestListener which will be notified with the domain and context object or
 * an eventual exception.
 *
 * Context data:
 * Context data is provided back via the returned endpoint's URI query string.
 * For the result endpoint, all available filters are filled from the query string.
 * To use the subscription update, a dedicated ContextType has to be passed to the request and will be used to notify the
 * subscription listener.
 *
 * Future features:
 * The Publisher takes an optional RBAC provider which will be used to sign requests. It can be overridden for each
 * individual request.
 *
 * @author Alexander Krimmm
 * @author rstein
 */
@SuppressWarnings({ "PMD.GodClass", "PMD.ExcessiveImports", "PMD.TooManyFields" })
public class DataSourcePublisher implements Runnable, Closeable {
    public static final int MIN_FRAMES_INTERNAL_MSG = 3;
    private static final Logger LOGGER = LoggerFactory.getLogger(DataSourcePublisher.class);
    private static final AtomicInteger INSTANCE_COUNT = new AtomicInteger();

    static { // register default data sources
        DataSource.register(CmwLightDataSource.FACTORY);
        DataSource.register(RestDataSource.FACTORY);
        DataSource.register(OpenCmwDataSource.FACTORY);
    }

    protected final String inprocCtrl = "inproc://dsPublisher#" + INSTANCE_COUNT.incrementAndGet();
    protected final Map<String, ThePromisedFuture<?, ?>> requests = new ConcurrentHashMap<>(); // <requestId, future for the get request>
    protected final Map<String, DataSource> clientMap = new ConcurrentHashMap<>(); // scheme://authority -> DataSource
    protected final AtomicInteger internalReqIdGenerator = new AtomicInteger(0);
    protected final ExecutorService executor; // NOPMD - threads are ok, not a webapp
    protected final ZContext context;
    protected final ZMQ.Poller poller;
    protected final ZMQ.Socket sourceSocket;
    protected final String clientId;
    private final IoBuffer byteBuffer = new FastByteBuffer(0, true, null); // never actually used
    private final IoClassSerialiser ioClassSerialiser = new IoClassSerialiser(byteBuffer);
    private final AtomicBoolean shallRun = new AtomicBoolean(false);
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final EventStore rawDataEventStore;
    private final RbacProvider rbacProvider;
    private final EventStore publicationTarget;
    private final AtomicReference<Thread> threadReference = new AtomicReference<>();

    public DataSourcePublisher(final RbacProvider rbacProvider, final ExecutorService executorService, final String... clientId) {
        this(null, null, rbacProvider, executorService, clientId);
        start(); // NOPMD
    }

    public DataSourcePublisher(final ZContext ctx, final EventStore publicationTarget, final RbacProvider rbacProvider, final ExecutorService executorService, final String... clientId) {
        this.context = Objects.requireNonNullElse(ctx, new ZContext(N_IO_THREADS));
        this.executor = Objects.requireNonNullElse(executorService, Executors.newCachedThreadPool());
        poller = context.createPoller(1);
        // control socket for adding subscriptions / triggering requests from other threads
        sourceSocket = context.createSocket(SocketType.DEALER);
        setDefaultSocketParameters(sourceSocket);
        sourceSocket.bind(inprocCtrl);
        poller.register(sourceSocket, ZMQ.Poller.POLLIN);

        // instantiate internal event store and handler for raw messages
        // the internal handler deserialises updates, republishes them, or notifies external listeners/futures
        rawDataEventStore = EventStore.getFactory().setSingleProducer(true).setFilterConfig(EvtTypeFilter.class).build();
        rawDataEventStore.register(this::internalEventHandler);

        // setup clientId to identify subscribers from server
        this.clientId = clientId.length == 1 ? clientId[0] : DataSourcePublisher.class.getName();
        this.rbacProvider = rbacProvider;

        this.publicationTarget = publicationTarget; // register external event store
    }

    public ZContext getContext() {
        return context;
    }

    public EventStore getRawDataEventStore() {
        return rawDataEventStore;
    }

    public Client getClient() {
        return new Client();
    }

    @Override
    public void close() {
        shallRun.set(false);
        Thread thread = threadReference.get();
        if (thread != null) {
            try {
                thread.join(2L * HEARTBEAT_INTERVAL); // extra margin since the poller is running also at exactly 'heartbeatInterval'
            } catch (InterruptedException e) { // NOPMD NOSONAR - re-throwing with different type
                throw new IllegalStateException(thread.getName() + " did not shut down in " + HEARTBEAT_INTERVAL + " ms", e);
            }
        }
        thread = threadReference.get();
        if (running.get() && thread != null) {
            LOGGER.atWarn().addArgument(thread.getName()).log("'{}' did not shut down as requested, going to forcefully interrupt");
            thread.interrupt();
        }
        if (!context.isClosed()) {
            poller.close();
            sourceSocket.close();
        }
    }

    public void start() {
        Thread thread = threadReference.get();
        if (thread != null) {
            LOGGER.atWarn().addArgument(thread.getName()).log("Thread '{}' already running");
            return;
        }
        final String threadName = DataSourcePublisher.class.getSimpleName() + "Thread-" + clientId;
        thread = new Thread(null, this, threadName, 0); // NOPMD - not a webapp
        threadReference.set(thread);
        thread.setDaemon(true);
        thread.start();
    }

    public void stop() {
        close();
    }

    @Override
    public void run() {
        try {
            if (shallRun.getAndSet(true)) {
                return; // is already running
            }
            running.set(true);
            // start the ring buffer and its processors
            rawDataEventStore.start();
            // event loop polling all data sources and performing regular housekeeping jobs
            long nextHousekeeping = System.currentTimeMillis(); // immediately perform first housekeeping
            long timeOut;
            int pollerReturn;
            do {
                boolean dataAvailable = !context.isClosed();
                while (dataAvailable && System.currentTimeMillis() < nextHousekeeping && shallRun.get()) {
                    dataAvailable = handleDataSourceSockets(); // get data from clients
                    dataAvailable |= handleControlSocket(); // check specifically for control socket
                }
                final long now = System.currentTimeMillis();
                nextHousekeeping = clientMap.values().stream().mapToLong(DataSource::housekeeping).min().orElse(now + HEARTBEAT_INTERVAL);
                timeOut = nextHousekeeping - now;
                // remove closed client sockets from poller
                clientMap.values().stream().filter(DataSource::isClosed).forEach(s -> poller.unregister(s.getSocket()));
                // wait for next message or next requested housekeeping
                Metrics.counter(DataSourcePublisher.class.getSimpleName() + ".eventLoopRate").increment();
                pollerReturn = poller.poll(Math.max(0L, timeOut)); // ensure that argument is never negative which would lead to blocking the poller indefinitely (0 returns immediately)
            } while ((timeOut <= 0 || -1 != pollerReturn) && !Thread.interrupted() && shallRun.get() && !context.isClosed());
            if (shallRun.get()) {
                LOGGER.atError().addArgument(Thread.interrupted()).addArgument(context.isClosed()).addArgument(pollerReturn).addArgument(clientMap.values()).log("abnormally terminated (int={},ctx={},poll={}) - abort run() - clients = {}");
            } else {
                LOGGER.atDebug().log("shutting down DataSourcePublisher");
            }
            rawDataEventStore.stop();
            for (DataSource dataSource : clientMap.values()) {
                try {
                    poller.unregister(dataSource.getSocket());
                    dataSource.close();
                } catch (Exception e) { // NOPMD
                    // shut-down close
                    LOGGER.atError().setCause(e).addArgument(dataSource.toString()).log("data source {} did not close properly");
                }
            }
            running.set(false);
            threadReference.set(null);
        } catch (Exception e) {
            LOGGER.atError().setCause(e).log("Unrecoverable Error in DataSourcePublisher, shutting down application");
            System.exit(1);
        }
    }

    protected boolean handleControlSocket() {
        final ZMsg controlMsg = ZMsg.recvMsg(sourceSocket, false);
        if (controlMsg == null) {
            return false; // no more data available on control socket
        }

        // msgType, requestId and endpoint have to be always present
        assert controlMsg.size() >= MIN_FRAMES_INTERNAL_MSG : "ignoring invalid message - message size: " + controlMsg.size();

        final Command msgType = Command.getCommand(controlMsg.pollFirst().getData());
        final String requestId = requireNonNull(controlMsg.pollFirst()).getString(UTF_8);
        final URI endpoint = URI.create(requireNonNull(controlMsg.pollFirst()).getString(UTF_8));
        final byte[] data = controlMsg.isEmpty() ? EMPTY_FRAME : controlMsg.pollFirst().getData();
        final byte[] rbacToken = controlMsg.isEmpty() ? EMPTY_FRAME : controlMsg.pollFirst().getData();

        final DataSource client = getClient(endpoint); // get client for endpoint
        switch (msgType) {
        case SUBSCRIBE: // subscribe: 0b, requestId, addr/dev/prop?sel&filters
            client.subscribe(requestId, endpoint, rbacToken);
            return true;
        case GET_REQUEST: // 1b, reqId, endpoint
            client.get(requestId, endpoint, data, rbacToken);
            return true;
        case SET_REQUEST: // 2b, reqId, endpoint, data
            client.set(requestId, endpoint, data, rbacToken);
            return true;
        case UNSUBSCRIBE: // 3b, reqId, endpoint
            client.unsubscribe(requestId);
            requests.remove(requestId);
            return true;
        default:
        }
        throw new UnsupportedOperationException("Illegal operation type");
    }

    protected boolean handleDataSourceSockets() {
        boolean dataAvailable = false;
        for (final DataSource entry : clientMap.values()) {
            final ZMsg reply = entry.getMessage();
            if (reply == null || reply.isEmpty()) {
                continue; // no data received, queue empty or there was data received, but only used for internal state of the client
            }

            dataAvailable = true;
            // the received data consists of the following frames:
            // replyType(byte), reqId(string), endpoint(string), dataBody(byte[])
            final String reqId = requireNonNull(reply.pollFirst()).getString(UTF_8);
            final ThePromisedFuture<?, ?> returnFuture = requests.get(reqId);
            assert returnFuture != null : "no future available for reqId:" + reqId;
            rawDataEventStore.getRingBuffer().publishEvent((event, sequence) -> {
                if (returnFuture.getReplyType() != Command.SUBSCRIBE) { // remove entries for one time replies
                    assert returnFuture.getInternalRequestID().equals(reqId)
                        : "requestID mismatch";
                    requests.remove(reqId);
                }
                final String endpoint = requireNonNull(reply.pollFirst()).getString(UTF_8);
                final InternalDomainObject internalData = new InternalDomainObject(reply, returnFuture); // NOPMD -- need to create
                event.arrivalTimeStamp = System.currentTimeMillis();
                event.payload = Objects.requireNonNullElseGet(event.payload, SharedPointer::new); // NOPMD - need to create new shared pointer instance
                event.payload.set(internalData); // InternalDomainObject containing - ZMsg containing header, body and exception frame + Future

                final EvtTypeFilter evtTypeFilter = event.getFilter(EvtTypeFilter.class);
                evtTypeFilter.updateType = returnFuture.requestType;
                evtTypeFilter.property = URI.create(endpoint);
            });
        }
        return dataAvailable;
    }

    protected <R> ThePromisedFuture<R, ?> newRequestFuture(final URI endpoint, final Class<R> requestedDomainObjType, final Command requestType, final String requestId) {
        FilterRegistry.checkClassForNewFilters(requestedDomainObjType);
        final ThePromisedFuture<R, ?> requestFuture = new ThePromisedFuture<>(endpoint, requestedDomainObjType, null, requestType, requestId, null);
        final Object oldEntry = requests.put(requestId, requestFuture);
        assert oldEntry == null : "requestID '" + requestId + "' already present in requestFutureMap";
        return requestFuture;
    }

    protected <R, C> ThePromisedFuture<R, C> newSubscriptionFuture(final URI endpoint, final Class<R> requestedDomainObjType, final Class<C> contextType, final String requestId, final NotificationListener<R, C> listener) {
        FilterRegistry.checkClassForNewFilters(requestedDomainObjType);
        final ThePromisedFuture<R, C> requestFuture = new ThePromisedFuture<>(endpoint, requestedDomainObjType, contextType, Command.SUBSCRIBE, requestId, listener);
        final Object oldEntry = requests.put(requestId, requestFuture);
        assert oldEntry == null : "requestID '" + requestId + "' already present in requestFutureMap";
        return requestFuture;
    }

    @SuppressWarnings({ "PMD.UnusedFormalParameter" }) // method signature is mandated by functional interface
    protected void internalEventHandler(final RingBufferEvent event, final long sequence, final boolean endOfBatch) {
        final EvtTypeFilter evtTypeFilter = event.getFilter(EvtTypeFilter.class);
        final boolean notifyFuture;
        switch (evtTypeFilter.updateType) {
        case GET_REQUEST:
        case SET_REQUEST:
            notifyFuture = true;
            break;
        case SUBSCRIBE:
            notifyFuture = false;
            break;
        default:
            // ignore other reply types for now
            // todo: publish statistics, connection state and getRequests
            return;
        }
        final URI endpointURI = evtTypeFilter.property;
        // get data from internal ring-buffer domain object
        final InternalDomainObject domainObject = requireNonNull(event.payload.get(InternalDomainObject.class), "empty payload");
        final byte[] body = requireNonNull(domainObject.data.poll(), "null body data").getData();
        final String exceptionMsg = requireNonNull(domainObject.data.poll(), "null exception message").getString(UTF_8);
        Object replyDomainObject = null;
        if (exceptionMsg.isBlank()) {
            try {
                final Class<?> reqClassType = domainObject.future.getRequestedDomainObjType();
                if (reqClassType.isAssignableFrom(BinaryData.class)) {
                    // special case: handle byte[] and/or -- albeit UTF8 byte[] encoded -- String data
                    replyDomainObject = new BinaryData(domainObject.future.getInternalRequestID(), MimeType.BINARY, body);
                } else if (body.length != 0) {
                    ioClassSerialiser.setDataBuffer(FastByteBuffer.wrap(body));
                    replyDomainObject = ioClassSerialiser.deserialiseObject(reqClassType);
                    ioClassSerialiser.setDataBuffer(byteBuffer); // allow received byte array to be released
                }
                if (notifyFuture) {
                    domainObject.future.castAndSetReply(replyDomainObject); // notify callback
                }
                if (domainObject.future.listener != null) {
                    final Object finalDomainObj = replyDomainObject;
                    final Object contextObject;
                    if (domainObject.future.contextType == null) {
                        contextObject = QueryParameterParser.getMap(endpointURI.getQuery());
                    } else {
                        contextObject = QueryParameterParser.parseQueryParameter(domainObject.future.contextType, endpointURI.getQuery());
                    }
                    executor.submit(() -> domainObject.future.notifyListener(finalDomainObj, contextObject)); // NOPMD - threads are ok, not a webapp
                }
            } catch (Exception e) { // NOPMD: exception is forwarded to client
                final StringWriter sw = new StringWriter();
                final PrintWriter pw = new PrintWriter(sw);
                e.printStackTrace(pw);
                final ProtocolException protocolException = new ProtocolException(ANSI_RED + "error deserialising object:\n" + sw.toString() + ANSI_RESET);
                if (notifyFuture) {
                    domainObject.future.setException(protocolException);
                } else {
                    executor.submit(() -> domainObject.future.listener.updateException(protocolException)); // NOPMD - threads are ok, not a webapp
                }
            }
        } else if (notifyFuture) {
            domainObject.future.setException(new ProtocolException(exceptionMsg)); // notify exception callback
        } else {
            executor.submit(() -> domainObject.future.listener.updateException(new ProtocolException(exceptionMsg))); // NOPMD - threads are ok, not a webapp
        }
        // publish to external ring buffer
        if (publicationTarget != null) {
            publicationTarget.getRingBuffer().publishEvent(this::publishToExternalStore, event, replyDomainObject, exceptionMsg);
        }
    }

    @SuppressWarnings({ "PMD.UnusedFormalParameter" }) // method signature is mandated by functional interface
    protected void publishToExternalStore(final RingBufferEvent publishEvent, final long seq, final RingBufferEvent sourceEvent, final Object replyDomainObject, final String exception) {
        sourceEvent.copyTo(publishEvent);
        publishEvent.payload = new SharedPointer<>();
        if (replyDomainObject != null) {
            publishEvent.payload.set(replyDomainObject);
        }
        if (exception != null && !exception.isBlank()) {
            publishEvent.throwables.add(new ProtocolException(exception));
        }
        final EvtTypeFilter evtTypeFilter = publishEvent.getFilter(EvtTypeFilter.class);
        // update other filter in destination ring-buffer
        for (final Filter filter : publishEvent.filters) {
            if (!(filter instanceof EvtTypeFilter)) {
                try {
                    final Filter filterFromQuery = QueryParameterParser.parseQueryParameter(filter.getClass(), evtTypeFilter.property.getQuery());
                    filterFromQuery.copyTo(filter);
                } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
                    LOGGER.atWarn().addArgument(evtTypeFilter.property).setCause(e).log("Error while parsing filters from endpoint: {}");
                }
            }
        }
    }

    protected DataSource getClient(final URI endpoint) {
        // N.B. protected method so that knowledgeable/courageous developer can define their own multiplexing 'key' map-criteria
        // e.g. a key including the volatile authority and/or a more specific 'device/property' path information, e.g.
        // key := "<scheme>://authority/path"  (N.B. usually the authority is resolved by the DnsResolver/any Broker)
        return clientMap.computeIfAbsent(endpoint.getScheme() + ":/" + getDeviceName(endpoint), requestedEndPoint -> {
            final DataSource dataSource = DataSource.getFactory(URI.create(requestedEndPoint)).newInstance(context, endpoint, Duration.ofMillis(100), executor, Long.toString(internalReqIdGenerator.incrementAndGet()));
            poller.register(dataSource.getSocket(), ZMQ.Poller.POLLIN);
            return dataSource;
        });
    }

    // TODO: alternative listener api similar to mdp worker: mdp-like(single lambda, domain objects + access to raw data + exception or update)
    public interface NotificationListener<R, C> {
        void dataUpdate(final R updatedObject, final C contextObject);
        void updateException(final Throwable exception);
    }

    public class Client implements Closeable {
        private final ZMQ.Socket clientSocket;
        private IoBuffer byteBuffer;
        private IoClassSerialiser ioClassSerialiser;

        private Client() { // accessed via outer class method
            clientSocket = context.createSocket(SocketType.DEALER);
            clientSocket.connect(inprocCtrl);
        }

        public <R, C> Future<R> get(URI endpoint, final C requestContext, final Class<R> requestedDomainObjType, final RbacProvider... rbacProvider) {
            final String requestId = clientId + internalReqIdGenerator.incrementAndGet();
            final URI endpointQuery = getEndpointQuery(endpoint, requestContext);
            final ThePromisedFuture<R, ?> rThePromisedFuture = newRequestFuture(endpointQuery, requestedDomainObjType, Command.GET_REQUEST, requestId);
            request(requestId, Command.GET_REQUEST, endpointQuery, null, requestContext, rbacProvider);
            return rThePromisedFuture;
        }

        public <R, C> Future<R> set(final URI endpoint, final R requestBody, final C requestContext, final Class<R> requestedDomainObjType, final RbacProvider... rbacProvider) {
            final String requestId = clientId + internalReqIdGenerator.incrementAndGet();
            final URI endpointQuery = getEndpointQuery(endpoint, requestContext);
            final ThePromisedFuture<R, ?> rThePromisedFuture = newRequestFuture(endpointQuery, requestedDomainObjType, Command.SET_REQUEST, requestId);
            request(requestId, Command.SET_REQUEST, endpointQuery, requestBody, requestContext, rbacProvider);
            return rThePromisedFuture;
        }

        public <T> String subscribe(final URI endpoint, final Class<T> requestedDomainObjType, final RbacProvider... rbacProvider) {
            return subscribe(endpoint, requestedDomainObjType, null, null, null, rbacProvider);
        }

        public <T, C> String subscribe(final URI endpoint, final Class<T> requestedDomainObjType, final C context, final Class<C> contextType, NotificationListener<T, C> listener, final RbacProvider... rbacProvider) {
            final String requestId = clientId + internalReqIdGenerator.incrementAndGet();
            final URI endpointQuery = getEndpointQuery(endpoint, context);
            final String reqId = newSubscriptionFuture(endpointQuery, requestedDomainObjType, contextType, requestId, listener).internalRequestID;
            request(requestId, Command.SUBSCRIBE, endpointQuery, null, context, rbacProvider);
            return reqId;
        }

        public void unsubscribe(String requestId) {
            // signal socket for get with endpoint and request id
            final ZMsg msg = new ZMsg();
            msg.add(Command.UNSUBSCRIBE.getData());
            msg.add(requestId);
            msg.add(requests.get(requestId).endpoint.toString());
            msg.send(clientSocket);
        }

        @Override
        public void close() {
            try {
                clientSocket.close();
            } catch (Exception e) { // NOPMD
                LOGGER.atError().setCause(e).addArgument(DataSourcePublisher.class.getSimpleName()).addArgument(clientId).log("error closing {} resources for clientID: '{}'");
            }
        }

        private IoClassSerialiser getSerialiser() {
            if (ioClassSerialiser == null) {
                byteBuffer = new FastByteBuffer(1024, true, null);
                ioClassSerialiser = new IoClassSerialiser(byteBuffer);
            }
            byteBuffer.reset();
            return ioClassSerialiser;
        }

        private <C> URI getEndpointQuery(URI endpoint, C requestContext) {
            URI endpointQuery = endpoint;
            if (requestContext != null) {
                try {
                    endpointQuery = QueryParameterParser.appendQueryParameter(endpoint, QueryParameterParser.generateQueryParameter(requestContext));
                } catch (URISyntaxException e) {
                    throw new IllegalArgumentException("Invalid URL syntax for endpoint", e);
                }
            }
            return endpointQuery;
        }

        private <R, C> void request(final String requestId, final Command requestType, final URI endpoint, R requestBody, C requestContext, final RbacProvider... rbacProvider) {
            FilterRegistry.checkClassForNewFilters(requestContext);
            // signal socket for get with endpoint and request id
            final ZMsg msg = new ZMsg();
            msg.add(requestType.getData());
            msg.add(requestId);
            msg.add(endpoint.toString());
            if (requestBody == null) {
                msg.add(EMPTY_ZFRAME);
            } else {
                final Class<? extends IoSerialiser> matchingSerialiser = DataSource.getFactory(endpoint).getMatchingSerialiserType(endpoint);
                final IoClassSerialiser serialiser = getSerialiser(); // lazily initialize IoClassSerialiser
                serialiser.setMatchedIoSerialiser(matchingSerialiser); // needs to be converted in DataSource impl
                serialiser.serialiseObject(requestBody);
                msg.add(Arrays.copyOfRange(byteBuffer.elements(), 0, byteBuffer.position()));
            }
            // RBAC
            if (rbacProvider.length > 0 || DataSourcePublisher.this.rbacProvider != null) {
                // final RbacProvider rbac = rbacProvider.length > 0 ? rbacProvider[0] : DataSourcePublisher.this.rbacProvider // NOPMD - future use
                // rbac.sign(msg); // todo: sign message and add rbac token and signature
                LOGGER.atWarn().log("RbacProvider not yet implemented");
            } else {
                msg.add(EMPTY_ZFRAME);
            }

            if (!msg.send(clientSocket)) {
                LOGGER.atWarn().addArgument(requestType).addArgument(msg).log("could not send {} from client to source - message: {}");
            }
        }
    }

    protected static class ThePromisedFuture<R, C> extends CustomFuture<R> { // NOPMD - no need for setters/getters here
        private final URI endpoint;
        private final Class<R> requestedDomainObjType;
        private final Class<C> contextType;
        private final Command requestType;
        private final String internalRequestID;
        private final NotificationListener<R, C> listener;

        public ThePromisedFuture(final URI endpoint, final Class<R> requestedDomainObjType, final Class<C> contextType, final Command requestType, final String internalRequestID, final NotificationListener<R, C> listener) {
            super();
            this.endpoint = endpoint;
            this.requestedDomainObjType = requestedDomainObjType;
            this.contextType = contextType;
            this.requestType = requestType;
            this.internalRequestID = internalRequestID;
            this.listener = listener;
        }

        public URI getEndpoint() {
            return endpoint;
        }

        public Command getReplyType() {
            return requestType;
        }

        public Class<R> getRequestedDomainObjType() {
            return requestedDomainObjType;
        }

        public String getInternalRequestID() {
            return internalRequestID;
        }

        public void notifyListener(final Object obj, final Object contextObject) {
            if (obj == null || !requestedDomainObjType.isAssignableFrom(obj.getClass()) || !contextType.isAssignableFrom(contextObject.getClass())) {
                LOGGER.atError().addArgument(requestedDomainObjType.getName()).addArgument(obj == null ? "null" : obj.getClass().getName()).log("Got wrong type for notification, got {} expected {}");
            } else {
                //noinspection unchecked - cast is checked dynamically
                listener.dataUpdate((R) obj, (C) contextObject); // NOPMD NOSONAR - cast is checked before implicitly
            }
        }

        @SuppressWarnings("unchecked")
        protected void castAndSetReply(final Object newValue) {
            this.setReply((R) newValue);
        }
    }

    protected static class InternalDomainObject {
        public final ZMsg data;
        public final ThePromisedFuture<?, ?> future;

        protected InternalDomainObject(final ZMsg data, final ThePromisedFuture<?, ?> future) {
            this.data = requireNonNull(data, "null data");
            this.future = requireNonNull(future, "null future");
        }

        @Override
        public String toString() {
            return "InternalDomainObject{data=" + data + ", future=" + future + '}';
        }
    }
}
