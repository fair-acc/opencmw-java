package io.opencmw.client;

import static java.util.Objects.requireNonNull;

import static io.opencmw.OpenCmwConstants.*;
import static io.opencmw.OpenCmwConstants.HEARTBEAT_DEFAULT;
import static io.opencmw.client.DataSourceFilter.ReplyType.GET;
import static io.opencmw.client.DataSourceFilter.ReplyType.SET;
import static io.opencmw.client.DataSourceFilter.ReplyType.SUBSCRIBE;

import java.io.Closeable;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import io.opencmw.EventStore;
import io.opencmw.Filter;
import io.opencmw.QueryParameterParser;
import io.opencmw.RingBufferEvent;
import io.opencmw.client.cmwlight.CmwLightDataSource;
import io.opencmw.client.rest.RestDataSource;
import io.opencmw.filter.EvtTypeFilter;
import io.opencmw.rbac.RbacProvider;
import io.opencmw.serialiser.IoBuffer;
import io.opencmw.serialiser.IoClassSerialiser;
import io.opencmw.serialiser.IoSerialiser;
import io.opencmw.serialiser.spi.FastByteBuffer;
import io.opencmw.utils.CustomFuture;
import io.opencmw.utils.SharedPointer;
import io.opencmw.utils.SystemProperties;

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
public class DataSourcePublisher implements Runnable, Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataSourcePublisher.class);
    public static final int MIN_FRAMES_INTERNAL_MSG = 3;
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    private static final ZFrame EMPTY_FRAME = new ZFrame(EMPTY_BYTE_ARRAY);
    private static final AtomicInteger INSTANCE_COUNT = new AtomicInteger();
    private final String inprocCtrl = "inproc://dsPublisher#" + INSTANCE_COUNT.incrementAndGet();
    private final Map<String, ThePromisedFuture<?, ?>> requests = new ConcurrentHashMap<>(); // <requestId, future for the get request>
    private final Map<String, DataSource> clientMap = new ConcurrentHashMap<>(); // scheme://authority -> DataSource
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicInteger internalReqIdGenerator = new AtomicInteger(0);
    protected final long heartbeatInterval = SystemProperties.getValueIgnoreCase(HEARTBEAT, HEARTBEAT_DEFAULT); // [ms] time between to heartbeats in ms
    private final EventStore rawDataEventStore;
    private final ZContext context = new ZContext(SystemProperties.getValueIgnoreCase(N_IO_THREADS, N_IO_THREADS_DEFAULT));
    private final ZMQ.Poller poller;
    private final ZMQ.Socket controlSocket;
    private final IoBuffer byteBuffer = new FastByteBuffer(0, true, null); // never actually used
    private final IoClassSerialiser ioClassSerialiser = new IoClassSerialiser(byteBuffer);
    private final String clientId;
    private final RbacProvider rbacProvider;
    private final ExecutorService executor; // NOPMD - threads are ok, not a webapp
    private EventStore publicationTarget;

    static { // register default data sources
        DataSource.register(CmwLightDataSource.FACTORY);
        DataSource.register(RestDataSource.FACTORY);
        DataSource.register(OpenCmwDataSource.FACTORY);
    }

    public DataSourcePublisher(final RbacProvider rbacProvider, final ExecutorService executorService, final String... clientId) {
        this.executor = executorService == null ? Executors.newCachedThreadPool() : executorService;
        poller = context.createPoller(1);
        // control socket for adding subscriptions / triggering requests from other threads
        controlSocket = context.createSocket(SocketType.DEALER);
        controlSocket.bind(inprocCtrl);
        poller.register(controlSocket, ZMQ.Poller.POLLIN);
        // instantiate internal event store for raw messages
        rawDataEventStore = EventStore.getFactory().setSingleProducer(true).setFilterConfig(DataSourceFilter.class).build();
        // setup clientId to identify subscribers from server
        this.clientId = clientId.length == 1 ? clientId[0] : DataSourcePublisher.class.getName();
        this.rbacProvider = rbacProvider;
        // setup internal event handler to deserialise updates and republish them or notify listeners/futures
        rawDataEventStore.register(this::internalEventHandler);
    }

    public DataSourcePublisher(final EventStore publicationTarget, final RbacProvider rbacProvider, final ExecutorService executorService, final String... clientId) {
        this(rbacProvider, executorService, clientId);
        // register external event store
        this.publicationTarget = publicationTarget;
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
        context.destroy();
    }

    public class Client implements Closeable {
        private final ZMQ.Socket controlSocket;
        private IoBuffer byteBuffer;
        private IoClassSerialiser ioClassSerialiser;

        private Client() { // accessed via outer class method
            controlSocket = context.createSocket(SocketType.DEALER);
            controlSocket.connect(inprocCtrl);
        }

        private IoClassSerialiser getSerialiser() {
            if (ioClassSerialiser == null) {
                byteBuffer = new FastByteBuffer(1024, true, null);
                ioClassSerialiser = new IoClassSerialiser(byteBuffer);
            }
            byteBuffer.reset();
            return ioClassSerialiser;
        }

        public <R, C> Future<R> get(URI endpoint, final C requestContext, final Class<R> requestedDomainObjType, final RbacProvider... rbacProvider) {
            final String requestId = clientId + internalReqIdGenerator.incrementAndGet();
            final URI endpointQuery = request(requestId, GET, endpoint, null, requestContext, rbacProvider);
            return newRequestFuture(endpointQuery, requestedDomainObjType, GET, requestId);
        }

        public <R, C> Future<R> set(final URI endpoint, final R requestBody, final C requestContext, final Class<R> requestedDomainObjType, final RbacProvider... rbacProvider) {
            final String requestId = clientId + internalReqIdGenerator.incrementAndGet();
            final URI endpointQuery = request(requestId, SET, endpoint, requestBody, requestContext, rbacProvider);
            return newRequestFuture(endpointQuery, requestedDomainObjType, SET, requestId);
        }

        public <T> String subscribe(final URI endpoint, final Class<T> requestedDomainObjType, final RbacProvider... rbacProvider) {
            return subscribe(endpoint, requestedDomainObjType, null, null, null, rbacProvider);
        }

        public <T, C> String subscribe(final URI endpoint, final Class<T> requestedDomainObjType, final C context, final Class<C> contextType, NotificationListener<T, C> listener, final RbacProvider... rbacProvider) {
            final String requestId = clientId + internalReqIdGenerator.incrementAndGet();
            final URI endpointQuery = request(requestId, SUBSCRIBE, endpoint, null, context, rbacProvider);
            return newSubscriptionFuture(endpointQuery, requestedDomainObjType, contextType, requestId, listener).internalRequestID;
        }

        public void unsubscribe(String requestId) {
            // signal socket for get with endpoint and request id
            final ZMsg msg = new ZMsg();
            msg.add(new byte[] { DataSourceFilter.ReplyType.UNSUBSCRIBE.getID() });
            msg.add(requestId);
            msg.add(requests.get(requestId).endpoint.toString());
            msg.send(controlSocket);
        }

        private <R, C> URI request(final String requestId, final DataSourceFilter.ReplyType replyType, final URI endpoint, R requestBody, C requestContext, final RbacProvider... rbacProvider) {
            URI endpointQuery = endpoint;
            if (requestContext != null) {
                try {
                    endpointQuery = QueryParameterParser.appendQueryParameter(endpoint, QueryParameterParser.generateQueryParameter(requestContext));
                } catch (URISyntaxException e) {
                    throw new IllegalArgumentException("Invalid URL syntax for endpoint", e);
                }
            }
            // signal socket for get with endpoint and request id
            final ZMsg msg = new ZMsg();
            msg.add(new byte[] { replyType.getID() });
            msg.add(requestId);
            msg.add(endpointQuery.toString());
            if (requestBody == null) {
                msg.add(EMPTY_FRAME);
            } else {
                final Class<? extends IoSerialiser> matchingSerialiser = DataSource.getFactory(endpoint).getMatchingSerialiserType(endpoint);
                final IoClassSerialiser serialiser = getSerialiser(); // lazily initialize IoClassSerialiser
                serialiser.setMatchedIoSerialiser(matchingSerialiser); // needs to be converted in DataSource impl
                serialiser.serialiseObject(requestBody);
                msg.add(Arrays.copyOfRange(byteBuffer.elements(), 0, byteBuffer.position()));
            }
            // RBAC
            if (rbacProvider.length > 0 || DataSourcePublisher.this.rbacProvider != null) {
                final RbacProvider rbac = rbacProvider.length > 0 ? rbacProvider[0] : DataSourcePublisher.this.rbacProvider; // NOPMD - future use
                // rbac.sign(msg); // todo: sign message and add rbac token and signature
            } else {
                msg.add(EMPTY_FRAME);
            }
            msg.send(controlSocket);

            return endpointQuery;
        }

        @Override
        public void close() {
            controlSocket.close();
        }
    }

    public void start() {
        new Thread(null, this, "DataSourceProducerThread-" + clientId, 0).start(); // NOPMD - not a webapp
    }

    public void stop() {
        running.set(false);
        close();
    }

    @Override
    public void run() {
        if (running.getAndSet(true)) {
            return; // is already running
        }
        // start the ring buffer and its processors
        rawDataEventStore.start();
        // event loop polling all data sources and performing regular housekeeping jobs
        long nextHousekeeping = System.currentTimeMillis(); // immediately perform first housekeeping
        long tout = 0L;
        while (!Thread.interrupted() && running.get() && (tout <= 0 || -1 != poller.poll(tout))) {
            boolean dataAvailable = true;
            while (dataAvailable && System.currentTimeMillis() < nextHousekeeping && running.get()) {
                dataAvailable = handleDataSourceSockets(); // get data from clients
                dataAvailable |= handleControlSocket(); // check specifically for control socket
            }
            nextHousekeeping = clientMap.values().stream().mapToLong(DataSource::housekeeping).min().orElse(System.currentTimeMillis() + heartbeatInterval);
            tout = nextHousekeeping - System.currentTimeMillis();
        }
        if (running.get()) {
            LOGGER.atError().addArgument(clientMap.values()).log("poller returned negative value - abort run() - clients = {}");
        } else {
            LOGGER.atDebug().log("Shutting down DataSourcePublisher");
        }
        rawDataEventStore.stop();
    }

    protected boolean handleControlSocket() {
        final ZMsg controlMsg = ZMsg.recvMsg(controlSocket, false);
        if (controlMsg == null) {
            return false; // no more data available on control socket
        }
        if (controlMsg.size() < MIN_FRAMES_INTERNAL_MSG) { // msgType, requestId and endpoint have to be always present
            LOGGER.atDebug().log("ignoring invalid message");
            return true; // ignore invalid partial message
        }
        final DataSourceFilter.ReplyType msgType = DataSourceFilter.ReplyType.valueOf(controlMsg.pollFirst().getData()[0]);
        final String requestId = requireNonNull(controlMsg.pollFirst()).getString(Charset.defaultCharset());
        final String endpoint = requireNonNull(controlMsg.pollFirst()).getString(Charset.defaultCharset());
        final byte[] data = controlMsg.isEmpty() ? EMPTY_BYTE_ARRAY : controlMsg.pollFirst().getData();
        final byte[] rbacToken = controlMsg.isEmpty() ? EMPTY_BYTE_ARRAY : controlMsg.pollFirst().getData();

        final URI ep = URI.create(endpoint);
        final DataSource client = getClient(ep); // get client for endpoint
        switch (msgType) {
        case SUBSCRIBE: // subscribe: 0b, requestId, addr/dev/prop?sel&filters
            client.subscribe(requestId, ep, rbacToken);
            break;
        case GET: // 1b, reqId, endpoint
            client.get(requestId, ep, data, rbacToken);
            break;
        case SET: // 2b, reqId, endpoint, data
            client.set(requestId, ep, data, rbacToken);
            break;
        case UNSUBSCRIBE: // 3b, reqId, endpoint
            client.unsubscribe(requestId);
            requests.remove(requestId);
            break;
        case UNKNOWN:
        default:
            throw new UnsupportedOperationException("Illegal operation type");
        }
        return true;
    }

    protected boolean handleDataSourceSockets() {
        boolean dataAvailable = false;
        for (DataSource entry : clientMap.values()) {
            final ZMsg reply = entry.getMessage();
            if (reply == null) {
                continue; // no data received, queue empty
            }
            dataAvailable = true;
            if (reply.isEmpty()) {
                continue; // there was data received, but only used for internal state of the client
            }
            // the received data consists of the following frames:
            // replyType(byte), reqId(string), endpoint(string), dataBody(byte[])
            rawDataEventStore.getRingBuffer().publishEvent((event, sequence) -> {
                final String reqId = requireNonNull(reply.pollFirst()).getString(Charset.defaultCharset());
                final ThePromisedFuture<?, ?> returnFuture = requests.get(reqId);
                if (returnFuture.getReplyType() != SUBSCRIBE) { // remove entries for one time replies
                    assert returnFuture.getInternalRequestID().equals(reqId)
                        : "requestID mismatch";
                    requests.remove(reqId);
                }
                final String endpoint = requireNonNull(reply.pollFirst()).getString(Charset.defaultCharset());
                event.arrivalTimeStamp = System.currentTimeMillis();
                event.payload = new SharedPointer<>(); // NOPMD - need to create new shared pointer instance
                event.payload.set(reply); // ZMsg containing header, body and exception frame
                final DataSourceFilter dataSourceFilter = event.getFilter(DataSourceFilter.class);
                dataSourceFilter.future = returnFuture;
                dataSourceFilter.eventType = returnFuture.replyType;
                dataSourceFilter.endpoint = endpoint;
                dataSourceFilter.arrivalTimestamp = event.arrivalTimeStamp;
            });
        }
        return dataAvailable;
    }

    private void internalEventHandler(final RingBufferEvent event, final long sequence, final boolean endOfBatch) {
        final DataSourceFilter dataSourceFilter = event.getFilter(DataSourceFilter.class);
        final ThePromisedFuture<?, ?> future = dataSourceFilter.future;
        final URI endpointURI = URI.create(dataSourceFilter.endpoint);
        if (future.replyType == SUBSCRIBE || future.replyType == DataSourceFilter.ReplyType.GET || future.replyType == DataSourceFilter.ReplyType.SET) {
            // get data from socket
            final ZMsg cmwMsg = event.payload.get(ZMsg.class);
            final byte[] body = requireNonNull(cmwMsg.poll()).getData();
            String exc = requireNonNull(cmwMsg.poll()).getString(Charset.defaultCharset());
            Object domainObj = null;
            final boolean notifyFuture = future.replyType == DataSourceFilter.ReplyType.GET || future.replyType == DataSourceFilter.ReplyType.SET;
            if (exc == null || exc.isBlank()) {
                try {
                    if (body != null && body.length != 0) {
                        ioClassSerialiser.setDataBuffer(FastByteBuffer.wrap(body));
                        domainObj = ioClassSerialiser.deserialiseObject(future.getRequestedDomainObjType());
                        ioClassSerialiser.setDataBuffer(byteBuffer); // allow received byte array to be released
                    }
                    if (notifyFuture) {
                        future.castAndSetReply(domainObj); // notify callback
                    }
                    if (future.listener != null) {
                        final Object finalDomainObj = domainObj;
                        final Object contextObject;
                        if (future.contextType == null) {
                            contextObject = QueryParameterParser.getMap(endpointURI.getQuery());
                        } else {
                            contextObject = QueryParameterParser.parseQueryParameter(future.contextType, endpointURI.getQuery());
                        }
                        executor.submit(() -> future.notifyListener(finalDomainObj, contextObject)); // NOPMD - threads are ok, not a webapp
                    }
                } catch (Exception e) {
                    if (notifyFuture) {
                        exc = "Error deserialising object: " + e.getMessage();
                        future.setException(e);
                    } else {
                        executor.submit(() -> future.listener.updateException(e)); // NOPMD - threads are ok, not a webapp
                    }
                }
            } else if (notifyFuture) {
                future.setException(new Exception(exc)); // notify exception callback
            } else {
                final String finalExc = exc;
                executor.submit(() -> future.listener.updateException(new Exception(finalExc))); // NOPMD - threads are ok, not a webapp
            }
            // publish to external ring buffer
            if (publicationTarget != null) {
                publicationTarget.getRingBuffer().publishEvent(this::publishToExternalStore, domainObj, exc, dataSourceFilter);
            }
        } else {
            // ignore other reply types for now todo: publish statistics, connection state and getRequests
            LOGGER.atInfo().addArgument(event.payload.get()).log("{}");
        }
    }

    private void publishToExternalStore(final RingBufferEvent publishEvent,
            final long seq,
            final Object obj,
            final String exception,
            final DataSourceFilter dataSourceFilter) {
        publishEvent.arrivalTimeStamp = dataSourceFilter.arrivalTimestamp;
        if (exception != null && !exception.isBlank()) {
            publishEvent.throwables.add(new Exception(exception));
        }
        publishEvent.payload = new SharedPointer<>();
        if (obj != null) {
            publishEvent.payload.set(obj);
        }
        for (final Filter filter : publishEvent.filters) {
            if (filter instanceof EvtTypeFilter) {
                ((EvtTypeFilter) filter).evtType = EvtTypeFilter.DataType.DEVICE_DATA;
                ((EvtTypeFilter) filter).updateType = EvtTypeFilter.UpdateType.COMPLETE;
                ((EvtTypeFilter) filter).typeName = dataSourceFilter.endpoint;
            } else {
                final URI endpoint = URI.create(dataSourceFilter.endpoint);
                try {
                    final Filter filterFromQuery = QueryParameterParser.parseQueryParameter(filter.getClass(), endpoint.getQuery());
                    filterFromQuery.copyTo(filter);
                } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
                    LOGGER.atWarn().addArgument(endpoint).setCause(e).log("Error while parsing filters from endpoint: {}");
                }
            }
        }
    }

    private DataSource getClient(final URI endpoint) {
        return clientMap.computeIfAbsent(endpoint.getScheme() + "://" + endpoint.getAuthority(), requestedEndPoint -> {
            final DataSource dataSource = DataSource.getFactory(URI.create(requestedEndPoint)).newInstance(context, endpoint, Duration.ofMillis(100), Long.toString(internalReqIdGenerator.incrementAndGet()));
            poller.register(dataSource.getSocket(), ZMQ.Poller.POLLIN);
            return dataSource;
        });
    }

    protected <R> ThePromisedFuture<R, ?> newRequestFuture(final URI endpoint,
            final Class<R> requestedDomainObjType,
            final DataSourceFilter.ReplyType replyType,
            final String requestId) {
        final ThePromisedFuture<R, ?> requestFuture = new ThePromisedFuture<>(endpoint, requestedDomainObjType, null, replyType, requestId, null);
        final Object oldEntry = requests.put(requestId, requestFuture);
        assert oldEntry == null : "requestID '" + requestId + "' already present in requestFutureMap";
        return requestFuture;
    }

    protected <R, C> ThePromisedFuture<R, C> newSubscriptionFuture(final URI endpoint,
            final Class<R> requestedDomainObjType,
            final Class<C> contextType,
            final String requestId,
            final NotificationListener<R, C> listener) {
        final ThePromisedFuture<R, C> requestFuture = new ThePromisedFuture<>(endpoint, requestedDomainObjType, contextType, SUBSCRIBE, requestId, listener);
        final Object oldEntry = requests.put(requestId, requestFuture);
        assert oldEntry == null : "requestID '" + requestId + "' already present in requestFutureMap";
        return requestFuture;
    }

    protected static class ThePromisedFuture<R, C> extends CustomFuture<R> { // NOPMD - no need for setters/getters here
        private final URI endpoint;
        private final Class<R> requestedDomainObjType;
        private final Class<C> contextType;
        private final DataSourceFilter.ReplyType replyType;
        private final String internalRequestID;
        private final NotificationListener<R, C> listener;

        public ThePromisedFuture(final URI endpoint,
                final Class<R> requestedDomainObjType,
                final Class<C> contextType,
                final DataSourceFilter.ReplyType replyType,
                final String internalRequestID,
                final NotificationListener<R, C> listener) {
            super();
            this.endpoint = endpoint;
            this.requestedDomainObjType = requestedDomainObjType;
            this.contextType = contextType;
            this.replyType = replyType;
            this.internalRequestID = internalRequestID;
            this.listener = listener;
        }

        public URI getEndpoint() {
            return endpoint;
        }

        public DataSourceFilter.ReplyType getReplyType() {
            return replyType;
        }

        public Class<R> getRequestedDomainObjType() {
            return requestedDomainObjType;
        }

        @SuppressWarnings("unchecked")
        protected void castAndSetReply(final Object newValue) {
            this.setReply((R) newValue);
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
    }

    // TODO: alternative listener api similar to mdp worker: mdp-like(single lambda, domain objects + access to raw data + exception or update)
    public interface NotificationListener<R, C> {
        void dataUpdate(final R updatedObject, final C contextObject);
        void updateException(final Throwable exception);
    }
}
