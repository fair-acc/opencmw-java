package de.gsi.microservice.datasource;

import java.nio.charset.Charset;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import de.gsi.microservice.datasource.cmwlight.CmwLightClient;
import de.gsi.microservice.datasource.rest.RestDataSource;
import de.gsi.serializer.IoBuffer;
import de.gsi.serializer.IoSerialiser;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.*;

import com.lmax.disruptor.EventHandler;

import de.gsi.microservice.EventStore;
import de.gsi.microservice.RingBufferEvent;
import de.gsi.microservice.filter.*;
import de.gsi.microservice.utils.SharedPointer;
import de.gsi.serializer.IoClassSerialiser;
import de.gsi.serializer.spi.FastByteBuffer;

/**
 * Publishes events from different sources into a common {@link EventStore} and takes care of setting the appropriate
 * filters and deserialisation of the domain objects.
 *
 * The subscribe/unsubscribe/set/get methods can be called from any thread and are decoupled from the actual
 *
 * @author Alexander Krimmm
 * @author rstein
 */
public class DataSourcePublisher implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataSourcePublisher.class);
    private static final int CONTROL_SOCKET_INDEX = 0;
    private static final AtomicInteger INSTANCE_COUNT = new AtomicInteger();
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    private static final ZFrame EMPTY_FRAME = new ZFrame(EMPTY_BYTE_ARRAY);
    private final String inprocCtrl = "inproc://dsPublisher#" + INSTANCE_COUNT.incrementAndGet();
    protected final Map<String, ThePromisedFuture<?>> requestFutureMap = new ConcurrentHashMap<>(); // <requestId, future for the get request>
    protected final Map<String, DataSource> clientMap = new HashMap<>(); // <endpoint, DataSource>
    protected final List<DataSource> clients = new ArrayList<>();
    protected final List<DataSource> clientsThreadsafe = Collections.synchronizedList(new ArrayList<>());

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicInteger internalReqIdGenerator = new AtomicInteger(0);
    private final EventStore eventStore;
    private final ZMQ.Poller poller;
    private final ZContext context = new ZContext(1);
    private final ZMQ.Socket controlSocket;
    private final IoBuffer byteBuffer = new FastByteBuffer(2000); // zero length buffer to use when there is no data to deserialise
    private final IoClassSerialiser ioClassSerialiser = new IoClassSerialiser(byteBuffer);

    private final ThreadLocal<ZMQ.Socket> perThreadControlSocket = new ThreadLocal<>() { // creates a client control socket for each calling thread
        private ZMQ.Socket result;

        @Override
        public void remove() {
            if (result != null) {
                result.disconnect(inprocCtrl);
            }
            super.remove();
        }

        @Override
        protected ZMQ.Socket initialValue() {
            result = context.createSocket(SocketType.DEALER);
            result.connect(inprocCtrl);
            return result;
        }
    };
    private final String clientId;
    private final RbacProvider rbacProvider;

    public DataSourcePublisher(final RbacProvider rbacProvider, final EventStore publicationTarget, final String... clientId) {
        this(rbacProvider, clientId);
        eventStore.register((event, sequence, endOfBatch) -> {
            final DataSourceFilter dataSourceFilter = event.getFilter(DataSourceFilter.class);
            if (dataSourceFilter.eventType.equals(DataSourceFilter.ReplyType.SUBSCRIBE)) {
                final ThePromisedFuture<?> future = dataSourceFilter.future;
                if (future.replyType == DataSourceFilter.ReplyType.SUBSCRIBE) {
                    final Class<?> domainClass = future.getRequestedDomainObjType();
                    final ZMsg cmwMsg = event.payload.get(ZMsg.class);
                    final String header = cmwMsg.poll().getString(Charset.defaultCharset());
                    final byte[] body = cmwMsg.poll().getData();
                    final String exc = cmwMsg.poll().getString(Charset.defaultCharset());
                    Object domainObj = null;
                    if (body != null && body.length != 0) {
                        ioClassSerialiser.setDataBuffer(FastByteBuffer.wrap(body));
                        domainObj = ioClassSerialiser.deserialiseObject(domainClass);
                        ioClassSerialiser.setDataBuffer(byteBuffer); // allow received byte array to be released
                    }
                    publicationTarget.getRingBuffer().publishEvent((publishEvent, seq, obj, msg) -> {
                        final TimingCtx contextFilter = publishEvent.getFilter(TimingCtx.class);
                        final EvtTypeFilter evtTypeFilter = publishEvent.getFilter(EvtTypeFilter.class);
                        publishEvent.arrivalTimeStamp = event.arrivalTimeStamp;
                        publishEvent.payload = new SharedPointer<>();
                        publishEvent.payload.set(obj);
                        if (exc != null && !exc.isBlank()) {
                            publishEvent.throwables.add(new Exception(exc));
                        }
                        try {
                            contextFilter.setSelector(dataSourceFilter.context, 0);
                        } catch (Exception e) {
                            LOGGER.atError().setCause(e).addArgument(dataSourceFilter.context).log("No valid context: {}");
                        }
                        // contextFilter.acqts = msg.dataContext.acqStamp; // needs to be added?
                        // contextFilter.ctxName = // what should go here?
                        evtTypeFilter.evtType = EvtTypeFilter.DataType.DEVICE_DATA;
                        evtTypeFilter.typeName = dataSourceFilter.device + '/' + dataSourceFilter.property;
                        evtTypeFilter.updateType = EvtTypeFilter.UpdateType.COMPLETE;
                    }, domainObj, cmwMsg);
                } else if (future.replyType == DataSourceFilter.ReplyType.GET) {
                    final ZMsg cmwMsg = event.payload.get(ZMsg.class);
                    final String header = cmwMsg.poll().getString(Charset.defaultCharset());
                    final byte[] body = cmwMsg.poll().getData();
                    final String exc = cmwMsg.poll().getString(Charset.defaultCharset());
                    ioClassSerialiser.setDataBuffer(FastByteBuffer.wrap(body));
                    final Object obj = ioClassSerialiser.deserialiseObject(future.getRequestedDomainObjType());
                    // todo: set exception for future
                    future.setReply(future.getRequestedDomainObjType().cast(obj));
                    ioClassSerialiser.setDataBuffer(byteBuffer); // allow received byte array to be released
                } else {
                    // ignore other reply types for now
                }
            } else {
                // todo: publish statistics, connection state and getRequests
                LOGGER.atInfo().addArgument(event.payload.get()).log("{}");
            }
        });
    }

    public DataSourcePublisher(final RbacProvider rbacProvider, final EventHandler<RingBufferEvent> eventHandler, final String... clientId) {
        this(rbacProvider, clientId);
        eventStore.register(eventHandler);
    }

    public DataSourcePublisher(final RbacProvider rbacProvider, final String... clientId) {
        poller = context.createPoller(1);
        // control socket for adding subscriptions / triggering requests from other threads
        controlSocket = context.createSocket(SocketType.DEALER);
        controlSocket.bind(inprocCtrl);
        poller.register(controlSocket, ZMQ.Poller.POLLIN);
        // instantiate event store
        eventStore = EventStore.getFactory().setSingleProducer(true).setFilterConfig(DataSourceFilter.class).build();
        // register default handlers
        DataSource.register(CmwLightClient.FACTORY);
        DataSource.register(RestDataSource.FACTORY);
        this.clientId = clientId.length == 1 ? clientId[0] : DataSourcePublisher.class.getName();
        this.rbacProvider = rbacProvider;
    }

    public ZContext getContext() {
        return context;
    }

    public EventStore getEventStore() {
        return eventStore;
    }

    public <R> Future<R> get(String endpoint, final Class<R> requestedDomainObjType, final RbacProvider... rbacProvider) {
        return get(endpoint, null, null, requestedDomainObjType, rbacProvider);
    }

    /**
     * Perform an asynchronous get request on the given device/property.
     * Checks if a client for this service already exists and if it does performs the asynchronous get on it, otherwise
     * it starts a new client and performs it there.
     *
     * @param endpoint endpoint address for the property e.g. 'rda3://hostname:port/property?selector&amp;filter', file:///path/to/directory, mdp://host:port
     * @param requestFilter optional map of optional filters e.g. Map.of("channelName", "VoltageChannel")
     * @param requestBody optional domain object payload to be send with the request
     * @param requestedDomainObjType the requested result domain object type
     * @param <R> The type of the deserialised requested result domain object
     * @return A future which will be able to retrieve the deserialised result
     */
    public <R> Future<R> get(String endpoint, final Map<String, Object> requestFilter, final Object requestBody, final Class<R> requestedDomainObjType, final RbacProvider... rbacProvider) {
        final String requestId = clientId + internalReqIdGenerator.incrementAndGet();
        final Future<R> requestFuture = newFuture(endpoint, requestFilter, requestBody, requestedDomainObjType, DataSourceFilter.ReplyType.GET, requestId);
        final Class<? extends IoSerialiser> matchingSerialiser = DataSource.getFactory(endpoint).getMatchingSerialiserType(endpoint);

        // signal socket for get with endpoint and request id
        final ZMsg msg = new ZMsg();
        msg.add(new byte[] { DataSourceFilter.ReplyType.GET.getID() });
        msg.add(requestId);
        msg.add(endpoint);
        if (requestFilter != null) {
            ioClassSerialiser.getDataBuffer().reset();
            ioClassSerialiser.setMatchedIoSerialiser(matchingSerialiser); // needs to be converted in DataSource impl
            ioClassSerialiser.serialiseObject(requestFilter);
            msg.add(Arrays.copyOfRange(ioClassSerialiser.getDataBuffer().elements(), 0, ioClassSerialiser.getDataBuffer().position()));
        } else {
            msg.add(EMPTY_FRAME);
        }
        if (requestBody != null) {
            ioClassSerialiser.getDataBuffer().reset();
            ioClassSerialiser.setMatchedIoSerialiser(matchingSerialiser); // needs to be converted in DataSource impl
            ioClassSerialiser.serialiseObject(requestBody);
            msg.add(Arrays.copyOfRange(ioClassSerialiser.getDataBuffer().elements(), 0, ioClassSerialiser.getDataBuffer().position()));
        } else {
            msg.add(EMPTY_FRAME);
        }
        // RBAC
        if (rbacProvider.length > 0 || this.rbacProvider != null) {
            final RbacProvider rbac = rbacProvider.length > 0 ? rbacProvider[0] : this.rbacProvider;
            // rbac.sign(msg);
            // todo: sign message and add rbac token and signature
        } else {
            msg.add(EMPTY_FRAME);
        }

        msg.send(perThreadControlSocket.get());
        //TODO: do we need the following 'remove()'
        perThreadControlSocket.remove();
        return requestFuture;
    }

    public <T> void set(final String endpoint, final Map<String, Object> requestFilter, final Class<T> dataType, final T domainObject, final RbacProvider... rbacProvider) {
        final String requestId = clientId + internalReqIdGenerator.incrementAndGet();
        final Class<? extends IoSerialiser> matchingSerialiser = DataSource.getFactory(endpoint).getMatchingSerialiserType(endpoint);
        ioClassSerialiser.getDataBuffer().reset();
        ioClassSerialiser.setMatchedIoSerialiser(matchingSerialiser);
        ioClassSerialiser.serialiseObject(domainObject);
        final byte[] requestBody = Arrays.copyOfRange(ioClassSerialiser.getDataBuffer().elements(), 0, ioClassSerialiser.getDataBuffer().position());
        newFuture(endpoint, requestFilter, requestBody, dataType, DataSourceFilter.ReplyType.GET, requestId);

        // signal socket for get with endpoint and request id
        final ZMsg msg = new ZMsg();
        msg.add(new byte[] { DataSourceFilter.ReplyType.SET.getID() });
        msg.add(requestId);
        msg.add(endpoint);
        if (requestFilter != null) {
            ioClassSerialiser.getDataBuffer().reset();
            ioClassSerialiser.setMatchedIoSerialiser(matchingSerialiser); // needs to be converted in DataSource impl
            ioClassSerialiser.serialiseObject(requestFilter);
            msg.add(Arrays.copyOfRange(ioClassSerialiser.getDataBuffer().elements(), 0, ioClassSerialiser.getDataBuffer().position()));
        } else {
            msg.add(EMPTY_FRAME);
        }
        if (requestBody != null) {
            msg.add(requestBody);
        } else {
            msg.add(EMPTY_FRAME); // todo: should this be an error, set without body doesn't seem to be useful
        }
        // RBAC
        if (rbacProvider.length > 0 || this.rbacProvider != null) {
            final RbacProvider rbac = rbacProvider.length > 0 ? rbacProvider[0] : this.rbacProvider;
            // rbac.sign(msg);
            // todo: sign message and add rbac token and signature
        } else {
            msg.add(EMPTY_FRAME);
        }

        msg.send(perThreadControlSocket.get());
        //TODO: do we need the following 'remove()'
        perThreadControlSocket.remove();
    }

    public <T> void subscribe(final String endpoint, final Class<T> requestedDomainObjType) {
        subscribe(endpoint, requestedDomainObjType, null, null);
    }

    public <R> void subscribe(final String endpoint, final Class<R> requestedDomainObjType, final Map<String, Object> requestFilter, final Object requestBody, final RbacProvider... rbacProvider) {
        final String requestId = clientId + internalReqIdGenerator.incrementAndGet();
        final Class<? extends IoSerialiser> matchingSerialiser = DataSource.getFactory(endpoint).getMatchingSerialiserType(endpoint);
        newFuture(endpoint, requestFilter, requestBody, requestedDomainObjType, DataSourceFilter.ReplyType.SUBSCRIBE, requestId);

        // signal socket for get with endpoint and request id
        final ZMsg msg = new ZMsg();
        msg.add(new byte[] { DataSourceFilter.ReplyType.SUBSCRIBE.getID() });
        msg.add(requestId);
        msg.add(endpoint);
        if (requestFilter != null) {
            ioClassSerialiser.getDataBuffer().reset();
            ioClassSerialiser.serialiseObject(requestFilter);
            msg.add(Arrays.copyOfRange(ioClassSerialiser.getDataBuffer().elements(), 0, ioClassSerialiser.getDataBuffer().position()));
        } else {
            msg.add(EMPTY_FRAME);
        }
        if (requestBody != null) {
            ioClassSerialiser.getDataBuffer().reset();
            ioClassSerialiser.setMatchedIoSerialiser(matchingSerialiser); // needs to be converted in DataSource impl
            ioClassSerialiser.serialiseObject(requestBody);
            msg.add(Arrays.copyOfRange(ioClassSerialiser.getDataBuffer().elements(), 0, ioClassSerialiser.getDataBuffer().position()));
        } else {
            msg.add(EMPTY_FRAME);
        }
        // RBAC
        if (rbacProvider.length > 0 || this.rbacProvider != null) {
            final RbacProvider rbac = rbacProvider.length > 0 ? rbacProvider[0] : this.rbacProvider;
            // rbac.sign(msg);
            // todo: sign message and add rbac token and signature
        } else {
            msg.add(EMPTY_FRAME);
        }

        msg.send(perThreadControlSocket.get());
        //TODO: do we need the following 'remove()'
        perThreadControlSocket.remove();
    }

    public void unsubscribe(String endpoint) {
        final String requestId = clientId + internalReqIdGenerator.incrementAndGet(); // should this use the req id from the subscription?
        // signal socket for get with endpoint and request id
        final ZMsg msg = new ZMsg();
        msg.add(new byte[] { DataSourceFilter.ReplyType.UNSUBSCRIBE.getID() });
        msg.add(requestId);
        msg.add(endpoint);
        msg.send(perThreadControlSocket.get());
        //TODO: do we need the following 'remove()'
        perThreadControlSocket.remove();
    }

    @Override
    public void run() {
        // start the ring buffer and its processors
        eventStore.start();
        // event loop polling all data sources and performing regular housekeeping jobs
        running.set(true);
        long nextHousekeeping = System.currentTimeMillis(); // immediately perform first housekeeping
        while (!Thread.interrupted() && running.get()) {
            final long tout = nextHousekeeping - System.currentTimeMillis();
            final int result = poller.poll(tout > 0 ? tout : 0);
            if (result < 0) {
                // loop needs to be terminated
                LOGGER.atDebug().addArgument(result).addArgument(clients).log("poller returned {} - abort run() - clients = {}");
                break;
            }

            if (result > 0) {
                // get data from clients
                boolean dataAvailable = true;
                while (dataAvailable && System.currentTimeMillis() < nextHousekeeping && running.get()) {
                    dataAvailable = handleDataSourceSockets();

                    // check specificaly for control socket
                    if (poller.pollin(CONTROL_SOCKET_INDEX)) {
                        dataAvailable |= handleControlSocket();
                    }
                }
            }

            nextHousekeeping = clients.stream().mapToLong(DataSource::housekeeping).min().orElse(System.currentTimeMillis() + 1000);
        }
    }

    public void start() {
        new Thread(this).start();
    }

    protected boolean handleControlSocket() {
        boolean dataAvailable = false;
        final ZMsg controlMsg = ZMsg.recvMsg(controlSocket, false);
        if (controlMsg.size() < 3) { // msgType, requestId and endpoint have to be always present
            LOGGER.atDebug().log("ignoring invalid message");
            return true; // ignore invalid partial message
        }
        final DataSourceFilter.ReplyType msgType = DataSourceFilter.ReplyType.valueOf(controlMsg.pollFirst().getData()[0]);
        final String requestId = controlMsg.pollFirst().getString(Charset.defaultCharset());
        final String endpoint = controlMsg.pollFirst().getString(Charset.defaultCharset());
        final byte[] filters = controlMsg.isEmpty() ? EMPTY_BYTE_ARRAY : controlMsg.pollFirst().getData();
        final byte[] data = controlMsg.isEmpty() ? EMPTY_BYTE_ARRAY : controlMsg.pollFirst().getData();
        final byte[] rbacToken = controlMsg.isEmpty() ? EMPTY_BYTE_ARRAY : controlMsg.pollFirst().getData();

        final DataSource client = getClient(endpoint, filters); // get client for endpoint
        switch (msgType) {
            case SUBSCRIBE: // subscribe: 0b, requestId, addr/dev/prop?sel&filters, [filter]
                client.subscribe(requestId, rbacToken); // issue get request
                break;
            case GET: // get: 1b, reqId, addr/dev/prop?sel&filters, [filter]
                client.get(requestId, endpoint, filters, data, rbacToken); // issue get request
                break;
            case SET: // set: 2b, reqId, addr/dev/prop?sel&filters, data, add data to blocking queue instead?
                client.set(requestId, endpoint, filters, data, rbacToken);
                break;
            case UNSUBSCRIBE: //unsub: 3b, reqId, endpoint
                client.unsubscribe();
                requestFutureMap.remove(requestId);
                break;
            case UNKNOWN:
            default:
                throw new UnsupportedOperationException("Illegal operation type");
        }
        return dataAvailable;
    }

    protected boolean handleDataSourceSockets() {
        boolean dataAvailable = false;
        for (DataSource entry : clients) {
            final ZMsg reply = entry.getMessage();
            if (reply == null) {
                continue; // no data received, queue empty
            }
            dataAvailable = true;
            if (reply.isEmpty()) {
                continue; // there was data received, but only used for internal state of the client
            }
            // the received data consists of the following frames: replyType(byte), reqId(string), endpoint(string), dataBody(byte[])
            eventStore.getRingBuffer().publishEvent((event, sequence) -> {
                final String reqId = reply.pollFirst().getString(Charset.defaultCharset());
                final ThePromisedFuture<?> returnFuture = requestFutureMap.get(reqId);
                if (returnFuture.getReplyType() != DataSourceFilter.ReplyType.SUBSCRIBE) { // remove entries for one time replies
                    assert returnFuture.getInternalRequestID().equals(reqId) : "requestID mismatch";
                    requestFutureMap.remove(reqId);
                }
                final Endpoint endpoint = new Endpoint(reply.pollFirst().getString(Charset.defaultCharset()));
                event.arrivalTimeStamp = System.currentTimeMillis();
                event.payload = new SharedPointer<>();
                event.payload.set(reply); // ZMsg containing header, body and exception frame
                final DataSourceFilter dataSourceFilter = event.getFilter(DataSourceFilter.class);
                dataSourceFilter.future = returnFuture;
                dataSourceFilter.eventType = DataSourceFilter.ReplyType.SUBSCRIBE;
                dataSourceFilter.device = endpoint.getDevice();
                dataSourceFilter.property = endpoint.getProperty();
                dataSourceFilter.context = endpoint.getSelector();
            });
        }
        return dataAvailable;
    }

    @NotNull
    protected <R> ThePromisedFuture<R> newFuture(final String endpoint, final Map<String, Object> requestFilter, final Object requestBody, final Class<R> requestedDomainObjType, final DataSourceFilter.ReplyType replyType, final String requestId) {
        final ThePromisedFuture<R> requestFuture = new ThePromisedFuture<>(endpoint, requestFilter, requestBody, requestedDomainObjType, replyType, requestId);
        final Object oldEntry = requestFutureMap.put(requestId, requestFuture);
        assert oldEntry == null : "requestID '" + requestId + "' already present in requestFutureMap";
        return requestFuture;
    }

    private DataSource getClient(final String endpoint, final byte[] filters) {
        return clientMap.computeIfAbsent(endpoint, requestedEndPoint -> {
            final DataSource dataSource = DataSource.getFactory(requestedEndPoint).newInstance(context, requestedEndPoint, Duration.ofMillis(100), Long.toString(internalReqIdGenerator.incrementAndGet()), filters);
            clients.add(dataSource);
            clientsThreadsafe.add(dataSource);
            poller.register(dataSource.getSocket(), ZMQ.Poller.POLLIN);
            return dataSource;
        });
    }



    public static class ThePromisedFuture<R> implements Future<R> {
        private final Lock lock = new ReentrantLock();
        private final Condition processorNotifyCondition = lock.newCondition();
        private final AtomicBoolean running = new AtomicBoolean(true);
        private final AtomicBoolean requestCancel = new AtomicBoolean(false);
        private final AtomicBoolean cancelled = new AtomicBoolean(false);
        private final String endpoint;
        private final Map<String, Object> requestFilter;
        private final Object requestBody;
        private final Class<R> requestedDomainObjType;
        private final DataSourceFilter.ReplyType replyType;
        private final String internalRequestID;
        private R reply = null;

        ThePromisedFuture(final String endpoint, final Map<String, Object> requestFilter, final Object requestBody, final Class<R> requestedDomainObjType, final DataSourceFilter.ReplyType replyType, final String internalRequestID) {
            this.endpoint = endpoint;
            this.requestFilter = requestFilter;
            this.requestBody = requestBody;
            this.requestedDomainObjType = requestedDomainObjType;
            this.replyType = replyType;
            this.internalRequestID = internalRequestID;
        }

        @Override
        public boolean cancel(final boolean mayInterruptIfRunning) {
            if (running.getAndSet(false)) {
                cancelled.set(true);
                return !requestCancel.getAndSet(true);
            }
            return false;
        }

        @Override
        public R get() throws InterruptedException{
            if (isDone()) {
                return reply;
            }
            lock.lock();
            try {
                while (!isDone()) {
                    processorNotifyCondition.await();
                }
            } finally {
                lock.unlock();
            }
            return reply;
        }

        @Override
        public R get(final long timeout, final @NotNull TimeUnit unit) throws InterruptedException, TimeoutException {
            if (isCancelled()) {
                throw new CancellationException("Future was already cancelled");
            }
            if (isDone()) {
                return reply;
            }
            lock.lock();
            try {
                while (!isDone()) {
                    if (!processorNotifyCondition.await(timeout, unit)) {
                        throw new TimeoutException("Future timed out");
                    }
                    if (isCancelled()) {
                        throw new CancellationException("Future was already cancelled");
                    }
                }
            } catch (InterruptedException e) {
                lock.unlock();
                throw e;
            } finally {
                lock.unlock();
            }
            return reply;
        }

        public String getEndpoint() {
            return endpoint;
        }

        public DataSourceFilter.ReplyType getReplyType() {
            return replyType;
        }

        public Object getRequestBody() {
            return requestBody;
        }

        public Map<String, Object> getRequestFilter() {
            return requestFilter;
        }

        public Class<R> getRequestedDomainObjType() {
            return requestedDomainObjType;
        }

        @Override
        public boolean isCancelled() {
            return cancelled.get();
        }

        @Override
        public boolean isDone() {
            return (reply != null && !running.get()) || cancelled.get();
        }

        @SuppressWarnings("unchecked")
        protected void setReply(final Object newValue) {
            if (running.getAndSet(false)) {
                this.reply = (R) newValue;
            }
            notifyListener();
        }

        public String getInternalRequestID() {
            return internalRequestID;
        }

        private void notifyListener() {
            lock.lock();
            try {
                processorNotifyCondition.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }
}
