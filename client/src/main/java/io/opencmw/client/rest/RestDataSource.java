
package io.opencmw.client.rest;

import static io.opencmw.OpenCmwConstants.setDefaultSocketParameters;
import static io.opencmw.OpenCmwProtocol.EMPTY_FRAME;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.MalformedURLException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.spi.LoggingEventBuilder;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

import io.opencmw.MimeType;
import io.opencmw.client.DataSource;
import io.opencmw.client.DnsResolver;
import io.opencmw.serialiser.IoSerialiser;
import io.opencmw.serialiser.spi.JsonSerialiser;
import io.opencmw.utils.NoDuplicatesList;

import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.sse.EventSource;
import okhttp3.sse.EventSourceListener;
import okhttp3.sse.EventSources;

@SuppressWarnings({ "PMD.TooManyFields", "PMD.ExcessiveImports" })
public class RestDataSource extends DataSource implements Runnable {
    private static final List<String> APPLICABLE_SCHEMES = List.of("http", "https");
    private static final List<DnsResolver> RESOLVERS = Collections.synchronizedList(new NoDuplicatesList<>());
    public static final Factory FACTORY = new Factory() {
        @Override
        public List<String> getApplicableSchemes() {
            return APPLICABLE_SCHEMES;
        }

        @Override
        public Class<? extends IoSerialiser> getMatchingSerialiserType(final @NotNull URI endpoint) {
            return JsonSerialiser.class;
        }

        @Override
        public DataSource newInstance(final ZContext context, final @NotNull URI endpoint, final @NotNull Duration timeout, final @NotNull ExecutorService executorService, final @NotNull String clientId) {
            return new RestDataSource(context, endpoint, timeout, clientId);
        }

        @Override
        public List<DnsResolver> getRegisteredDnsResolver() {
            return RESOLVERS;
        }
    };
    private static final Logger LOGGER = LoggerFactory.getLogger(RestDataSource.class);
    private static final int WAIT_TIMEOUT_MILLIS = 1000;
    private static final AtomicInteger REST_DATA_SOURCE_INSTANCE = new AtomicInteger();
    private static final int MAX_RETRIES = 3;
    private static final AtomicLong PUBLICATION_COUNTER = new AtomicLong();
    protected static OkHttpClient okClient;
    protected static EventSource.Factory eventSourceFactory;
    protected final AtomicBoolean run = new AtomicBoolean(true); // NOPMD
    protected final String uniqueID;
    protected final byte[] uniqueIdBytes;
    protected final URI endpoint;
    protected final Duration timeOut;
    protected final String clientID;
    protected int cancelLastCall; // needed for unit-testing only
    protected final ZContext ctxCopy;
    protected final Object newData = new Object(); // to notify event loop that new data has arrived
    protected final Timer timer = new Timer();
    protected final List<RestCallBack> pendingCallbacks = Collections.synchronizedList(new ArrayList<>());
    protected final List<RestCallBack> completedCallbacks = Collections.synchronizedList(new ArrayList<>());
    protected final BlockingQueue<String> requestQueue = new LinkedBlockingDeque<>();
    protected final Map<String, EventSource> sseSource = new HashMap<>(); // NOPMD - only accessed from main thread
    protected Socket internalSocket; // facing towards the internal REST client API
    protected Socket externalSocket; // facing towards the DataSource manager
    protected final TimerTask wakeupTask = new TimerTask() {
        @Override
        public void run() {
            synchronized (newData) {
                newData.notifyAll();
            }
        }
    };

    protected RestDataSource(final ZContext ctx, final URI endpoint) {
        this(ctx, endpoint, Duration.ofMillis(0), RestDataSource.class.getName());
    }

    /**
     * Constructor
     * @param ctx ZeroMQ context to use
     * @param endpoint Endpoint to subscribe to
     * @param timeOut after which the request defaults to a time-out exception (no data)
     * @param clientID subscription id to be able to process the notification updates.
     */
    public RestDataSource(final ZContext ctx, final URI endpoint, final Duration timeOut, final String clientID) {
        super(endpoint);
        synchronized (LOGGER) { // prevent race condition between multiple constructor invocations
            // initialised only when needed, ie. when RestDataSource is actually instantiated
            if (okClient == null) {
                okClient = new OkHttpClient(); // NOPMD
                eventSourceFactory = EventSources.createFactory(okClient); // NOPMD
            }
        }

        if (timeOut == null) {
            throw new IllegalArgumentException("timeOut is null");
        }
        this.ctxCopy = ctx == null ? new ZContext() : ctx;
        this.endpoint = endpoint;
        this.timeOut = timeOut;
        this.clientID = clientID;

        uniqueID = clientID + "PID=" + ManagementFactory.getRuntimeMXBean().getName() + "-InstanceID=" + REST_DATA_SOURCE_INSTANCE.getAndIncrement();
        uniqueIdBytes = uniqueID.getBytes(ZMQ.CHARSET);
        if (timeOut.toMillis() > 0) {
            timer.scheduleAtFixedRate(wakeupTask, 0, timeOut.toMillis());
        }

        internalSocket = ctxCopy.createSocket(SocketType.PAIR);
        setDefaultSocketParameters(internalSocket);
        if (!internalSocket.setIdentity(uniqueIdBytes)) {
            throw new IllegalStateException("could not set identity on internalSocket");
        }
        if (!internalSocket.bind("inproc://" + uniqueID)) {
            throw new IllegalStateException("could not bind internalSocket to: inproc://" + uniqueID);
        }

        externalSocket = ctxCopy.createSocket(SocketType.PAIR);
        setDefaultSocketParameters(externalSocket);
        if (!externalSocket.connect("inproc://" + uniqueID)) {
            throw new IllegalStateException("could not bind externalSocket to: inproc://" + uniqueID);
        }

        LOGGER.atTrace().addArgument(endpoint).log("connecting to REST endpoint: '{}'");

        start(); // NOPMD - starts on initialisation
    }

    @Override
    public void get(final String requestId, final URI endpoint, final byte[] data, final byte[] rbacToken) {
        enqueueRequest(requestId); //TODO: refactor interface
    }

    @Override
    public void set(final String requestId, final URI endpoint, final byte[] data, final byte[] rbacToken) {
        throw new UnsupportedOperationException("set not (yet) implemented");
    }

    public void enqueueRequest(final String hashKey) {
        if (!requestQueue.offer(hashKey)) {
            throw new IllegalStateException("could not add hashKey " + hashKey + " to request queue of endpoint " + endpoint);
        }
        synchronized (newData) {
            newData.notifyAll();
        }
    }

    @Override
    public void subscribe(final String reqId, final URI endpoint, final byte[] rbacToken) {
        try {
            final var request = new Request.Builder().url(endpoint.toURL()).build();
            sseSource.put(reqId, eventSourceFactory.newEventSource(request, new EventSourceListener() {
                @Override
                public void onEvent(final @NotNull EventSource eventSource, final String id, final String type, final @NotNull String data) {
                    final String pubKey = clientID + "#" + PUBLICATION_COUNTER.getAndIncrement();
                    getRequest(pubKey, endpoint.toString(), MimeType.TEXT); // poll actual endpoint
                }
            }));
        } catch (MalformedURLException e) {
            LOGGER.atWarn().addArgument(endpoint).setCause(e).log("Error building uri for endpoint: {}");
        }
    }

    @Override
    public void unsubscribe(final String reqId) {
        final EventSource source = sseSource.remove(reqId);
        if (source != null) {
            source.cancel();
        }
    }

    public ZContext getCtx() {
        return ctxCopy;
    }

    @Override
    public ZMQ.Socket getSocket() {
        return externalSocket;
    }

    @Override
    public void close() {
        internalSocket.close();
        externalSocket.close();
    }

    @Override
    protected Factory getFactory() {
        return FACTORY;
    }

    /**
     * Gets called whenever data is available on the DataSource's socket.
     * Should then try to receive data and return any results back to the calling event loop.
     * @return null if there is no more data available, a Zero length ZMsg if there was data which was only used internally
     * or a ZMsg with [reqId, endpoint, byte[] data, [byte[] optional RBAC token]]
     */
    @Override
    public ZMsg getMessage() {
        return ZMsg.recvMsg(externalSocket, false);
    }

    @Override
    public long housekeeping() {
        synchronized (newData) {
            ArrayList<RestCallBack> temp = new ArrayList<>(pendingCallbacks);
            for (RestCallBack callBack : temp) {
                callBack.checkTimeOut();
            }

            try {
                while (!requestQueue.isEmpty()) {
                    final String hash = requestQueue.take();
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.atTrace().addArgument(hash).log("external request with hashKey = '{}'");
                    }
                    getRequest(hash, endpoint.toString(), MimeType.TEXT);
                }
            } catch (InterruptedException e) { // NOSONAR NOPMD
                LOGGER.atError().setCause(e).addArgument(endpoint).log("error in retrieving requestQueue items for endpoint: {}");
            }
        }
        return System.currentTimeMillis() + timeOut.toMillis();
    }

    public void run() { // NOPMD NOSONAR - complexity
        run.set(true);
        try {
            while (run.get() && !Thread.interrupted()) {
                synchronized (newData) {
                    if (completedCallbacks.isEmpty() && requestQueue.isEmpty()) {
                        // nothing to do, wait for signals
                        final long waitMax;
                        if (timeOut.toMillis() <= 0) {
                            waitMax = TimeUnit.MILLISECONDS.toMillis(WAIT_TIMEOUT_MILLIS);
                        } else {
                            waitMax = timeOut.toMillis();
                        }
                        // N.B. is automatically updated in case of time-out and/or new arriving data/exceptions
                        newData.wait(waitMax);
                    }

                    for (RestCallBack callBack : completedCallbacks) {
                        // notify data

                        final byte[] data;
                        if (callBack.response == null) {
                            // exception branch
                            data = EMPTY_FRAME;
                        } else {
                            data = callBack.response.peekBody(Long.MAX_VALUE).bytes();
                            callBack.response.close();
                        }
                        final byte[] exception = callBack.exception == null ? EMPTY_FRAME : callBack.exception.getMessage().getBytes(StandardCharsets.UTF_8);

                        final var msg = new ZMsg(); // NOPMD - instantiation in loop
                        msg.add(callBack.hashKey);
                        msg.add(callBack.endPointName);
                        msg.add(data);
                        msg.add(exception);

                        if (!msg.send(internalSocket)) {
                            throw new IllegalStateException("internalSocket could not send message - error code: " + internalSocket.errno());
                        }
                    }
                    completedCallbacks.clear();

                    housekeeping();
                }
            }
        } catch (final Exception e) { // NOPMD NOSONAR -- terminate normally beyond this point
            LOGGER.atError().setCause(e).log("data acquisition loop abnormally terminated");
        } finally {
            externalSocket.close();
            internalSocket.close();
        }
        LOGGER.atTrace().addArgument(uniqueID).addArgument(run.get()).log("stop poller thread for uniqueID={} - run={}");
    }

    public void start() {
        new Thread(this).start(); // NOPMD
    }

    public void stop() {
        final HashSet<String> sseSourceKeyCopy = new HashSet<>(sseSource.keySet());
        for (final String reqId : sseSourceKeyCopy) {
            unsubscribe(reqId);
        }
        run.set(false);
    }

    protected void getRequest(final String hashKey, final String path, final MimeType mimeType) {
        final var request = new Request.Builder().url(path).get().addHeader("Accept", mimeType.toString()).build();
        if (LOGGER.isTraceEnabled()) {
            LOGGER.atTrace().addArgument(endpoint).addArgument(path).addArgument(request).log("new request for {} - {} : request{}");
        }
        final var callBack = new RestCallBack(hashKey, path, mimeType);
        pendingCallbacks.add(callBack);
        final var call = okClient.newCall(request);
        call.enqueue(callBack);
        if (cancelLastCall > 0) {
            call.cancel(); // needed only for unit-testing
            cancelLastCall--;
        }
    }

    public class RestCallBack implements Callback {
        private final String hashKey;
        private final String endPointName;
        private final MimeType mimeType;
        private final long requestTimeStamp = System.currentTimeMillis();
        private boolean active = true;
        private final AtomicInteger retryCount = new AtomicInteger();
        private final Lock lock = new ReentrantLock();
        private Response response;
        private Exception exception;

        public RestCallBack(final String hashKey, final String endPointName, final MimeType mimeType) {
            this.hashKey = hashKey;
            this.endPointName = endPointName;
            this.mimeType = mimeType;
        }

        @Override
        public String toString() {
            return "RestCallBack{hashKey='" + hashKey + '\'' + ", endPointName='" + endPointName + '\'' + ", requestTimeStamp=" + requestTimeStamp + ", active=" + active + ", retryCount=" + retryCount + ", result=" + response + ", exception=" + exception + '}';
        }

        public void checkTimeOut() {
            if (!active || timeOut.toMillis() <= 0) {
                return;
            }
            final long now = System.currentTimeMillis();
            if (requestTimeStamp + timeOut.toMillis() < now) {
                // mark failed and notify
                lock.lock();
                try {
                    exception = new TimeoutException("ts=" + now + " - time-out of REST request for endpoint: " + endpoint);
                    notifyResult();
                } finally {
                    lock.unlock();
                }
            }
        }

        @Override
        public void onFailure(@NotNull final Call call, @NotNull final IOException e) {
            if (!active) {
                return;
            }

            if (retryCount.incrementAndGet() <= MAX_RETRIES) {
                lock.lock();
                exception = e;
                lock.unlock();
                final LoggingEventBuilder logger = LOGGER.atWarn();
                if (LOGGER.isTraceEnabled()) {
                    logger.setCause(e);
                }
                logger.addArgument(retryCount.get()).addArgument(MAX_RETRIES).addArgument(endpoint).log("retry {} of {}: could not connect/receive from endpoint {}");
                // TODO: add more sophisticated exponential back-off
                LockSupport.parkNanos(timeOut.toMillis() * (1L << (2 * (retryCount.get() - 1))));
                final var request = new Request.Builder().url(endPointName).get().addHeader("Accept", mimeType.toString()).build();
                final var repeatedCall = okClient.newCall(request);
                repeatedCall.enqueue(this);
                if (cancelLastCall > 0) {
                    repeatedCall.cancel(); // needed only for unit-testing
                    cancelLastCall--;
                }
                return;
            }
            LOGGER.atWarn().setCause(e).addArgument(MAX_RETRIES).addArgument(endpoint).log("failed after {} connect/receive retries - abort");
            lock.lock();
            try {
                exception = e;
                notifyResult();
            } finally {
                lock.unlock();
                LOGGER.atWarn().addArgument(e.getLocalizedMessage()).log("RestCallBack-Failure: '{}'");
            }
        }

        @Override
        public void onResponse(@NotNull final Call call, @NotNull final Response response) {
            if (!active) {
                return;
            }
            lock.lock();
            try {
                this.response = response;
                notifyResult();
            } finally {
                lock.unlock();
            }
            if (LOGGER.isTraceEnabled()) {
                LOGGER.atTrace().addArgument(response).log("RestCallBack: '{}'");
            }
        }

        private void notifyResult() {
            synchronized (newData) {
                active = false;
                pendingCallbacks.remove(this);
                completedCallbacks.add(this);
                newData.notifyAll();
            }
        }
    }
}
