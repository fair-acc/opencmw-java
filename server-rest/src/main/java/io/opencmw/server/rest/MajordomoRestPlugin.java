package io.opencmw.server.rest;

import static java.nio.charset.StandardCharsets.UTF_8;

import static io.javalin.apibuilder.ApiBuilder.get;
import static io.javalin.apibuilder.ApiBuilder.post;
import static io.javalin.plugin.openapi.dsl.DocumentedContentKt.anyOf;
import static io.javalin.plugin.openapi.dsl.DocumentedContentKt.documentedContent;
import static io.opencmw.OpenCmwConstants.setDefaultSocketParameters;
import static io.opencmw.OpenCmwProtocol.Command.GET_REQUEST;
import static io.opencmw.OpenCmwProtocol.Command.READY;
import static io.opencmw.OpenCmwProtocol.Command.SET_REQUEST;
import static io.opencmw.OpenCmwProtocol.Command.UNKNOWN;
import static io.opencmw.OpenCmwProtocol.EMPTY_FRAME;
import static io.opencmw.OpenCmwProtocol.MdpMessage;
import static io.opencmw.OpenCmwProtocol.MdpMessage.receive;
import static io.opencmw.OpenCmwProtocol.MdpSubProtocol.PROT_CLIENT;
import static io.opencmw.OpenCmwProtocol.MdpSubProtocol.PROT_WORKER;
import static io.opencmw.server.MajordomoBroker.INTERNAL_ADDRESS_PUBLISHER;
import static io.opencmw.server.MmiServiceHelper.INTERNAL_SERVICE_NAMES;
import static io.opencmw.server.MmiServiceHelper.INTERNAL_SERVICE_OPENAPI;
import static io.opencmw.server.rest.RestServer.prefixPath;
import static io.opencmw.server.rest.util.CombinedHandler.SseState.CONNECTED;
import static io.opencmw.server.rest.util.CombinedHandler.SseState.DISCONNECTED;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.net.ProtocolException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

import org.apache.commons.lang3.StringUtils;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import io.javalin.apibuilder.ApiBuilder;
import io.javalin.core.security.Role;
import io.javalin.http.BadRequestResponse;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.http.sse.SseClient;
import io.javalin.plugin.openapi.dsl.OpenApiBuilder;
import io.javalin.plugin.openapi.dsl.OpenApiDocumentation;
import io.opencmw.MimeType;
import io.opencmw.OpenCmwProtocol;
import io.opencmw.QueryParameterParser;
import io.opencmw.rbac.RbacRole;
import io.opencmw.serialiser.FieldDescription;
import io.opencmw.serialiser.annotations.MetaInfo;
import io.opencmw.serialiser.spi.ClassFieldDescription;
import io.opencmw.serialiser.utils.ClassUtils;
import io.opencmw.server.BasicMdpWorker;
import io.opencmw.server.MajordomoWorker;
import io.opencmw.server.rest.util.CombinedHandler;
import io.opencmw.server.rest.util.MessageBundle;
import io.opencmw.utils.Cache;
import io.opencmw.utils.CustomFuture;

import com.google.common.io.ByteStreams;
import com.jsoniter.extra.Base64Support;
import com.jsoniter.output.JsonStream;
import com.jsoniter.spi.JsonException;

/**
 * Majordomo Broker REST/HTTP plugin.
 *
 * This opens two http ports and converts and forwards incoming request to the OpenCMW protocol and provides
 * some basic admin functionality
 *
 * <p>
 * Server parameter can be controlled via the following system properties:
 * <ul>
 * <li><em>restServerHostName</em>: host name or IP address the server should bind to
 * <li><em>restServerPort</em>: the HTTP port
 * <li><em>restServerPort2</em>: the HTTP/2 port (encrypted)
 * <li><em>restKeyStore</em>: the path to the file containing the key store for the encryption
 * <li><em>restKeyStorePassword</em>: the path to the file containing the key store for the encryption
 * <li><em>restUserPasswordStore</em>: the path to the file containing the user passwords and roles encryption
 * </ul>
 * @see RestServer for more details regarding the RESTful specific aspects
 *
 * @author rstein
 */
@MetaInfo(description = "Majordomo Broker REST/HTTP plugin.<br><br>"
                        + " This opens two http ports and converts and forwards incoming request to the OpenCMW protocol and provides<br>"
                        + " some basic admin functionality<br>",
        unit = "MajordomoRestPlugin")
@SuppressWarnings({ "PMD.ExcessiveImports", "PMD.TooManyStaticImports", "PMD.DoNotUseThreads" }) // makes the code more readable/shorter lines
public class MajordomoRestPlugin extends BasicMdpWorker {
    private static final Logger LOGGER = LoggerFactory.getLogger(MajordomoRestPlugin.class);
    private static final byte[] RBAC = {}; // TODO: implement RBAC between Majordomo and Worker
    private static final int LAST_NOTIFY_MAX_HISTORY = 20; // remember last 10 notifications
    private static final long LAST_NOTIFY_TIMEOUT = 20; // [s] remember last notifications for at most
    private static final String TEMPLATE_EMBEDDED_HTML = "/velocity/property/defaultTextPropertyLayout.vm";
    private static final String TEMPLATE_BAD_REQUEST = "/velocity/errors/badRequest.vm";
    private static final String TAG_MENU_ITEMS = "navContent";
    private static final String TAG_TEXT_BODY = "textBody";
    private static final String TAG_NO_MENU = "noMenu";
    private static final String TAG_NOTIFY_ID = "notifyID";
    private static final AtomicLong REQUEST_COUNTER = new AtomicLong();
    private static final AtomicLong NOTIFY_COUNTER = new AtomicLong();
    protected final ZMQ.Socket subSocket;
    protected final Map<String, AtomicInteger> subscriptionCount = new ConcurrentHashMap<>();
    protected final Map<Long, MdpMessage> subscriptionCache; // <notify_counter, last message>
    protected static final byte[] REST_SUB_ID = "REST_SUBSCRIPTION".getBytes(UTF_8);
    protected final ConcurrentMap<String, OpenApiDocumentation> registeredEndpoints = new ConcurrentHashMap<>();
    private final BlockingArrayQueue<MdpMessage> requestQueue = new BlockingArrayQueue<>();
    private final ConcurrentMap<String, CustomFuture<MdpMessage>> requestReplies = new ConcurrentHashMap<>();
    private final BiConsumer<SseClient, CombinedHandler.SseState> newSseClientHandler;
    private final AtomicReference<String> rootService = new AtomicReference<>("/mmi.service");
    private final Map<String, String> menuMap = new ConcurrentSkipListMap<>(); // NOPMD NOSONAR <menu-tag,property-path>
    static {
        try {
            Base64Support.enable();
        } catch (JsonException e) {
            // do nothing -- ensures that this is initialised only once
        }
    }

    public MajordomoRestPlugin(ZContext ctx, final String serverDescription, String httpAddress, final RbacRole<?>... rbacRoles) {
        super(ctx, MajordomoRestPlugin.class.getSimpleName(), rbacRoles);
        assert (httpAddress != null);
        RestServer.setName(Objects.requireNonNullElse(serverDescription, MajordomoRestPlugin.class.getName()));
        subSocket = ctx.createSocket(SocketType.SUB);
        setDefaultSocketParameters(subSocket);
        subSocket.connect(INTERNAL_ADDRESS_PUBLISHER);
        subSocket.subscribe(INTERNAL_SERVICE_NAMES);
        subscriptionCount.computeIfAbsent(INTERNAL_SERVICE_NAMES, s -> new AtomicInteger()).incrementAndGet();
        subscriptionCache = Cache.<Long, MdpMessage>builder().withLimit(LAST_NOTIFY_MAX_HISTORY).withTimeout(LAST_NOTIFY_TIMEOUT, TimeUnit.SECONDS).build();

        newSseClientHandler = (client, state) -> {
            final String queryString = client.ctx.queryString() == null ? "" : ("?" + client.ctx.queryString());
            final String subService = StringUtils.stripEnd(StringUtils.stripStart(client.ctx.path(), "/"), "/") + queryString;
            LOGGER.atDebug().addArgument(state).addArgument(subService).addArgument(subscriptionCount.computeIfAbsent(subService, s -> new AtomicInteger()).get()).log("RestPlugin {} to '{}' - existing subscriber count: {}");
            if (state == CONNECTED && subscriptionCount.computeIfAbsent(subService, s -> new AtomicInteger()).incrementAndGet() == 1) {
                subSocket.subscribe(subService);
            }
            if (state == DISCONNECTED && subscriptionCount.computeIfAbsent(subService, s -> new AtomicInteger()).decrementAndGet() <= 0) {
                subSocket.unsubscribe(subService);
                subscriptionCount.remove(subService);
            }
        };

        // add default root - here: redirect to mmi.service
        RestServer.getInstance().get("/", restCtx -> restCtx.redirect(rootService.get()), RestServer.getDefaultRole());

        registerHandler(getDefaultRequestHandler()); // NOPMD - one-time call OK

        LOGGER.atInfo().addArgument(MajordomoRestPlugin.class.getName()).addArgument(RestServer.getPublicURI()).log("{} started on address: {}");
    }

    /**
     * @return atomic reference to default root '/' service redirect path (default: 'mmi.service')
     */
    public AtomicReference<String> getRootService() {
        return rootService;
    }

    /**
     * @return default menu map &lt;Menu Item, path to service&gt;
     */
    public Map<String, String> getMenuMap() {
        return menuMap;
    }

    @Override
    public boolean notify(final @NotNull MdpMessage notifyMessage) {
        notifyRaw(notifyMessage);
        return false;
    }

    @Override
    public synchronized void start() { // NOPMD 'synchronized' comes from JDK class definition
        // send subscription request for new service added notifications
        super.start();

        final Thread dispatcher = new Thread(getDispatcherTask());
        dispatcher.setDaemon(true);
        dispatcher.setName(MajordomoRestPlugin.class.getSimpleName() + "Dispatcher");
        dispatcher.start();

        final Thread serviceListener = new Thread(getServiceSubscriptionTask());
        serviceListener.setDaemon(true);
        serviceListener.setName(MajordomoRestPlugin.class.getSimpleName() + "Subscriptions");
        serviceListener.start();

        // perform initial get request
        String services = "(uninitialised)";
        final CustomFuture<MdpMessage> reply = dispatchRequest(new MdpMessage(null, PROT_CLIENT, GET_REQUEST, INTERNAL_SERVICE_NAMES.getBytes(UTF_8), EMPTY_FRAME, URI.create(INTERNAL_SERVICE_NAMES), EMPTY_FRAME, "", RBAC));
        try {
            final MdpMessage msg = reply.get();
            services = msg.data == null ? "" : new String(msg.data, UTF_8);
            Arrays.stream(StringUtils.split(services, ",:;")).forEach(this::registerEndPoint);
        } catch (final Exception e) { // NOPMD -- erroneous worker replies shall not stop the broker
            LOGGER.atError().setCause(e).addArgument(services).log("could not perform initial registering of endpoints {}");
        }
    }

    protected static OpenCmwProtocol.Command getCommand(@NotNull final Context restCtx) {
        switch (restCtx.method()) {
        case "GET":
            return GET_REQUEST;
        case "POST":
            return SET_REQUEST;
        default:
            if (LOGGER.isDebugEnabled()) {
                LOGGER.atWarn().addArgument(restCtx.req).log("unknown request: {}");
            }
            return UNKNOWN;
        }
    }

    protected RequestHandler getDefaultRequestHandler() {
        return handler -> {
            switch (handler.req.command) {
            case PARTIAL:
            case FINAL:
                if (handler.req.clientRequestID.length == 0 || Arrays.equals(REST_SUB_ID, handler.req.clientRequestID)) {
                    handler.rep = null; // NOPMD needs to be 'null' to suppress message being further processed
                    break;
                }
                final String clientRequestID = new String(handler.req.clientRequestID, UTF_8);
                final CustomFuture<MdpMessage> replyFuture = requestReplies.remove(clientRequestID);
                if (replyFuture == null) {
                    LOGGER.atWarn().addArgument(clientRequestID).addArgument(handler.req).log("could not match clientRequestID '{}' to Future. msg was: {}");
                    return;
                }
                if (handler.req.errors == null || handler.req.errors.isBlank()) {
                    replyFuture.setReply(handler.req);
                } else {
                    // exception occurred - forward it
                    replyFuture.setException(new ProtocolException(handler.req.errors));
                }
                handler.rep = null; // NOPMD needs to be 'null' to suppress message being further processed
                return;
            case W_NOTIFY:
                notifySubscribedClients(handler.req);
                return;
            case GET_REQUEST:
            case SET_REQUEST:
            case DISCONNECT:
            case READY:
            case SUBSCRIBE:
            case UNSUBSCRIBE:
            case W_HEARTBEAT:
            case UNKNOWN:
            default:
                break;
            }
        };
    }

    protected Runnable getDispatcherTask() {
        return () -> {
            shallRun.set(true);
            final Queue<MdpMessage> notifyCopy = new ArrayDeque<>();
            while (shallRun.get() && !Thread.interrupted()) {
                synchronized (requestQueue) {
                    try {
                        requestQueue.wait();
                        if (!requestQueue.isEmpty()) {
                            notifyCopy.addAll(requestQueue);
                            requestQueue.clear();
                        }
                    } catch (InterruptedException e) {
                        LOGGER.atWarn().setCause(e).log("Interrupted!");
                        // restore interrupted state...
                        Thread.currentThread().interrupt();
                    }
                }
                if (notifyCopy.isEmpty()) {
                    continue;
                }
                notifyCopy.forEach(this::notify);
                notifyCopy.clear();
            }
        };
    }

    protected Runnable getServiceSubscriptionTask() { // NOSONAR NOPMD - complexity is acceptable
        return () -> {
            shallRun.set(true);
            try (ZMQ.Poller subPoller = ctx.createPoller(1)) {
                subPoller.register(subSocket, ZMQ.Poller.POLLIN);
                while (shallRun.get() && !Thread.interrupted() && !ctx.isClosed() && subPoller.poll(TimeUnit.MILLISECONDS.toMillis(100)) != -1) {
                    if (ctx.isClosed()) {
                        break;
                    }
                    // handle message from or to broker
                    boolean dataReceived = true;
                    while (dataReceived && !ctx.isClosed()) {
                        dataReceived = false;
                        // handle subscription message from or to broker
                        final MdpMessage brokerMsg = receive(subSocket, false);
                        if (brokerMsg != null) {
                            dataReceived = true;
                            liveness = heartBeatLiveness;

                            // handle subscription message
                            if (brokerMsg.data != null && brokerMsg.getServiceName().startsWith(INTERNAL_SERVICE_NAMES)) {
                                final String newServiceName = new String(brokerMsg.data, UTF_8); // NOPMD in-loop instantiation necessary
                                registerEndPoint(newServiceName);
                                // special handling for internal MMI interface
                                if (newServiceName.contains("/mmi.")) {
                                    registerEndPoint(StringUtils.split(newServiceName, "/", 2)[1]);
                                }
                            }
                            notifySubscribedClients(brokerMsg);
                        }
                    }
                }
            }
        };
    }

    @Override
    protected void reconnectToBroker() {
        super.reconnectToBroker();
        final byte[] classNameByte = this.getClass().getName().getBytes(UTF_8); // used for OpenAPI purposes
        new MdpMessage(null, PROT_WORKER, READY, serviceBytes, EMPTY_FRAME, Objects.requireNonNull(RestServer.getPublicURI()), classNameByte, "", RBAC).send(workerSocket);
        new MdpMessage(null, PROT_WORKER, READY, serviceBytes, EMPTY_FRAME, Objects.requireNonNull(RestServer.getLocalURI()), classNameByte, "", RBAC).send(workerSocket);
    }

    protected void registerEndPoint(final String endpoint) {
        synchronized (registeredEndpoints) {
            // needs to be synchronised since Javalin get(..), put(..) seem to be not thread safe (usually initialised during startup)
            registeredEndpoints.computeIfAbsent(endpoint, ep -> {
                final MdpMessage requestMsg = new MdpMessage(null, PROT_CLIENT, GET_REQUEST, INTERNAL_SERVICE_OPENAPI.getBytes(UTF_8), EMPTY_FRAME, URI.create(INTERNAL_SERVICE_OPENAPI), ep.getBytes(UTF_8), "", RBAC);
                final CustomFuture<MdpMessage> openApiReply = dispatchRequest(requestMsg);
                try {
                    final MdpMessage serviceOpenApiData = openApiReply.get();
                    if (!serviceOpenApiData.errors.isBlank()) {
                        LOGGER.atWarn().addArgument(ep).addArgument(serviceOpenApiData).log("received erroneous message for service '{}': {}");
                        return null;
                    }
                    final String handlerClassName = new String(serviceOpenApiData.data, UTF_8);

                    OpenApiDocumentation openApi = getOpenApiDocumentation(handlerClassName);

                    final Set<Role> accessRoles = RestServer.getDefaultRole();
                    RestServer.getInstance().routes(() -> {
                        ApiBuilder.before(ep, restCtx -> {
                            // for some strange reason this needs to be executed to be able to read 'restCtx.formParamMap()'
                            if ("POST".equals(restCtx.method())) {
                                final Map<String, List<String>> map = restCtx.formParamMap();
                                if (map.size() == 0) {
                                    LOGGER.atDebug().addArgument(restCtx.req.getPathInfo()).log("{} called without form data");
                                }
                            }
                        });
                        post(ep + "*", OpenApiBuilder.documented(openApi, getDefaultServiceRestHandler(ep)), accessRoles);
                        get(ep + "*", OpenApiBuilder.documented(openApi, getDefaultServiceRestHandler(ep)), accessRoles);
                    });

                    return openApi;
                } catch (final Exception e) { // NOPMD -- erroneous worker replies shall not stop the broker
                    LOGGER.atError().setCause(e).addArgument(ep).log("could not register endpoint {}");
                }
                return null;
            });
        }
    }

    @NotNull
    private OpenApiDocumentation getOpenApiDocumentation(final String handlerClassName) {
        OpenApiDocumentation openApi = OpenApiBuilder.document();
        try {
            final Class<?> clazz = Class.forName(handlerClassName);
            final ClassFieldDescription fieldDescription = ClassUtils.getFieldDescription(clazz);
            openApi.operation(openApiOperation -> {
                openApiOperation.description(fieldDescription.getFieldDescription() + " - " + handlerClassName);
                openApiOperation.operationId("myOperationId");
                openApiOperation.summary(fieldDescription.getFieldUnit());
                openApiOperation.deprecated(false);
                openApiOperation.addTagsItem("user");
            });

            if (MajordomoWorker.class.isAssignableFrom(clazz)) {
                // class is a MajordomoWorker derivative
                final ParameterizedType genericSuperClass = (ParameterizedType) clazz.getGenericSuperclass();
                final Class<?> ctxClass = (Class<?>) genericSuperClass.getActualTypeArguments()[0];
                final Class<?> inClass = (Class<?>) genericSuperClass.getActualTypeArguments()[1];
                final Class<?> outClass = (Class<?>) genericSuperClass.getActualTypeArguments()[2];

                final ClassFieldDescription ctxFilter = ClassUtils.getFieldDescription(ctxClass);
                for (FieldDescription field : ctxFilter.getChildren()) {
                    ClassFieldDescription classField = (ClassFieldDescription) field;
                    openApi.queryParam(classField.getFieldName(), (Class<?>) classField.getType());
                    openApi.formParam(classField.getFieldName(), (Class<?>) classField.getType(), false); // find definition for required or not
                }

                openApi.body(anyOf(documentedContent(outClass), documentedContent(inClass)));
                openApi.body(outClass).json("200", outClass); // JSON definition
                openApi.html("200").result("demo output"); // HTML definition

                //TODO: continue here -- work in progress
            }

        } catch (Exception e) { // NOPMD
            LOGGER.atWarn().setCause(e).addArgument(handlerClassName).log("could not find class definition for {}");
        }
        return openApi;
    }

    private CustomFuture<MdpMessage> dispatchRequest(final MdpMessage requestMsg) {
        final String requestID = MajordomoRestPlugin.class.getSimpleName() + "#" + REQUEST_COUNTER.getAndIncrement();
        requestMsg.clientRequestID = requestID.getBytes(UTF_8);
        CustomFuture<MdpMessage> reply = new CustomFuture<>();
        final Object ret = requestReplies.put(requestID, reply);
        if (ret != null) {
            LOGGER.atWarn().addArgument(requestID).addArgument(requestMsg.getServiceName()).log("duplicate request {} for service {}");
        }

        if (!requestQueue.offer(requestMsg)) {
            throw new IllegalStateException("could not add MdpMessage to requestQueue: " + requestMsg);
        }
        synchronized (requestQueue) {
            requestQueue.notifyAll();
        }
        return reply;
    }

    protected void notifySubscribedClients(final @NotNull MdpMessage msg) {
        final Long notifyID = NOTIFY_COUNTER.getAndIncrement();
        try {
            msg.topic = QueryParameterParser.appendQueryParameter(msg.topic, TAG_NO_MENU + '&' + TAG_NOTIFY_ID + '=' + notifyID);
        } catch (URISyntaxException e) {
            LOGGER.atWarn().setCause(e).addArgument(TAG_NOTIFY_ID).addArgument(msg.topic).log("could not append {} to {}");
            return;
        }
        subscriptionCache.put(notifyID, msg);
        final String notifyPath = prefixPath(msg.topic.getPath());
        final Queue<SseClient> clients = RestServer.getEventClients(notifyPath);
        final Predicate<SseClient> filter = c -> {
            final String clientPath = StringUtils.stripEnd(c.ctx.path(), "/");
            return clientPath.length() >= notifyPath.length() && clientPath.startsWith(notifyPath);
        };
        final String topicString = msg.topic.toString();
        clients.stream().filter(filter).forEach(s -> s.sendEvent(topicString));
    }

    private Handler getDefaultServiceRestHandler(final String restHandler) { // NOSONAR NOPMD - complexity is acceptable
        return new CombinedHandler(restCtx -> {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.atTrace().addArgument(restHandler).addArgument(restCtx.path()).addArgument(restCtx.fullUrl()).log("restHandler {} for service {} - full: {}");
            }

            final String service = StringUtils.stripStart(Objects.requireNonNullElse(restCtx.path(), restHandler), "/");
            final MimeType acceptMimeType = MimeType.getEnum(restCtx.header(RestServer.HTML_ACCEPT));
            final Map<String, String[]> parameterMap = restCtx.req.getParameterMap();

            final String[] mimeType = parameterMap.get("contentType");
            URI topic = mimeType == null || mimeType.length == 0 ? RestServer.appendUri(URI.create(restCtx.fullUrl()), "contentType=" + acceptMimeType.toString()) : URI.create(restCtx.fullUrl());

            OpenCmwProtocol.Command cmd = getCommand(restCtx);
            final byte[] requestData;
            if (cmd == SET_REQUEST) {
                requestData = getFormDataAsJson(restCtx);
            } else {
                requestData = EMPTY_FRAME;
            }

            final CustomFuture<MdpMessage> reply;
            final String[] notifyCounterString = parameterMap.get(TAG_NOTIFY_ID);
            if (cmd == GET_REQUEST && notifyCounterString != null && notifyCounterString.length == 1) {
                // short-cut for get response from cache after a notify (via subscription ID)
                reply = new CustomFuture<>();
                try {
                    final Long notifyCounter = Long.parseLong(notifyCounterString[0]);
                    final MdpMessage replyMsg = subscriptionCache.get(notifyCounter);
                    if (replyMsg == null) {
                        reply.setException(new ProtocolException("cached notification " + TAG_NOTIFY_ID + '=' + notifyCounter + " not available (anymore)"));
                    } else {
                        reply.setReply(replyMsg);
                        topic = replyMsg.topic;
                    }
                } catch (NumberFormatException e) {
                    throwHtmlException(restHandler, restCtx, service, acceptMimeType, e);
                }
            } else {
                final MdpMessage requestMsg = new MdpMessage(null, PROT_CLIENT, cmd, service.getBytes(UTF_8), EMPTY_FRAME, topic, requestData, "", RBAC);
                reply = dispatchRequest(requestMsg);
            }

            try {
                final MdpMessage replyMessage = reply.get(); //TODO: add max time-out -- only if not long-polling (to be checked)
                MimeType replyMimeType = QueryParameterParser.getMimeType(replyMessage.topic.getQuery());
                replyMimeType = replyMimeType == MimeType.UNKNOWN ? acceptMimeType : replyMimeType;

                switch (replyMimeType) {
                case HTML:
                    final String queryString = topic.getQuery() == null ? "" : ("?" + topic.getQuery());
                    if (cmd == SET_REQUEST) {
                        final String path = replyMessage.topic.getPath() + StringUtils.replace(queryString, "&noMenu", "");
                        restCtx.redirect(path);
                    } else {
                        final boolean noMenu = queryString.contains(TAG_NO_MENU);
                        Map<String, Object> dataMap = MessageBundle.baseModel(restCtx);

                        dataMap.put(TAG_MENU_ITEMS, menuMap);
                        dataMap.put(TAG_TEXT_BODY, new String(replyMessage.data, UTF_8));
                        dataMap.put(TAG_NO_MENU, noMenu);
                        restCtx.render(TEMPLATE_EMBEDDED_HTML, dataMap);
                    }
                    break;
                case TEXT:
                    restCtx.contentType(replyMimeType.toString());
                    restCtx.result(new String(replyMessage.data, UTF_8));
                    break;
                case BINARY:
                default:
                    restCtx.contentType(replyMimeType.toString());
                    restCtx.result(replyMessage.data);

                    break;
                }

            } catch (Exception e) { // NOPMD - exception is rethrown
                throwHtmlException(restHandler, restCtx, service, acceptMimeType, e);
            }
        }, newSseClientHandler);
    }

    private void throwHtmlException(final String restHandler, final Context restCtx, final String service, final MimeType acceptMimeType, final Exception e) {
        switch (acceptMimeType) {
        case HTML:
        case TEXT:
            Map<String, Object> dataMap = MessageBundle.baseModel(restCtx);
            dataMap.put(TAG_MENU_ITEMS, menuMap);
            dataMap.put("service", restHandler);
            dataMap.put("exceptionText", e);
            restCtx.render(TEMPLATE_BAD_REQUEST, dataMap);
            return;
        default:
        }
        throw new BadRequestResponse(MajordomoRestPlugin.class.getName() + ": could not process service '" + service + "' - exception:\n" + e.getMessage()); // NOPMD original exception forwarded within the text, BadRequestResponse does not support exception forwarding
    }

    private byte[] getFormDataAsJson(final Context restCtx) {
        final Map<String, List<String>> formMap = restCtx.formParamMap();
        final HashMap<String, Object> requestMap = new HashMap<>();
        formMap.forEach((k, v) -> {
            if (v.isEmpty()) {
                requestMap.put(k, null);
            } else {
                requestMap.put(k, v.get(0));
            }
        });

        if (restCtx.isMultipartFormData()) {
            requestMap.remove("files");
            restCtx.uploadedFiles("files").forEach(file -> {
                try {
                    @SuppressWarnings("UnstableApiUsage")
                    final byte[] rawData = ByteStreams.toByteArray(file.getContent()); // NOPMD NOSONAR
                    requestMap.put("resourceName", file.getFilename());
                    requestMap.put("contentType", file.getContentType());
                    requestMap.put("data", Base64.getEncoder().encodeToString(rawData));
                    requestMap.put("dataSize", rawData.length);
                } catch (IOException e) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.atWarn().setCause(e).log("could not parse uploaded file");
                    }
                }
            });
        }

        return JsonStream.serialize(requestMap).getBytes(UTF_8);
    }
}
