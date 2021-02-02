package io.opencmw.server.rest;

import static java.nio.charset.StandardCharsets.UTF_8;

import static io.javalin.apibuilder.ApiBuilder.get;
import static io.javalin.apibuilder.ApiBuilder.post;
import static io.javalin.plugin.openapi.dsl.DocumentedContentKt.anyOf;
import static io.javalin.plugin.openapi.dsl.DocumentedContentKt.documentedContent;
import static io.opencmw.OpenCmwProtocol.Command.*;
import static io.opencmw.OpenCmwProtocol.EMPTY_FRAME;
import static io.opencmw.OpenCmwProtocol.MdpMessage;
import static io.opencmw.OpenCmwProtocol.MdpMessage.receive;
import static io.opencmw.OpenCmwProtocol.MdpSubProtocol.PROT_CLIENT;
import static io.opencmw.OpenCmwProtocol.MdpSubProtocol.PROT_WORKER;
import static io.opencmw.server.MajordomoBroker.INTERNAL_ADDRESS_PUBLISHER;
import static io.opencmw.server.MmiServiceHelper.INTERNAL_SERVICE_NAMES;
import static io.opencmw.server.MmiServiceHelper.INTERNAL_SERVICE_OPENAPI;

import java.lang.reflect.ParameterizedType;
import java.net.ProtocolException;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.validation.constraints.NotNull;

import org.apache.commons.lang3.StringUtils;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import io.javalin.core.security.Role;
import io.javalin.http.BadRequestResponse;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.http.sse.SseClient;
import io.javalin.plugin.openapi.dsl.OpenApiBuilder;
import io.javalin.plugin.openapi.dsl.OpenApiDocumentation;
import io.opencmw.MimeType;
import io.opencmw.OpenCmwProtocol;
import io.opencmw.rbac.RbacRole;
import io.opencmw.serialiser.FieldDescription;
import io.opencmw.serialiser.annotations.MetaInfo;
import io.opencmw.serialiser.spi.ClassFieldDescription;
import io.opencmw.serialiser.utils.ClassUtils;
import io.opencmw.server.BasicMdpWorker;
import io.opencmw.server.MajordomoWorker;
import io.opencmw.server.rest.util.CombinedHandler;
import io.opencmw.server.rest.util.MessageBundle;
import io.opencmw.utils.CustomFuture;

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
    private static final byte[] RBAC = new byte[] {}; // TODO: implement RBAC between Majordomo and Worker
    private static final String TEMPLATE_EMBEDDED_HTML = "/velocity/property/defaultTextPropertyLayout.vm";
    private static final String TEMPLATE_BAD_REQUEST = "/velocity/errors/badRequest.vm";
    private static final AtomicLong REQUEST_COUNTER = new AtomicLong();
    protected final ConcurrentMap<String, OpenApiDocumentation> registeredEndpoints = new ConcurrentHashMap<>();
    private final BlockingArrayQueue<MdpMessage> requestQueue = new BlockingArrayQueue<>();
    private final ConcurrentMap<String, CustomFuture<MdpMessage>> requestReplies = new ConcurrentHashMap<>();

    public MajordomoRestPlugin(ZContext ctx, final String serverDescription, String httpAddress, final RbacRole<?>... rbacRoles) {
        super(ctx, MajordomoRestPlugin.class.getSimpleName(), rbacRoles);
        assert (httpAddress != null);
        RestServer.setName(Objects.requireNonNullElse(serverDescription, MajordomoRestPlugin.class.getName()));

        // add default root - here: redirect to mmi.service
        RestServer.getInstance().get("/", restCtx -> restCtx.redirect("/mmi.service"), RestServer.getDefaultRole());

        registerHandler(getDefaultRequestHandler()); // NOPMD - one-time call OK

        LOGGER.atInfo().addArgument(MajordomoRestPlugin.class.getName()).addArgument(RestServer.getPublicURI()).log("{} started on address: {}");
    }

    @Override
    public void notify(@NotNull final MdpMessage notifyMessage) {
        assert notifyMessage != null : "notify message must not be null";
        notifyRaw(notifyMessage);
    }

    @Override
    public synchronized void start() { // NOPMD 'synchronized' comes from JDK class definition
        final Thread dispatcher = new Thread(getDispatcherTask());
        dispatcher.setDaemon(true);
        dispatcher.setName(MajordomoRestPlugin.class.getSimpleName() + "Dispatcher");
        dispatcher.start();

        final Thread serviceListener = new Thread(getServiceSubscriptionTask());
        serviceListener.setDaemon(true);
        serviceListener.setName(MajordomoRestPlugin.class.getSimpleName() + "Subscriptions");
        serviceListener.start();

        // send subscription request for new service added notifications
        super.start();

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
            case GET_REQUEST:
                break;
            case SET_REQUEST:
                break;
            case PARTIAL:
            case FINAL:
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
                final String serviceName = handler.req.getSenderName();
                final String topicName = handler.req.topic.toString();
                final long eventTimeStamp = System.currentTimeMillis();
                final String notifyMessage = "new '" + topicName + "' @" + eventTimeStamp;
                final Queue<SseClient> sseClients = RestServer.getEventClients(serviceName);
                sseClients.forEach((final SseClient client) -> client.sendEvent(notifyMessage));
                return;
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
            final Queue<MdpMessage> notifyCopy = new ArrayDeque<>();
            while (runSocketHandlerLoop.get() && !Thread.interrupted()) {
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

    protected Runnable getServiceSubscriptionTask() {
        return () -> {
            try (ZMQ.Socket sub = ctx.createSocket(SocketType.SUB); ZMQ.Poller subPoller = ctx.createPoller(1)) {
                sub.setHWM(0);
                sub.connect(INTERNAL_ADDRESS_PUBLISHER);
                sub.subscribe(INTERNAL_SERVICE_NAMES);
                subPoller.register(sub, ZMQ.Poller.POLLIN);
                while (runSocketHandlerLoop.get() && !Thread.interrupted() && subPoller.poll() != -1) {
                    // handle message from or to broker
                    boolean dataReceived = true;
                    while (dataReceived) {
                        dataReceived = false;
                        // handle subscription message from or to broker
                        final MdpMessage brokerMsg = receive(sub, true);
                        if (brokerMsg != null) {
                            liveness = HEARTBEAT_LIVENESS;
                            // handle subscription message
                            if (brokerMsg.data == null) {
                                continue;
                            }
                            registerEndPoint(new String(brokerMsg.data, UTF_8)); // NOPMD in-loop instantiation necessary
                            dataReceived = true;
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
        new MdpMessage(null, PROT_WORKER, READY, serviceBytes, EMPTY_FRAME, RestServer.getPublicURI(), classNameByte, "", RBAC).send(workerSocket);
        new MdpMessage(null, PROT_WORKER, READY, serviceBytes, EMPTY_FRAME, RestServer.getLocalURI(), classNameByte, "", RBAC).send(workerSocket);
    }

    protected void registerEndPoint(final String endpoint) {
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
                    get(ep, OpenApiBuilder.documented(openApi, getDefaultServiceRestHandler(ep)), accessRoles);
                    post(ep, OpenApiBuilder.documented(openApi, getDefaultServiceRestHandler(ep)), accessRoles);
                });

                return openApi;
            } catch (final Exception e) { // NOPMD -- erroneous worker replies shall not stop the broker
                LOGGER.atError().setCause(e).addArgument(ep).log("could not register endpoint {}");
            }
            return null;
        });
    }

    @org.jetbrains.annotations.NotNull
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

    private Handler getDefaultServiceRestHandler(final String restHandler) {
        return new CombinedHandler(restCtx -> {
            LOGGER.atInfo().addArgument(restHandler).addArgument(restCtx.path()).addArgument(restCtx.fullUrl()).log("restHandler {} for service {} - full: {}");

            final String service = StringUtils.stripStart(Objects.requireNonNullElse(restCtx.path(), restHandler), "/");
            final MimeType acceptMimeType = MimeType.getEnum(restCtx.header(RestServer.HTML_ACCEPT));

            final Map<String, String[]> parameterMap = restCtx.req.getParameterMap();
            final String[] mimeType = parameterMap.get("contentType");
            final URI topic = mimeType == null || mimeType.length == 0 ? RestServer.appendUri(URI.create(restCtx.fullUrl()), "contentType=" + acceptMimeType.toString()) : URI.create(restCtx.fullUrl());

            OpenCmwProtocol.Command cmd = getCommand(restCtx);
            final MdpMessage requestMsg = new MdpMessage(null, PROT_CLIENT, cmd, service.getBytes(UTF_8), EMPTY_FRAME, topic, restCtx.bodyAsBytes(), "", RBAC);

            CustomFuture<MdpMessage> reply = dispatchRequest(requestMsg);
            try {
                final MdpMessage replyMessage = reply.get(); //TODO: add max time-out -- only if not long-polling (to be checked)
                switch (acceptMimeType) {
                case HTML:
                case TEXT:
                    Map<String, Object> dataMap = MessageBundle.baseModel(restCtx);
                    dataMap.put("textBody", new String(replyMessage.data, UTF_8));
                    if (cmd == SET_REQUEST) {
                        final String query = restCtx.req.getQueryString();
                        final String path = restCtx.req.getRequestURI() + "?" + query;
                        restCtx.redirect(path);
                    } else {
                        restCtx.render(TEMPLATE_EMBEDDED_HTML, dataMap);
                    }
                    break;
                default:
                    restCtx.result(replyMessage.data);
                    break;
                }

            } catch (Exception e) { // NOPMD - exception is rethrown
                switch (acceptMimeType) {
                case HTML:
                case TEXT:
                    Map<String, Object> dataMap = MessageBundle.baseModel(restCtx);
                    dataMap.put("service", restHandler);
                    dataMap.put("exceptionText", e);
                    restCtx.render(TEMPLATE_BAD_REQUEST, dataMap);
                    return;
                default:
                }
                throw new BadRequestResponse(MajordomoRestPlugin.class.getName() + ": could not process service '" + service + "' - exception:\n" + e.getMessage()); // NOPMD original exception forwared within the text, BadRequestResponse does not support exception forwarding
            }
        });
    }
}
