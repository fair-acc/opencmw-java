package io.opencmw.server;

import static java.nio.charset.StandardCharsets.UTF_8;

import static io.opencmw.OpenCmwProtocol.Command.SUBSCRIBE;
import static io.opencmw.OpenCmwProtocol.EMPTY_FRAME;
import static io.opencmw.OpenCmwProtocol.EMPTY_URI;
import static io.opencmw.OpenCmwProtocol.MdpSubProtocol.PROT_CLIENT;

import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;

import io.opencmw.MimeType;
import io.opencmw.OpenCmwProtocol;
import io.opencmw.OpenCmwProtocol.MdpMessage;
import io.opencmw.QueryParameterParser;
import io.opencmw.domain.BinaryData;
import io.opencmw.filter.FilterRegistry;
import io.opencmw.rbac.RbacRole;
import io.opencmw.serialiser.IoBuffer;
import io.opencmw.serialiser.IoClassSerialiser;
import io.opencmw.serialiser.annotations.MetaInfo;
import io.opencmw.serialiser.spi.BinarySerialiser;
import io.opencmw.serialiser.spi.CmwLightSerialiser;
import io.opencmw.serialiser.spi.FastByteBuffer;
import io.opencmw.serialiser.spi.JsonSerialiser;

/**
 * MajordomoWorker implementation including de-serialising and re-serialising to domain-objects.
 * <p>
 * This implements GET/SET/NOTIFY handlers that are driven by PoJo domain objects.
 *
 * @author rstein
 *
 * @param <C> generic type for the query/context mapping object
 * @param <I> generic type for the input domain object
 * @param <O> generic type for the output domain object (also notify)
 */
@SuppressWarnings({ "PMD.DataClass", "PMD.NPathComplexity", "PMD.ExcessiveImports" }) // PMD - false positive data class
@MetaInfo(description = "default MajordomoWorker implementation")
public class MajordomoWorker<C, I, O> extends BasicMdpWorker {
    private static final Logger LOGGER = LoggerFactory.getLogger(MajordomoWorker.class);
    private static final String TAG_NO_MENU = "noMenu";
    private static final int MAX_BUFFER_SIZE = 4000;
    protected final IoBuffer defaultBuffer = new FastByteBuffer(MAX_BUFFER_SIZE, true, null);
    protected final IoBuffer defaultNotifyBuffer = new FastByteBuffer(MAX_BUFFER_SIZE, true, null);
    protected final IoClassSerialiser deserialiser = new IoClassSerialiser(defaultBuffer);
    protected final IoClassSerialiser serialiser = new IoClassSerialiser(defaultBuffer);
    protected final IoClassSerialiser notifySerialiser = new IoClassSerialiser(defaultNotifyBuffer);
    protected final Class<C> contextClassType;
    protected final Class<I> inputClassType;
    protected final Class<O> outputClassType;
    protected final I emptyInput;
    protected Handler<C, I, O> handler;
    protected Handler<C, I, O> htmlHandler;

    public MajordomoWorker(final URI brokerAddress, final String serviceName,
            @NotNull final Class<C> contextClassType,
            @NotNull final Class<I> inputClassType,
            @NotNull final Class<O> outputClassType, final RbacRole<?>... rbacRoles) {
        this(null, brokerAddress, serviceName, contextClassType, inputClassType, outputClassType, rbacRoles);
    }

    public MajordomoWorker(final ZContext ctx, final String serviceName,
            @NotNull final Class<C> contextClassType,
            @NotNull final Class<I> inputClassType,
            @NotNull final Class<O> outputClassType, final RbacRole<?>... rbacRoles) {
        this(ctx, URI.create("inproc://broker"), serviceName, contextClassType, inputClassType, outputClassType, rbacRoles);
    }

    @SuppressWarnings({ "PMD.ExcessiveMethodLength", "PMD.PrematureDeclaration" })
    protected MajordomoWorker(final ZContext ctx, final URI brokerAddress, final String serviceName,
            @NotNull final Class<C> contextClassType,
            @NotNull final Class<I> inputClassType,
            @NotNull final Class<O> outputClassType, final RbacRole<?>... rbacRoles) {
        super(ctx, brokerAddress, serviceName, rbacRoles);

        this.contextClassType = contextClassType;
        this.inputClassType = inputClassType;
        this.outputClassType = outputClassType;
        try {
            emptyInput = inputClassType.getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new IllegalArgumentException("no default constructor for input type " + inputClassType.getName(), e);
        }
        deserialiser.setAutoMatchSerialiser(false);
        serialiser.setAutoMatchSerialiser(false);
        notifySerialiser.setAutoMatchSerialiser(false);
        serialiser.setMatchedIoSerialiser(BinarySerialiser.class);
        notifySerialiser.setMatchedIoSerialiser(BinarySerialiser.class);

        FilterRegistry.checkClassForNewFilters(contextClassType);
        FilterRegistry.checkClassForNewFilters(inputClassType);
        FilterRegistry.checkClassForNewFilters(outputClassType);
        try {
            // check if velocity is available
            Class.forName("org.apache.velocity.app.VelocityEngine");
            setHtmlHandler(new DefaultHtmlHandler<>(this.getClass(), null, null));
        } catch (ClassNotFoundException e) {
            LOGGER.atInfo().addArgument("velocity engine not present - omitting setting DefaultHtmlHandler()");
        }

        super.registerHandler(rawCtx -> {
            final URI reqTopic = rawCtx.req.topic;
            final String queryString = reqTopic.getQuery();
            final C requestCtx = QueryParameterParser.parseQueryParameter(contextClassType, rawCtx.req.topic.getQuery());
            final C replyCtx = QueryParameterParser.parseQueryParameter(contextClassType, rawCtx.req.topic.getQuery()); // reply is initially a copy of request
            final MimeType requestedMimeType = QueryParameterParser.getMimeType(queryString);
            // no MIME type given -> map default to BINARY
            rawCtx.mimeType = requestedMimeType == MimeType.UNKNOWN ? MimeType.BINARY : requestedMimeType;

            final I input = deserialiseData(rawCtx, inputClassType);
            final O output = outputClassType.getDeclaredConstructor().newInstance();

            // call user-handler
            handler.handle(rawCtx, requestCtx, input, replyCtx, output);

            serialiseData(serialiser, defaultBuffer, rawCtx, requestCtx, input, replyCtx, output);
        });
    }

    public Handler<C, I, O> getHandler() {
        return handler;
    }

    public void setHandler(final Handler<C, I, O> handler) {
        this.handler = handler;
    }

    public Handler<C, I, O> getHtmlHandler() {
        return htmlHandler;
    }

    public void setHtmlHandler(Handler<C, I, O> htmlHandler) {
        this.htmlHandler = htmlHandler;
    }

    public void notify(final @NotNull C replyCtx, final @NotNull O reply) throws Exception { // NOPMD part of message signature
        notify("", replyCtx, reply);
    }

    public void notify(final @NotNull String path, final @NotNull C replyCtx, final @NotNull O reply) throws Exception { // NOPMD part of message signature
        final URI notifyTopic = URI.create(serviceName + path);
        final List<URI> subTopics = new ArrayList<>(activeSubscriptions); // copy for decoupling/performance reasons
        final MdpMessage reqCtx = new MdpMessage(null, PROT_CLIENT, SUBSCRIBE, serviceName.getBytes(UTF_8), "notify".getBytes(UTF_8), EMPTY_URI, EMPTY_FRAME, "", RBAC);
        final OpenCmwProtocol.Context rawCtx = new OpenCmwProtocol.Context(reqCtx);
        final Map<URI, Boolean> published = new HashMap<>(activeSubscriptions.size()); // NOPMD - local use of HashMap
        for (final URI endpoint : subTopics) {
            final URI localNotify = QueryParameterParser.appendQueryParameter(notifyTopic, endpoint.getQuery());
            if (!subscriptionMatcher.test(localNotify, endpoint) || published.get(localNotify) != null) {
                // already-notified block further processing of message
                continue;
            }
            published.put(localNotify, true);

            final MimeType requestedMimeType = QueryParameterParser.getMimeType(endpoint.getQuery());
            // no MIME type given -> map default to BINARY
            rawCtx.mimeType = requestedMimeType == MimeType.UNKNOWN ? MimeType.BINARY : requestedMimeType;
            rawCtx.req.topic = rawCtx.mimeType == MimeType.HTML ? QueryParameterParser.appendQueryParameter(notifyTopic, TAG_NO_MENU) : notifyTopic;
            rawCtx.rep.topic = rawCtx.req.topic;
            serialiseData(notifySerialiser, defaultNotifyBuffer, rawCtx, replyCtx, emptyInput, replyCtx, reply);
            super.notify(rawCtx.rep);
        }
    }

    @Override
    public BasicMdpWorker registerHandler(final RequestHandler requestHandler) {
        throw new UnsupportedOperationException("do not overwrite low-level request handler, use either 'setHandler(...)' or " + BasicMdpWorker.class.getName() + " directly");
    }

    protected I deserialiseData(final OpenCmwProtocol.Context rawCtx, final @NotNull Class<I> inputClassType) throws InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        final I input;
        if (rawCtx.req.data.length > 0) {
            switch (rawCtx.mimeType) {
            case HTML:
            case JSON:
            case JSON_LD:
                deserialiser.setDataBuffer(FastByteBuffer.wrap(rawCtx.req.data));
                deserialiser.setMatchedIoSerialiser(JsonSerialiser.class);
                input = deserialiser.deserialiseObject(inputClassType);
                break;
            case CMWLIGHT:
                deserialiser.setDataBuffer(FastByteBuffer.wrap(rawCtx.req.data));
                deserialiser.setMatchedIoSerialiser(CmwLightSerialiser.class);
                input = deserialiser.deserialiseObject(inputClassType);
                break;
            case BINARY:
            default:
                deserialiser.setDataBuffer(FastByteBuffer.wrap(rawCtx.req.data));
                deserialiser.setMatchedIoSerialiser(BinarySerialiser.class);
                input = deserialiser.deserialiseObject(inputClassType);
                break;
            }
        } else {
            // return default input object
            input = inputClassType.getDeclaredConstructor().newInstance();
        }
        return input;
    }

    protected void serialiseData(final IoClassSerialiser classSerialiser, final IoBuffer buffer, final OpenCmwProtocol.Context rawCtx, final C requestCtx, final I input, final C replyCtx, final O output) throws Exception { // NOPMD - part of signature
        final String replyQuery = QueryParameterParser.generateQueryParameter(replyCtx);
        if (rawCtx.rep.topic == null) {
            final URI reqTopic = rawCtx.req.topic;
            rawCtx.rep.topic = new URI(reqTopic.getScheme(), reqTopic.getAuthority(), reqTopic.getPath(), replyQuery, reqTopic.getFragment());
        } else {
            final String oldQuery = rawCtx.rep.topic.getQuery();
            final String newQuery = oldQuery == null || oldQuery.isBlank() ? replyQuery : (oldQuery + "&" + replyQuery);
            rawCtx.rep.topic = new URI(rawCtx.rep.topic.getScheme(), rawCtx.rep.topic.getAuthority(), rawCtx.rep.topic.getPath(), newQuery, null);
        }
        final MimeType replyMimeType = QueryParameterParser.getMimeType(replyQuery);
        // no MIME type given -> stick with the one specified in the request (if it exists) or keep default: copy of raw binary data

        rawCtx.mimeType = replyMimeType == MimeType.UNKNOWN ? rawCtx.mimeType : replyMimeType;
        buffer.reset();
        switch (rawCtx.mimeType) {
        case HTML:
            htmlHandler.handle(rawCtx, requestCtx, input, replyCtx, output);
            break;
        case TEXT:
        case JSON:
        case JSON_LD:
            classSerialiser.setMatchedIoSerialiser(JsonSerialiser.class);
            classSerialiser.getMatchedIoSerialiser().setBuffer(buffer);
            classSerialiser.serialiseObject(output);
            buffer.flip();
            rawCtx.rep.data = Arrays.copyOf(buffer.elements(), buffer.limit() + 4);
            break;
        case CMWLIGHT:
            classSerialiser.setMatchedIoSerialiser(CmwLightSerialiser.class);
            classSerialiser.getMatchedIoSerialiser().setBuffer(buffer);
            classSerialiser.serialiseObject(output);
            buffer.flip();
            rawCtx.rep.data = Arrays.copyOf(buffer.elements(), buffer.limit());
            break;
        case BINARY:
            classSerialiser.setMatchedIoSerialiser(BinarySerialiser.class);
            classSerialiser.getMatchedIoSerialiser().setBuffer(buffer);
            classSerialiser.serialiseObject(output);
            buffer.flip();
            rawCtx.rep.data = Arrays.copyOf(buffer.elements(), buffer.limit());
            break;
        default:
            if (output instanceof BinaryData) {
                rawCtx.rep.data = ((BinaryData) output).data;
            }
            break;
        }
    }

    public interface Handler<C, I, O> {
        void handle(OpenCmwProtocol.Context ctx, C requestCtx, I request, C replyCtx, O reply) throws Exception; // NOPMD NOSONAR - design choice
    }
}
