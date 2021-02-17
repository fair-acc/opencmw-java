package io.opencmw.server;

import static io.opencmw.OpenCmwProtocol.Command.W_NOTIFY;
import static io.opencmw.OpenCmwProtocol.EMPTY_FRAME;
import static io.opencmw.OpenCmwProtocol.MdpSubProtocol.PROT_WORKER;

import java.net.URI;
import java.util.Arrays;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;

import io.opencmw.MimeType;
import io.opencmw.OpenCmwProtocol;
import io.opencmw.OpenCmwProtocol.MdpMessage;
import io.opencmw.QueryParameterParser;
import io.opencmw.domain.BinaryData;
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
@SuppressWarnings({ "PMD.DataClass", "PMD.NPathComplexity" }) // PMD - false positive data class
@MetaInfo(description = "default MajordomoWorker implementation")
public class MajordomoWorker<C, I, O> extends BasicMdpWorker {
    private static final Logger LOGGER = LoggerFactory.getLogger(MajordomoWorker.class);
    private static final int MAX_BUFFER_SIZE = 4000;
    protected final IoBuffer defaultBuffer = new FastByteBuffer(MAX_BUFFER_SIZE, true, null);
    protected final IoBuffer defaultNotifyBuffer = new FastByteBuffer(MAX_BUFFER_SIZE, true, null);
    protected final IoClassSerialiser deserialiser = new IoClassSerialiser(defaultBuffer);
    protected final IoClassSerialiser serialiser = new IoClassSerialiser(defaultBuffer);
    protected final IoClassSerialiser notifySerialiser = new IoClassSerialiser(defaultNotifyBuffer);
    protected final Class<C> contextClassType;
    protected final Class<I> inputClassType;
    protected final Class<O> outputClassType;
    protected Handler<C, I, O> handler;
    protected Handler<C, I, O> htmlHandler;

    public MajordomoWorker(final String brokerAddress, final String serviceName,
            @NotNull final Class<C> contextClassType,
            @NotNull final Class<I> inputClassType,
            @NotNull final Class<O> outputClassType, final RbacRole<?>... rbacRoles) {
        this(null, brokerAddress, serviceName, contextClassType, inputClassType, outputClassType, rbacRoles);
    }

    public MajordomoWorker(final ZContext ctx, final String serviceName,
            @NotNull final Class<C> contextClassType,
            @NotNull final Class<I> inputClassType,
            @NotNull final Class<O> outputClassType, final RbacRole<?>... rbacRoles) {
        this(ctx, "inproc://broker", serviceName, contextClassType, inputClassType, outputClassType, rbacRoles);
    }

    @SuppressWarnings({ "PMD.ExcessiveMethodLength", "PMD.PrematureDeclaration" })
    protected MajordomoWorker(final ZContext ctx, final String brokerAddress, final String serviceName,
            @NotNull final Class<C> contextClassType,
            @NotNull final Class<I> inputClassType,
            @NotNull final Class<O> outputClassType, final RbacRole<?>... rbacRoles) {
        super(ctx, brokerAddress, serviceName, rbacRoles);

        this.contextClassType = contextClassType;
        this.inputClassType = inputClassType;
        this.outputClassType = outputClassType;
        deserialiser.setAutoMatchSerialiser(false);
        serialiser.setAutoMatchSerialiser(false);
        notifySerialiser.setAutoMatchSerialiser(false);
        serialiser.setMatchedIoSerialiser(BinarySerialiser.class);
        notifySerialiser.setMatchedIoSerialiser(BinarySerialiser.class);

        try {
            // check if velocity is available
            Class.forName("org.apache.velocity.app.VelocityEngine");
            setHtmlHandler(new DefaultHtmlHandler<>(this.getClass(), null, null));
        } catch (ClassNotFoundException e) {
            LOGGER.atInfo().addArgument("velocity engine not present - omitting setting DefaultHtmlHandler()");
        }

        super.registerHandler(c -> {
            final URI reqTopic = c.req.topic;
            final String queryString = reqTopic.getQuery();
            final C requestCtx = QueryParameterParser.parseQueryParameter(contextClassType, c.req.topic.getQuery());
            final C replyCtx = QueryParameterParser.parseQueryParameter(contextClassType, c.req.topic.getQuery()); // reply is initially a copy of request
            final MimeType requestedMimeType = QueryParameterParser.getMimeType(queryString);
            // no MIME type given -> map default to BINARY
            c.mimeType = requestedMimeType == MimeType.UNKNOWN ? MimeType.BINARY : requestedMimeType;

            final I input;
            if (c.req.data.length > 0) {
                switch (c.mimeType) {
                case HTML:
                case JSON:
                case JSON_LD:
                    deserialiser.setDataBuffer(FastByteBuffer.wrap(c.req.data));
                    deserialiser.setMatchedIoSerialiser(JsonSerialiser.class);
                    input = deserialiser.deserialiseObject(inputClassType);
                    break;
                case CMWLIGHT:
                    deserialiser.setDataBuffer(FastByteBuffer.wrap(c.req.data));
                    deserialiser.setMatchedIoSerialiser(CmwLightSerialiser.class);
                    input = deserialiser.deserialiseObject(inputClassType);
                    break;
                case BINARY:
                default:
                    deserialiser.setDataBuffer(FastByteBuffer.wrap(c.req.data));
                    deserialiser.setMatchedIoSerialiser(BinarySerialiser.class);
                    input = deserialiser.deserialiseObject(inputClassType);
                    break;
                }
            } else {
                // return default input object
                input = inputClassType.getDeclaredConstructor().newInstance();
            }

            final O output = outputClassType.getDeclaredConstructor().newInstance();

            // call user-handler
            handler.handle(c, requestCtx, input, replyCtx, output);

            final String replyQuery = QueryParameterParser.generateQueryParameter(replyCtx);
            if (c.rep.topic == null) {
                c.rep.topic = new URI(reqTopic.getScheme(), reqTopic.getAuthority(), reqTopic.getPath(), replyQuery, reqTopic.getFragment());
            } else {
                final String oldQuery = c.rep.topic.getQuery();
                final String newQuery = oldQuery == null || oldQuery.isBlank() ? replyQuery : (oldQuery + "&" + replyQuery);
                c.rep.topic = new URI(c.rep.topic.getScheme(), c.rep.topic.getAuthority(), c.rep.topic.getPath(), newQuery, reqTopic.getFragment());
            }
            final MimeType replyMimeType = QueryParameterParser.getMimeType(replyQuery);
            // no MIME type given -> stick with the one specified in the request (if it exists) or keep default: BINARY
            c.mimeType = replyMimeType == MimeType.UNKNOWN ? c.mimeType : replyMimeType;

            defaultBuffer.reset();
            switch (c.mimeType) {
            case HTML:
                htmlHandler.handle(c, requestCtx, input, replyCtx, output);
                break;
            case TEXT:
            case JSON:
            case JSON_LD:
                serialiser.setMatchedIoSerialiser(JsonSerialiser.class);
                serialiser.getMatchedIoSerialiser().setBuffer(defaultBuffer);
                serialiser.serialiseObject(output);
                defaultBuffer.flip();
                c.rep.data = Arrays.copyOf(defaultBuffer.elements(), defaultBuffer.limit() + 4);
                break;
            case CMWLIGHT:
                serialiser.setMatchedIoSerialiser(CmwLightSerialiser.class);
                serialiser.getMatchedIoSerialiser().setBuffer(defaultBuffer);
                serialiser.serialiseObject(output);
                defaultBuffer.flip();
                c.rep.data = Arrays.copyOf(defaultBuffer.elements(), defaultBuffer.limit());
                break;
            case BINARY:
                serialiser.setMatchedIoSerialiser(BinarySerialiser.class);
                serialiser.getMatchedIoSerialiser().setBuffer(defaultBuffer);
                serialiser.serialiseObject(output);
                defaultBuffer.flip();
                c.rep.data = Arrays.copyOf(defaultBuffer.elements(), defaultBuffer.limit());
                break;
            default:
                if (output instanceof BinaryData) {
                    c.rep.data = ((BinaryData) output).data;
                }
                break;
            }
        });
    }

    @Override
    public BasicMdpWorker registerHandler(final RequestHandler requestHandler) {
        throw new UnsupportedOperationException("do not overwrite low-level request handler, use either 'setHandler(...)' or " + BasicMdpWorker.class.getName() + " directly");
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

    public void notify(final @NotNull C replyCtx, final @NotNull O reply) {
        notify("", replyCtx, reply);
    }

    public void notify(final @NotNull String path, final @NotNull C replyCtx, final @NotNull O reply) {
        defaultNotifyBuffer.reset();
        notifySerialiser.serialiseObject(reply);
        defaultNotifyBuffer.flip();
        final byte[] data = Arrays.copyOf(defaultNotifyBuffer.elements(), defaultNotifyBuffer.limit());
        final String query = QueryParameterParser.generateQueryParameter(replyCtx);
        URI topic = URI.create(serviceName + path + (query.isBlank() ? "" : ('?' + query)));
        super.notify(new MdpMessage(null, PROT_WORKER, W_NOTIFY, serviceBytes, EMPTY_FRAME, topic, data, "", RBAC));
    }

    public interface Handler<C, I, O> {
        void handle(OpenCmwProtocol.Context ctx, C requestCtx, I request, C replyCtx, O reply) throws Exception; // NOPMD NOSONAR - design choice
    }
}
