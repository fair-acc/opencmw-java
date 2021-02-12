package io.opencmw.server.rest.util;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

import javax.servlet.AsyncEvent;
import javax.servlet.AsyncListener;

import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.http.sse.SseClient;
import io.opencmw.MimeType;
import io.opencmw.server.rest.RestServer;

/**
 * Combined GET and SSE request handler. 
 * 
 * N.B. This based on an original idea/implementation found in Javalin's {@link io.javalin.http.sse.SseHandler}.
 * 
 * @author rstein 
 * 
 * @see io.javalin.http.sse.SseHandler
 */
public class CombinedHandler implements Handler {
    private static final Logger LOGGER = LoggerFactory.getLogger(CombinedHandler.class);
    private final Handler getHandler;
    private BiConsumer<SseClient, SseState> sseClientConnectHandler;

    private final Consumer<SseClient> clientConsumer = client -> {
        // TODO: upgrade to path & query matching - for the time being only path @see also MajordomoRestPlugin
        // final String queryString = client.ctx.queryString() == null ? "" : ("?" + client.ctx.queryString())
        // final String endPointName = StringUtils.stripEnd(client.ctx.path(), "/") + queryString
        final String endPointName = StringUtils.stripEnd(client.ctx.path(), "/");

        RestServer.getEventClients(endPointName).add(client);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.atDebug().addArgument(client.ctx.req.getRemoteHost()).addArgument(endPointName).log("added SSE client: '{}' to route '{}'");
        }

        if (sseClientConnectHandler != null) {
            sseClientConnectHandler.accept(client, SseState.CONNECTED);
        }
        client.sendEvent("connected", "Hello, new SSE client " + client.ctx.req.getRemoteHost());

        client.onClose(() -> {
            if (sseClientConnectHandler != null) {
                sseClientConnectHandler.accept(client, SseState.DISCONNECTED);
            }
            RestServer.getEventClients(endPointName).remove(client);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.atDebug().addArgument(client.ctx.req.getRemoteHost()).addArgument(endPointName).log("removed client: '{}' from route '{}'");
            }
        });
    };

    public CombinedHandler(@NotNull Handler getHandler) {
        this(getHandler, null);
    }

    public CombinedHandler(@NotNull Handler getHandler, BiConsumer<SseClient, SseState> sseClientConnectHandler) {
        this.getHandler = getHandler;
        this.sseClientConnectHandler = sseClientConnectHandler;
    }

    @Override
    public void handle(@NotNull Context ctx) throws Exception {
        if (MimeType.EVENT_STREAM.equals(RestServer.getRequestedMimeProtocol(ctx))) {
            ctx.res.setStatus(200);
            ctx.res.setCharacterEncoding("UTF-8");
            ctx.res.setContentType(MimeType.EVENT_STREAM.toString());
            ctx.res.addHeader(Header.CONNECTION, "close");
            ctx.res.addHeader(Header.CACHE_CONTROL, "no-cache");
            ctx.res.flushBuffer();

            ctx.req.startAsync(ctx.req, ctx.res);
            ctx.req.getAsyncContext().setTimeout(0);
            clientConsumer.accept(new SseClient(ctx));

            ctx.req.getAsyncContext().addListener(new AsyncListener() {
                @Override
                public void onComplete(AsyncEvent event) { /* not needed */
                }
                @Override
                public void onError(AsyncEvent event) {
                    event.getAsyncContext().complete();
                }
                @Override
                public void onStartAsync(AsyncEvent event) { /* not needed */
                }
                @Override
                public void onTimeout(AsyncEvent event) {
                    event.getAsyncContext().complete();
                }
            });
            return;
        }

        getHandler.handle(ctx);
    }

    public enum SseState {
        CONNECTED,
        DISCONNECTED
    }
}