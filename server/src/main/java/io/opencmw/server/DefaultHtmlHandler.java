package io.opencmw.server;

import java.io.StringWriter;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.apache.commons.lang3.StringUtils;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.zeromq.util.ZData;

import io.opencmw.OpenCmwProtocol;
import io.opencmw.domain.BinaryData;
import io.opencmw.serialiser.FieldDescription;
import io.opencmw.serialiser.spi.ClassFieldDescription;
import io.opencmw.serialiser.utils.ClassUtils;

public class DefaultHtmlHandler<C, I, O> implements MajordomoWorker.Handler<C, I, O> {
    public static final String NO_MENU = "noMenu";
    private static final String TEMPLATE_DEFAULT = "/velocity/property/defaultPropertyLayout.vm";
    protected final Class<?> mdpWorkerClass;
    protected final Consumer<Map<String, Object>> userContextMapModifier;
    protected final String velocityTemplate;
    protected VelocityEngine velocityEngine = new VelocityEngine();

    public DefaultHtmlHandler(final Class<?> mdpWorkerClass, final String velocityTemplate, final Consumer<Map<String, Object>> userContextMapModifier) {
        this.mdpWorkerClass = mdpWorkerClass;
        this.velocityTemplate = velocityTemplate == null ? TEMPLATE_DEFAULT : velocityTemplate;
        this.userContextMapModifier = userContextMapModifier;
        velocityEngine.setProperty("resource.loaders", "class");
        velocityEngine.setProperty("resource.loader.class.class", "org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader");
        velocityEngine.setProperty("velocimacro.library.autoreload", "true");
        velocityEngine.setProperty("resource.loader.file.cache", "false");
        velocityEngine.setProperty("velocimacro.inline.replace_global", "true");
        velocityEngine.init();
    }

    @Override
    public void handle(OpenCmwProtocol.Context rawCtx, C requestCtx, I request, C replyCtx, O reply) {
        final ClassFieldDescription fieldDescription = mdpWorkerClass == null ? null : ClassUtils.getFieldDescription(mdpWorkerClass);
        final String queryString = rawCtx.req.topic.getQuery();
        final boolean noMenu = queryString != null && queryString.contains(NO_MENU);

        final HashMap<String, Object> context = new HashMap<>();
        // pre-fill context

        context.put(NO_MENU, noMenu);
        try {
            context.put("requestedURI", rawCtx.req.topic.toString());
            context.put("requestedURInoFrame", QueryParameterParser.appendQueryParameter(rawCtx.req.topic, NO_MENU).toString());
        } catch (URISyntaxException e) {
            throw new IllegalStateException("appendURI error for " + rawCtx.req.topic, e);
        }
        context.put("service", StringUtils.stripStart(rawCtx.req.topic.getPath(), "/"));
        context.put("mdpClass", mdpWorkerClass);
        context.put("mdpMetaData", fieldDescription);
        context.put("mdpCommand", rawCtx.req.command);
        context.put("clientRequestID", ZData.toString(rawCtx.req.clientRequestID));

        context.put("requestTopic", rawCtx.req.topic);
        context.put("replyTopic", rawCtx.rep.topic);
        context.put("requestCtx", requestCtx);
        context.put("replyCtx", replyCtx);
        context.put("request", request);
        context.put("reply", reply);
        context.put("requestCtxClassData", generateQueryParameter(requestCtx));
        context.put("replyCtxClassData", generateQueryParameter(replyCtx));

        context.put("requestClassData", generateQueryParameter(request));
        if (BinaryData.class.equals(request.getClass())) {
            final byte[] rawData = ((BinaryData) request).data;
            context.put("requestMimeType", ((BinaryData) reply).contentType.toString());
            context.put("requestResourceName", ((BinaryData) reply).resourceName);
            context.put("requestRawData", Base64.getEncoder().encodeToString(rawData));
        }

        context.put("replyClassData", generateQueryParameter(reply));
        if (BinaryData.class.equals(reply.getClass())) {
            final byte[] rawData = ((BinaryData) reply).data;
            context.put("replyMimeType", ((BinaryData) reply).contentType.toString());
            context.put("replyResourceName", ((BinaryData) reply).resourceName);
            context.put("replyRawData", Base64.getEncoder().encodeToString(rawData));
        }

        if (userContextMapModifier != null) {
            userContextMapModifier.accept(context);
        }

        StringWriter writer = new StringWriter();
        velocityEngine.getTemplate(velocityTemplate).merge(new VelocityContext(context), writer);
        String returnVal = writer.toString();
        rawCtx.rep.data = returnVal.getBytes(StandardCharsets.UTF_8);
    }

    public static Map<ClassFieldDescription, String> generateQueryParameter(Object obj) {
        final ClassFieldDescription fieldDescription = ClassUtils.getFieldDescription(obj.getClass());
        final Map<ClassFieldDescription, String> map = new HashMap<>(); // NOPMD - no concurrent access, used in a single thread and is then destroyed
        final List<FieldDescription> children = fieldDescription.getChildren();
        for (FieldDescription child : children) {
            ClassFieldDescription field = (ClassFieldDescription) child;
            final BiFunction<Object, ClassFieldDescription, String> mapFunction = QueryParameterParser.CLASS_TO_STRING_CONVERTER.get(field.getType());
            final String str;
            if (mapFunction == null) {
                str = QueryParameterParser.CLASS_TO_STRING_CONVERTER.get(Object.class).apply(obj, field);
            } else {
                str = mapFunction.apply(obj, field);
            }
            map.put(field, StringUtils.stripEnd(StringUtils.stripStart(str, "\"["), "\"]"));
        }
        return map;
    }
}
