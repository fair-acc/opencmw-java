package io.opencmw.server;

import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
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
import io.opencmw.serialiser.FieldDescription;
import io.opencmw.serialiser.spi.ClassFieldDescription;
import io.opencmw.serialiser.utils.ClassUtils;

public class DefaultHtmlHandler<C, I, O> implements MajordomoWorker.Handler<C, I, O> {
    private static final String TEMPLATE_DEFAULT = "/velocity/property/defaultPropertyLayout.vm";
    protected VelocityEngine velocityEngine = new VelocityEngine();
    protected final Class<?> mdpWorkerClass;
    protected final Consumer<Map<String, Object>> userContextMapModifier;
    protected final String velocityTemplate;

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

        final HashMap<String, Object> context = new HashMap<>();
        // pre-fill context
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
        context.put("requestClassData", generateQueryParameter(request));
        context.put("replyCtxClassData", generateQueryParameter(replyCtx));
        context.put("replyClassData", generateQueryParameter(reply));
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
            map.put(field, str);
        }
        return map;
    }
}
