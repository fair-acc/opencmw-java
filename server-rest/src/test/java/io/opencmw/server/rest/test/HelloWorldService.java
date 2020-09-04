package io.opencmw.server.rest.test;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;

import io.opencmw.OpenCmwProtocol;
import io.opencmw.filter.TimingCtx;
import io.opencmw.rbac.RbacRole;
import io.opencmw.serialiser.annotations.MetaInfo;
import io.opencmw.server.MajordomoWorker;
import io.opencmw.server.rest.helper.ReplyDataType;
import io.opencmw.server.rest.helper.RequestDataType;
import io.opencmw.server.rest.helper.TestContext;

@MetaInfo(unit = "short description", description = "This is an example property implementation.<br>"
                                                    + "Use this as a starting point for implementing your own properties<br>")
public class HelloWorldService extends MajordomoWorker<TestContext, RequestDataType, ReplyDataType> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HelloWorldService.class);
    private String customFilter = "uninitialised";
    public HelloWorldService(final ZContext ctx, final RbacRole<?>... rbacRoles) {
        super(ctx, "helloWorld", TestContext.class, RequestDataType.class, ReplyDataType.class, rbacRoles);

        this.setHandler((rawCtx, reqCtx, in, repCtx, out) -> {
            // LOGGER.atInfo().addArgument(rawCtx).log("received rawCtx  = {}")
            LOGGER.atInfo().addArgument(reqCtx).log("received reqCtx  = {}");
            LOGGER.atInfo().addArgument(in.name).log("received in.name  = {}");

            // some arbitrary data processing
            final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX", Locale.UK);
            out.name = "Hello World! The local time is: " + sdf.format(System.currentTimeMillis());
            out.byteArray = in.name.getBytes(StandardCharsets.UTF_8);
            out.byteReturnType = 42;
            out.timingCtx = TimingCtx.get("FAIR.SELECTOR.C=3");
            out.timingCtx.bpcts = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
            if (rawCtx.req.command == OpenCmwProtocol.Command.SET_REQUEST) {
                // poor man's local setting management
                customFilter = in.customFilter;
            }
            out.lsaContext = customFilter;

            repCtx.ctx = out.timingCtx;
            repCtx.contentType = reqCtx.contentType;
            repCtx.testFilter = "HelloWorld - reply topic = " + reqCtx.testFilter;

            LOGGER.atInfo().addArgument(repCtx).log("received repCtx  = {}");
            LOGGER.atInfo().addArgument(out.name).log("received out.name = {}");
        });
    }
}
