package io.opencmw.server.rest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;

import io.opencmw.filter.TimingCtx;
import io.opencmw.rbac.BasicRbacRole;
import io.opencmw.rbac.RbacRole;
import io.opencmw.serialiser.annotations.MetaInfo;
import io.opencmw.server.MajordomoBroker;
import io.opencmw.server.MajordomoWorker;
import io.opencmw.server.rest.helper.ReplyDataType;
import io.opencmw.server.rest.helper.RequestDataType;
import io.opencmw.server.rest.helper.TestContext;

import zmq.util.Utils;

public class MajordomoRestPluginTests {
    private static final Logger LOGGER = LoggerFactory.getLogger(MajordomoRestPluginTests.class);

    public static void main(String[] args) throws IOException {
        MajordomoBroker broker = new MajordomoBroker("TestBroker", "", BasicRbacRole.values());
        final String brokerRouterAddress = broker.bind("tcp://*:" + Utils.findOpenPort());
        broker.bind("mds://*:" + Utils.findOpenPort());
        broker.start();

        MajordomoBroker broker2 = new MajordomoBroker("TestBroker2", brokerRouterAddress, BasicRbacRole.values());
        broker2.bind("tcp://*:" + Utils.findOpenPort());
        broker2.start();

        MajordomoRestPlugin restPlugin = new MajordomoRestPlugin(broker.getContext(), "My test REST server", "*:8080", BasicRbacRole.ADMIN);
        restPlugin.start();
        LOGGER.atInfo().log("Broker and REST plugin started");

        HelloWorldService propertyHandler = new HelloWorldService(broker.getContext());
        propertyHandler.start();
        // TODO: add OpenCMW client requesting binary and json models

        LOGGER.atInfo().log("added services");
    }

    @MetaInfo(unit = "short description", description = "This is an example property implementation.<br>"
                                                        + "Use this as a starting point for implementing your own properties<br>")
    private static class HelloWorldService extends MajordomoWorker<TestContext, RequestDataType, ReplyDataType> {
        public HelloWorldService(final ZContext ctx, final RbacRole<?>... rbacRoles) {
            super(ctx, "helloWorld", TestContext.class, RequestDataType.class, ReplyDataType.class, rbacRoles);

            this.setHandler((rawCtx, reqCtx, in, repCtx, out) -> {
                // LOGGER.atInfo().addArgument(rawCtx).log("received rawCtx  = {}")
                LOGGER.atInfo().addArgument(reqCtx).log("received reqCtx  = {}");
                LOGGER.atInfo().addArgument(in.name).log("received in.name  = {}");

                // some arbitrary data processing
                final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
                out.name = "Hello World! The local time is: " + sdf.format(System.currentTimeMillis());
                out.byteArray = in.name.getBytes(StandardCharsets.UTF_8);
                out.timingCtx = TimingCtx.get("FAIR.SELECTOR.C=3");
                out.timingCtx.bpcts = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());

                repCtx.ctx = out.timingCtx;

                repCtx.testValue = "HelloWorld - reply topic = " + reqCtx.testValue;

                LOGGER.atInfo().addArgument(repCtx).log("received reqCtx  = {}");
                LOGGER.atInfo().addArgument(out.name).log("received out.name = {}");
            });
        }
    }
}
