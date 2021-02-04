package io.opencmw.server.rest;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;

import io.opencmw.MimeType;
import io.opencmw.domain.BinaryData;
import io.opencmw.domain.NoData;
import io.opencmw.filter.TimingCtx;
import io.opencmw.rbac.BasicRbacRole;
import io.opencmw.rbac.RbacRole;
import io.opencmw.serialiser.annotations.MetaInfo;
import io.opencmw.server.MajordomoBroker;
import io.opencmw.server.MajordomoWorker;
import io.opencmw.server.rest.helper.ReplyDataType;
import io.opencmw.server.rest.helper.RequestDataType;
import io.opencmw.server.rest.helper.TestContext;

import com.google.common.io.ByteStreams;

import zmq.util.Utils;

public class MajordomoRestPluginTests {
    private static final Logger LOGGER = LoggerFactory.getLogger(MajordomoRestPluginTests.class);

    public static void main(String[] args) throws IOException {
        MajordomoBroker primaryBroker = new MajordomoBroker("PrimaryBroker", "", BasicRbacRole.values());
        final String brokerRouterAddress = primaryBroker.bind("tcp://*:" + Utils.findOpenPort());
        primaryBroker.bind("mds://*:" + Utils.findOpenPort());
        MajordomoRestPlugin restPlugin = new MajordomoRestPlugin(primaryBroker.getContext(), "My test REST server", "*:8080", BasicRbacRole.ADMIN);
        primaryBroker.start();
        restPlugin.start();
        LOGGER.atInfo().log("Broker and REST plugin started");

        // start simple test services/properties
        final HelloWorldService helloWorldService = new HelloWorldService(primaryBroker.getContext());
        helloWorldService.start();
        final ImageService imageService = new ImageService(primaryBroker.getContext());
        imageService.start();

        // TODO: add OpenCMW client requesting binary and json models

        // second broker to test DNS functionalities
        MajordomoBroker secondaryBroker = new MajordomoBroker("SecondaryTestBroker", brokerRouterAddress, BasicRbacRole.values());
        secondaryBroker.bind("tcp://*:" + Utils.findOpenPort());
        secondaryBroker.start();

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
                final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX", Locale.UK);
                out.name = "Hello World! The local time is: " + sdf.format(System.currentTimeMillis());
                out.byteArray = in.name.getBytes(StandardCharsets.UTF_8);
                out.byteReturnType = 42;
                out.timingCtx = TimingCtx.get("FAIR.SELECTOR.C=3");
                out.timingCtx.bpcts = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
                out.lsaContext = "MYFAIRCONTEXT.C1";

                repCtx.ctx = out.timingCtx;
                repCtx.contentType = reqCtx.contentType;
                repCtx.testFilter = "HelloWorld - reply topic = " + reqCtx.testFilter;

                LOGGER.atInfo().addArgument(repCtx).log("received repCtx  = {}");
                LOGGER.atInfo().addArgument(out.name).log("received out.name = {}");
            });
        }
    }

    @MetaInfo(unit = "test image service", description = "Simple test image service that rotates through"
                                                         + " a couple of pre-defined images @0.5 Hz")
    private static class ImageService extends MajordomoWorker<TestContext, NoData, BinaryData> {
        private static final String PROPERTY_NAME = "testImage";
        private static final String[] TEST_IMAGES = { "testimages/PM5544_test_signal.png", "testimages/SMPTE_Color_Bars.png" };
        private final byte[][] imageData;
        private final AtomicInteger selectedImage = new AtomicInteger();

        public ImageService(final ZContext ctx, final RbacRole<?>... rbacRoles) {
            super(ctx, PROPERTY_NAME, TestContext.class, NoData.class, BinaryData.class, rbacRoles);
            imageData = new byte[TEST_IMAGES.length][];
            for (int i = 0; i < TEST_IMAGES.length; i++) {
                try (final InputStream in = this.getClass().getResourceAsStream(TEST_IMAGES[i])) {
                    imageData[i] = ByteStreams.toByteArray(in);
                    LOGGER.atInfo().addArgument(TEST_IMAGES[i]).addArgument(imageData[i].length).log("read test image file: '{}' - bytes: {}");
                } catch (IOException e) {
                    LOGGER.atError().setCause(e).addArgument(TEST_IMAGES[i]).log("could not read test image file: '{}'");
                }
            }

            final Timer timer = new Timer();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    selectedImage.set(selectedImage.incrementAndGet() % imageData.length);
                    final TestContext replyCtx = new TestContext();
                    BinaryData reply = new BinaryData();
                    reply.resourceName = "test.png";
                    reply.data = imageData[selectedImage.get()]; //TODO rotate through images
                    replyCtx.contentType = MimeType.PNG;
                    ImageService.this.notify(replyCtx, reply);
                    // System.err.println("notify new image " + replyCtx)
                }
            }, TimeUnit.SECONDS.toMillis(1), TimeUnit.SECONDS.toMillis(2));

            this.setHandler((rawCtx, reqCtx, in, repCtx, reply) -> {
                final String path = StringUtils.stripStart(rawCtx.req.topic.getPath(), "/");
                LOGGER.atTrace().addArgument(reqCtx).addArgument(path).log("received reqCtx  = {} - path='{}'");

                reply.resourceName = StringUtils.stripStart(StringUtils.stripStart(path, PROPERTY_NAME), "/");
                reply.data = imageData[selectedImage.get()]; //TODO rotate through images
                reply.contentType = MimeType.PNG;

                if (reply.resourceName.contains(".")) {
                    repCtx.contentType = reply.contentType;
                } else {
                    repCtx.contentType = reqCtx.contentType;
                }
            });
        }
    }
}
