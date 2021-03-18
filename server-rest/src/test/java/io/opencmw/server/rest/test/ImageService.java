package io.opencmw.server.rest.test;

import java.io.IOException;
import java.io.InputStream;
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
import io.opencmw.rbac.RbacRole;
import io.opencmw.serialiser.annotations.MetaInfo;
import io.opencmw.server.MajordomoWorker;
import io.opencmw.server.rest.helper.TestContext;

import com.google.common.io.ByteStreams;

@MetaInfo(unit = "test image service", description = "Simple test image service that rotates through"
                                                     + " a couple of pre-defined images @0.5 Hz")
public class ImageService extends MajordomoWorker<TestContext, NoData, BinaryData> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ImageService.class);
    public static final String PROPERTY_NAME = "testImage";
    private static final String[] TEST_IMAGES = { "testimages/PM5544_test_signal.png", "testimages/SMPTE_Color_Bars.png" };
    private final byte[][] imageData;
    private final AtomicInteger selectedImage = new AtomicInteger();

    @SuppressWarnings("UnstableApiUsage")
    public ImageService(final ZContext ctx, final int updateInterval, final RbacRole<?>... rbacRoles) {
        super(ctx, PROPERTY_NAME, TestContext.class, NoData.class, BinaryData.class, rbacRoles);
        imageData = new byte[TEST_IMAGES.length][];
        for (int i = 0; i < TEST_IMAGES.length; i++) {
            try (final InputStream in = this.getClass().getResourceAsStream(TEST_IMAGES[i])) {
                imageData[i] = ByteStreams.toByteArray(in); // NOPMD NOSONAR
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
                reply.data = imageData[selectedImage.get()];
                replyCtx.contentType = MimeType.PNG;
                try {
                    ImageService.this.notify(replyCtx, reply);
                } catch (Exception e) {
                    LOGGER.atError().setCause(e).log("could not notify update");
                }
            }
        }, TimeUnit.MILLISECONDS.toMillis(updateInterval), TimeUnit.MILLISECONDS.toMillis(updateInterval));

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
