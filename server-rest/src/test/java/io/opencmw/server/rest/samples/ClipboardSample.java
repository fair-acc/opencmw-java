package io.opencmw.server.rest.samples;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opencmw.MimeType;
import io.opencmw.domain.BinaryData;
import io.opencmw.server.ClipboardWorker;
import io.opencmw.server.rest.test.HelloWorldService;
import io.opencmw.server.rest.test.ImageService;

import com.google.common.io.ByteStreams;

public class ClipboardSample {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClipboardSample.class);
    private static final String CLIPBOARD_SERVICE_NAME = "clipboard";
    private static final String[] TEST_IMAGES = { "testimages/PM5544_test_signal.png", "testimages/SMPTE_Color_Bars.png" };
    private static byte[][] imageData;
    private static final AtomicInteger selectedImage = new AtomicInteger();

    @SuppressWarnings("UnstableApiUsage")
    public static void main(String[] args) throws IOException {
        MajordomoRestPluginSample.launchBroker();
        final ClipboardWorker clipboard = new ClipboardWorker(CLIPBOARD_SERVICE_NAME, MajordomoRestPluginSample.primaryBroker.getContext(), null);
        assertEquals(CLIPBOARD_SERVICE_NAME, clipboard.getServiceName());
        clipboard.start();

        MajordomoRestPluginSample.restPlugin.getRootService().set("/clipboard");
        MajordomoRestPluginSample.restPlugin.getMenuMap().put("COMMON_NAV_ALLIMAGES", "/clipboard");
        MajordomoRestPluginSample.restPlugin.getMenuMap().put("COMMON_NAV_UPLOAD", "/clipboard/upload");
        MajordomoRestPluginSample.restPlugin.getMenuMap().put("Properties", "/mmi.service");
        MajordomoRestPluginSample.restPlugin.getMenuMap().put("Test", "/testImage");

        imageData = new byte[TEST_IMAGES.length][];
        for (int i = 0; i < TEST_IMAGES.length; i++) {
            try (final InputStream in = ImageService.class.getResourceAsStream(TEST_IMAGES[i])) {
                //noinspection UnstableApiUsage
                imageData[i] = ByteStreams.toByteArray(in); // NOPMD NOSONAR
                LOGGER.atInfo().addArgument(TEST_IMAGES[i]).addArgument(imageData[i].length).log("read test image file: '{}' - bytes: {}");
            } catch (IOException e) {
                LOGGER.atError().setCause(e).addArgument(TEST_IMAGES[i]).log("could not read test image file: '{}'");
            }
        }
        final Timer timer = new Timer(true);
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                selectedImage.set(selectedImage.incrementAndGet() % imageData.length);

                BinaryData reply = new BinaryData("/TestImages/test.png", MimeType.PNG, imageData[selectedImage.get()]);
                clipboard.uploadAndNotifyData(reply, "");
            }
        }, 1000, 10_000);

        // start simple test services/properties
        final HelloWorldService helloWorldService = new HelloWorldService(MajordomoRestPluginSample.primaryBroker.getContext());
        helloWorldService.start();
        final ImageService imageService = new ImageService(MajordomoRestPluginSample.primaryBroker.getContext(), 2000);
        imageService.start();

        LOGGER.atInfo().log(CLIPBOARD_SERVICE_NAME + " service started: " + clipboard.isAlive());
    }
}
