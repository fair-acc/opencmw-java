package io.opencmw.server;

import static org.junit.jupiter.api.Assertions.*;

import static io.opencmw.OpenCmwProtocol.Command.GET_REQUEST;
import static io.opencmw.OpenCmwProtocol.Command.SET_REQUEST;

import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opencmw.MimeType;
import io.opencmw.OpenCmwProtocol;
import io.opencmw.domain.BinaryData;
import io.opencmw.domain.NoData;
import io.opencmw.rbac.BasicRbacRole;
import io.opencmw.server.domain.BasicCtx;

import zmq.util.Utils;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ClipboardWorkerTests {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClipboardWorkerTests.class);
    private static final String CLIPBOARD_SERVICE_NAME = "clipboard";
    private MajordomoBroker broker;
    private URI extBrokerRouterAddress;
    private URI extBrokerPublisherAddress;

    @BeforeAll
    @Timeout(10)
    void init() throws IOException {
        broker = new MajordomoBroker("TestMdpBroker", null, BasicRbacRole.values());
        // broker.setDaemon(true); // use this if running in another app that
        // controls threads Can be called multiple times with different endpoints
        extBrokerRouterAddress = broker.bind(URI.create("mdp://localhost:" + Utils.findOpenPort()));
        extBrokerPublisherAddress = broker.bind(URI.create("mds://localhost:" + Utils.findOpenPort()));
        final ClipboardWorker clipboard = new ClipboardWorker(CLIPBOARD_SERVICE_NAME, broker.getContext(), null);
        assertEquals(CLIPBOARD_SERVICE_NAME, clipboard.getServiceName());

        broker.start();
        clipboard.start();

        // wait until all sockets and services are initialised
        LOGGER.atDebug().addArgument(extBrokerRouterAddress).log("ROUTER socket bound to: {}");
        LOGGER.atDebug().addArgument(extBrokerPublisherAddress).log("PUB socket bound to: {}");
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
    }

    @AfterAll
    @Timeout(10)
    void finish() {
        broker.stopBroker();
    }

    @Test
    void testSubmitExternal() {
        final BinaryData emptyDomainObject = new BinaryData();
        final BinaryData domainObject = new BinaryData("/testData.bin", MimeType.BINARY, new byte[] { 1, 2, 3, 4 });
        MajordomoTestClientSync client = new MajordomoTestClientSync(extBrokerRouterAddress, "customClientName");

        MajordomoTestClientSubscription<BinaryData> subClientRoot = new MajordomoTestClientSubscription<>(extBrokerPublisherAddress, "subClientRoot", BinaryData.class);
        MajordomoTestClientSubscription<BinaryData> subClientMatching = new MajordomoTestClientSubscription<>(extBrokerPublisherAddress, "subClientMatching", BinaryData.class);
        MajordomoTestClientSubscription<BinaryData> subClientNotMatching = new MajordomoTestClientSubscription<>(extBrokerPublisherAddress, "subClientNotMatching", BinaryData.class);
        subClientRoot.subscribe(CLIPBOARD_SERVICE_NAME + "*");
        subClientMatching.subscribe(CLIPBOARD_SERVICE_NAME + domainObject.resourceName);
        subClientNotMatching.subscribe(CLIPBOARD_SERVICE_NAME + "/differentData.bin");

        // wait to let subscription initialise
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));

        BinaryData replyMsg = client.send(SET_REQUEST, CLIPBOARD_SERVICE_NAME, domainObject, BinaryData.class);
        assertEquals(emptyDomainObject, replyMsg); // check that a default object has been returned

        replyMsg = client.send(GET_REQUEST, CLIPBOARD_SERVICE_NAME + domainObject.resourceName, new NoData(), BinaryData.class);
        assertEquals(domainObject, replyMsg); // check that a return objects are identical

        // send another object to the clipboard
        domainObject.resourceName = "/otherData.bin";
        client.send(SET_REQUEST, CLIPBOARD_SERVICE_NAME, domainObject, BinaryData.class);
        replyMsg = client.send(GET_REQUEST, CLIPBOARD_SERVICE_NAME + domainObject.resourceName, new NoData(), BinaryData.class);
        assertEquals(domainObject, replyMsg); // check that a return objects are identical

        // check subscription result
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(200));

        assertEquals(2, subClientRoot.mdpMessages.size(), "raw message count");
        assertEquals(2, subClientRoot.domainMessages.size(), "domain message count");
        assertEquals(1, subClientMatching.mdpMessages.size(), "matching message count");
        assertEquals(1, subClientMatching.domainMessages.size(), "matching message count");
        assertEquals(0, subClientNotMatching.mdpMessages.size(), "non-matching message count");
        assertEquals(0, subClientNotMatching.domainMessages.size(), "non-matching message count");

        // publish null name
        domainObject.resourceName = null;
        client.send(GET_REQUEST, CLIPBOARD_SERVICE_NAME, new NoData(), BinaryData.class);
        domainObject.resourceName = "";
        client.send(GET_REQUEST, CLIPBOARD_SERVICE_NAME, new NoData(), BinaryData.class);

        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
        assertEquals(2, subClientRoot.domainMessages.size(), "notified erroneous data");

        OpenCmwProtocol.MdpMessage htmlResult = client.send(GET_REQUEST, CLIPBOARD_SERVICE_NAME + domainObject.resourceName + "?contentType=text/html", OpenCmwProtocol.EMPTY_FRAME);
        assertNotNull(htmlResult.data);
        final String htmlData = new String(htmlResult.data, StandardCharsets.UTF_8);
        assertTrue(htmlData.contains("/testData.bin"));
        assertTrue(htmlData.contains("/otherData.bin"));
        htmlResult = client.send(GET_REQUEST, CLIPBOARD_SERVICE_NAME + "subcategory/upload?contentType=text/html", OpenCmwProtocol.EMPTY_FRAME);
        assertNotNull(htmlResult.data);
        htmlResult = client.send(GET_REQUEST, CLIPBOARD_SERVICE_NAME + domainObject.resourceName + "?sse&contentType=text/html", OpenCmwProtocol.EMPTY_FRAME);
        assertNotNull(htmlResult.data);
        htmlResult = client.send(GET_REQUEST, CLIPBOARD_SERVICE_NAME + domainObject.resourceName + "?longPolling=1000&contentType=text/html", OpenCmwProtocol.EMPTY_FRAME);
        assertNotNull(htmlResult.data);

        subClientRoot.stopClient();
    }

    @Test
    void testDataContainer() {
        final BinaryData domainObject = new BinaryData("/testData.bin", MimeType.BINARY, new byte[] { 1, 2, 3, 4 });
        assertDoesNotThrow(() -> new ClipboardWorker.DataContainer(domainObject));

        final ClipboardWorker.DataContainer container = new ClipboardWorker.DataContainer(domainObject);
        assertEquals(domainObject.resourceName, container.getResourceName());
        assertEquals(domainObject.resourceName, URLDecoder.decode(container.getEncodedResourceName(), StandardCharsets.UTF_8));
        assertEquals(domainObject.resourceName.substring(1), container.getFileName());
        assertEquals(domainObject.resourceName.substring(1), container.getFileName());
        assertNotNull(container.getTimeStamp());
        assertNotNull(container.getMimeData());
    }

    @Test
    void testMisc() {
        assertTrue(ClipboardWorker.isDisplayableContent(MimeType.PDF), "PDF displayable");
        assertTrue(ClipboardWorker.isDisplayableContent(MimeType.PNG), "PNG displayable");
        assertFalse(ClipboardWorker.isDisplayableContent(MimeType.BINARY), "binary displayable");
        assertFalse(ClipboardWorker.isDisplayableContent(MimeType.PPT), "ppt displayable");

        assertDoesNotThrow(BasicCtx::new);
        final BasicCtx ctx = new BasicCtx();
        assertNotNull(ctx.toString());
    }
}
