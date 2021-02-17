package io.opencmw.server.rest;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.*;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opencmw.MimeType;
import io.opencmw.rbac.BasicRbacRole;
import io.opencmw.server.MajordomoBroker;
import io.opencmw.server.rest.test.HelloWorldService;
import io.opencmw.server.rest.test.ImageService;

import okhttp3.Headers;
import okhttp3.MultipartBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.sse.EventSource;
import okhttp3.sse.EventSourceListener;
import okhttp3.sse.EventSources;
import zmq.util.Utils;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MajordomoRestPluginTests {
    private static final Logger LOGGER = LoggerFactory.getLogger(MajordomoRestPluginTests.class);
    private MajordomoBroker primaryBroker;
    private String brokerRouterAddress;
    private MajordomoBroker secondaryBroker;
    private String secondaryBrokerRouterAddress;
    private OkHttpClient okHttp;

    @BeforeAll
    void init() throws IOException {
        okHttp = getUnsafeOkHttpClient(); // N.B. ignore SSL certificates
        primaryBroker = new MajordomoBroker("PrimaryBroker", "", BasicRbacRole.values());
        brokerRouterAddress = primaryBroker.bind("mdp://localhost:" + Utils.findOpenPort());
        primaryBroker.bind("mds://localhost:" + Utils.findOpenPort());
        MajordomoRestPlugin restPlugin = new MajordomoRestPlugin(primaryBroker.getContext(), "My test REST server", "*:8080", BasicRbacRole.ADMIN);
        primaryBroker.start();
        restPlugin.start();
        LOGGER.atInfo().log("Broker and REST plugin started");

        // start simple test services/properties
        final HelloWorldService helloWorldService = new HelloWorldService(primaryBroker.getContext());
        helloWorldService.start();
        final ImageService imageService = new ImageService(primaryBroker.getContext(), 100);
        imageService.start();

        // TODO: add OpenCMW client requesting binary and json models

        // second broker to test DNS functionalities
        secondaryBroker = new MajordomoBroker("SecondaryTestBroker", brokerRouterAddress, BasicRbacRole.values());
        secondaryBrokerRouterAddress = secondaryBroker.bind("tcp://*:" + Utils.findOpenPort());
        secondaryBroker.start();
    }

    @AfterAll
    void finish() {
        secondaryBroker.stopBroker();
        primaryBroker.stopBroker();
    }

    @ParameterizedTest
    @ValueSource(strings = { "http://localhost:8080", "https://localhost:8443" })
    void testDns(final String address) throws IOException {
        final Request request = new Request.Builder().url(address + "/mmi.dns?noMenu").addHeader("accept", MimeType.HTML.getMediaType()).get().build();
        final Response response = okHttp.newCall(request).execute();
        final String body = Objects.requireNonNull(response.body()).string();

        assertThat(body, containsString(brokerRouterAddress));
        assertThat(body, containsString(secondaryBrokerRouterAddress));
        assertThat(body, containsString("http://localhost:8080"));
    }

    @ParameterizedTest
    @EnumSource(value = MimeType.class, names = { "HTML", "BINARY", "JSON", "CMWLIGHT", "TEXT", "UNKNOWN" })
    void testGet(final MimeType contentType) throws IOException {
        final Request request = new Request.Builder().url("http://localhost:8080/helloWorld?noMenu").addHeader("accept", contentType.getMediaType()).get().build();
        final Response response = okHttp.newCall(request).execute();
        final Headers header = response.headers();
        final String body = Objects.requireNonNull(response.body()).string();

        assertEquals(contentType.getMediaType(), header.get("Content-Type"), "you get the content type you asked for");
        assertThat(body, containsString("byteReturnType"));
        assertThat(body, containsString("Hello World! The local time is:"));

        switch (contentType) {
        case JSON:
            assertThat(body, containsString("\"byteReturnType\": 42,"));
            break;
        case TEXT:
        default:
            break;
        }
    }

    @ParameterizedTest
    @EnumSource(value = MimeType.class, names = { "HTML", "BINARY", "JSON", "CMWLIGHT", "TEXT", "UNKNOWN" })
    void testGetException(final MimeType contentType) throws IOException {
        final Request request = new Request.Builder().url("http://localhost:8080/mmi.openapi?noMenu").addHeader("accept", contentType.getMediaType()).get().build();
        final Response response = okHttp.newCall(request).execute();
        final Headers header = response.headers();
        final String body = Objects.requireNonNull(response.body()).string();
        switch (contentType) {
        case HTML:
        case TEXT:
            assertEquals(200, response.code());
            assertThat(body, containsString("java.util.concurrent.ExecutionException: java.net.ProtocolException"));
            break;
        case BINARY:
        case JSON:
        case CMWLIGHT:
        case UNKNOWN:
            assertEquals(400, response.code());
            break;
        default:
            throw new IllegalStateException("test case not covered");
        }
    }

    @ParameterizedTest
    @EnumSource(value = MimeType.class, names = { "HTML", "JSON" })
    void testSet(final MimeType contentType) throws IOException {
        final Request setRequest = new Request.Builder() //
                                           .url("http://localhost:8080/helloWorld?noMenu")
                                           .addHeader("accept", contentType.getMediaType())
                                           .post(new MultipartBody.Builder().setType(MultipartBody.FORM) //
                                                           .addFormDataPart("name", "needsName")
                                                           .addFormDataPart("customFilter", "myCustomName")
                                                           .addFormDataPart("byteReturnType", "1984")
                                                           .build())
                                           .build();
        final Response setResponse = okHttp.newCall(setRequest).execute();
        assertEquals(200, setResponse.code());

        final Request getRequest = new Request.Builder().url("http://localhost:8080/helloWorld?noMenu").addHeader("accept", contentType.getMediaType()).get().build();
        final Response response = okHttp.newCall(getRequest).execute();
        final Headers header = response.headers();
        final String body = Objects.requireNonNull(response.body()).string();
        switch (contentType) {
        case HTML:
            assertThat(body, containsString("name=\"lsaContext\" value='myCustomName'"));
            break;
        case JSON:
            assertThat(body, containsString("\"lsaContext\": \"myCustomName\","));
            break;
        default:
            throw new IllegalStateException("test case not covered");
        }
    }

    @Test
    void testSSE() {
        AtomicInteger eventCounter = new AtomicInteger();
        Request request = new Request.Builder().url("http://localhost:8080/" + ImageService.PROPERTY_NAME).build();
        EventSourceListener eventSourceListener = new EventSourceListener() {
            private final BlockingQueue<Object> events = new LinkedBlockingDeque<>();

            @Override
            public void onEvent(final @NotNull EventSource eventSource, final String id, final String type, @NotNull String data) {
                eventCounter.getAndIncrement();
            }
        };
        final EventSource source = EventSources.createFactory(okHttp).newEventSource(request, eventSourceListener);
        await().alias("wait for thread to start worker").atMost(1, TimeUnit.SECONDS).until(eventCounter::get, greaterThanOrEqualTo(3));
        assertThat(eventCounter.get(), greaterThanOrEqualTo(3));
        source.cancel();
    }

    private static OkHttpClient getUnsafeOkHttpClient() {
        try {
            // Create a trust manager that does not validate certificate chains
            final TrustManager[] trustAllCerts = new TrustManager[] {
                new X509TrustManager(){
                        @Override
                        public void checkClientTrusted(java.security.cert.X509Certificate[] chain, String authType){}

                        @Override
                        public void checkServerTrusted(java.security.cert.X509Certificate[] chain, String authType){}

                        @Override
                        public java.security.cert.X509Certificate[] getAcceptedIssuers(){
                                return new java.security.cert.X509Certificate[] {};
        }
    }
};

// Install the all-trusting trust manager
final SSLContext sslContext = SSLContext.getInstance("SSL");
sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
// Create an ssl socket factory with our all-trusting manager
final SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();

OkHttpClient.Builder builder = new OkHttpClient.Builder();
builder.sslSocketFactory(sslSocketFactory, (X509TrustManager) trustAllCerts[0]);
builder.hostnameVerifier((hostname, session) -> true);
return builder.build();
}
catch (Exception e) {
    throw new RuntimeException(e);
}
}
}
