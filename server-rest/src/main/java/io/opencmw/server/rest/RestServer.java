package io.opencmw.server.rest;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.servlet.ServletOutputStream;

import org.eclipse.jetty.alpn.server.ALPNServerConnectionFactory;
import org.eclipse.jetty.http2.HTTP2Cipher;
import org.eclipse.jetty.http2.server.HTTP2ServerConnectionFactory;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.session.SessionHandler;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.javalin.Javalin;
import io.javalin.apibuilder.ApiBuilder;
import io.javalin.core.compression.Gzip;
import io.javalin.core.event.HandlerMetaInfo;
import io.javalin.core.security.Role;
import io.javalin.core.util.Header;
import io.javalin.core.util.RouteOverviewPlugin;
import io.javalin.http.Context;
import io.javalin.http.sse.SseClient;
import io.javalin.http.util.RateLimit;
import io.javalin.plugin.json.JavalinJson;
import io.javalin.plugin.metrics.MicrometerPlugin;
import io.javalin.plugin.openapi.OpenApiOptions;
import io.javalin.plugin.openapi.OpenApiPlugin;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.ui.ReDocOptions;
import io.javalin.plugin.openapi.ui.SwaggerOptions;
import io.opencmw.MimeType;
import io.opencmw.rbac.BasicRbacRole;
import io.opencmw.rbac.RbacRole;
import io.opencmw.server.rest.admin.RestServerAdmin;
import io.opencmw.server.rest.login.LoginController;
import io.opencmw.server.rest.user.RestUserHandler;
import io.opencmw.server.rest.user.RestUserHandlerImpl;
import io.opencmw.server.rest.util.MessageBundle;
import io.swagger.v3.oas.models.info.Info;

import com.jsoniter.JsonIterator;
import com.jsoniter.output.JsonStream;

/**
 * Small RESTful server helper class.
 *
 * <p>
 * The Javalin framework is being used internally: <a href="https://javalin.io/" target="_blank">https://javalin.io/</a>
 *
 * The primary purposes of this utility class is to provide
 * <ul>
 * <li>some convenience methods, default configuration (in particular relating to SSL and HTTP/2), and
 * <li>to wrap the primary REST server implementation in view of back-end server upgrades or changing API.
 * <li>to provide every GET route also with an long-polling and SSE listener/data-retrieval management.
 * </ul>
 *
 * <p>
 * Server parameter can be controlled via the following system properties:
 * <ul>
 * <li><em>restServerHostName</em>: host name or IP address the server should bind to
 * <li><em>restServerPort</em>: the HTTP port
 * <li><em>restServerPort2</em>: the HTTP/2 port (encrypted)
 * <li><em>restKeyStore</em>: the path to the file containing the key store for the encryption
 * <li><em>restKeyStorePassword</em>: the path to the file containing the key store for the encryption
 * <li><em>restUserPasswordStore</em>: the path to the file containing the user passwords and roles encryption
 * </ul>
 * some design choices: minimise exposing Javalin API outside this class, no usage of UI specific classes (ie. JavaFX)
 *
 * @author rstein
 */
public final class RestServer { // NOPMD -- nomen est omen
    public static final String TAG_REST_SERVER_HOST_NAME = "restServerHostName";
    public static final String TAG_REST_SERVER_PORT = "restServerPort";
    public static final String TAG_REST_SERVER_PORT2 = "restServerPort2";
    public static final String REST_KEY_STORE = "restKeyStore";
    public static final String REST_KEY_STORE_PASSWORD = "restKeyStorePassword";
    // some HTML constants
    public static final String HTML_ACCEPT = "accept";
    private static final Logger LOGGER = LoggerFactory.getLogger(RestServer.class);
    private static final String DEFAULT_HOST_NAME = "0";
    private static final int DEFAULT_PORT = 8080;
    private static final int DEFAULT_PORT2 = 8443;
    private static final String REST_PROTOCOL = "protocol";

    private static final String TEMPLATE_UNAUTHORISED = "/velocity/errors/unauthorised.vm";
    private static final String TEMPLATE_ACCESS_DENIED = "/velocity/errors/accessDenied.vm";
    private static final String TEMPLATE_NOT_FOUND = "/velocity/errors/notFound.vm";
    private static final String TEMPLATE_BAD_REQUEST = "/velocity/errors/badRequest.vm";
    private static final ConcurrentMap<String, Queue<SseClient>> EVENT_LISTENER_SSE = new ConcurrentHashMap<>();
    private static final List<HandlerMetaInfo> ENDPOINTS = new ArrayList<>();
    private static final Consumer<HandlerMetaInfo> ENDPOINT_ADDED_HANDLER = ENDPOINTS::add;
    private static Javalin instance;
    private static MimeType defaultProtocol = MimeType.HTML;
    private static RestUserHandler userHandler = new RestUserHandlerImpl(BasicRbacRole.NULL); // include basic Rbac role definition
    private static String serverName = "Undefined REST Server";

    private RestServer() {
        // this is a utility class
    }

    public static void addLongPollingCookie(final Context ctx, final String key, final long lastUpdateMillies) {
        // N.B. this is a workaround since javax.servlet.http.Cookie does not support the SameSite cookie field.
        // workaround inspired by: https://github.com/tipsy/javalin/issues/780
        final String cookieComment = "stores the servcer-side time stamp of the last valid update (required for long-polling)";
        final String cookie = key + "=" + lastUpdateMillies + "; Comment=\"" + cookieComment + "\"; Expires=-1; SameSite=Strict;";
        ctx.res.addHeader("Set-Cookie", cookie);
    }

    public static URI appendUri(URI oldUri, String appendQuery) throws URISyntaxException {
        return new URI(oldUri.getScheme(), oldUri.getAuthority(), oldUri.getPath(),
                oldUri.getQuery() == null ? appendQuery : oldUri.getQuery() + "&" + appendQuery, oldUri.getFragment());
    }

    /**
     * guards this end point and returns HTTP error response if predefined rate limit is exceeded
     *
     * @param ctx         end point context handler
     * @param numRequests number of calls
     * @param timeUnit    time base reference
     */
    public static void applyRateLimit(final Context ctx, final int numRequests, final TimeUnit timeUnit) {
        new RateLimit(ctx).requestPerTimeUnit(numRequests, timeUnit); //
    }

    public static MimeType getDefaultProtocol() {
        return defaultProtocol;
    }

    public static Set<Role> getDefaultRole() {
        return Collections.singleton(new RestRole(BasicRbacRole.ANYONE));
    }

    public static List<HandlerMetaInfo> getEndpoints() {
        return ENDPOINTS;
    }

    public static Queue<SseClient> getEventClients(@NotNull final String endpointName) {
        if (endpointName.isEmpty()) {
            throw new IllegalArgumentException("endpointNmae must not be empty");
        }

        final String fullEndPointName = prefixPath(endpointName);
        return EVENT_LISTENER_SSE.computeIfAbsent(fullEndPointName, key -> new ConcurrentLinkedQueue<>());
    }

    public static ConcurrentMap<String, Queue<SseClient>> getEventClientMap() {
        return EVENT_LISTENER_SSE;
    }

    public static String getHostName() {
        return System.getProperty(TAG_REST_SERVER_HOST_NAME, DEFAULT_HOST_NAME);
    }

    public static int getHostPort() {
        final String property = System.getProperty(TAG_REST_SERVER_PORT, Integer.toString(DEFAULT_PORT));
        try {
            return Integer.parseInt(property);
        } catch (final NumberFormatException e) {
            LOGGER.atError().addArgument(TAG_REST_SERVER_PORT).addArgument(property).addArgument(DEFAULT_PORT).log("could not parse {}='{}' return default port {}");
            return DEFAULT_PORT;
        }
    }

    public static int getHostPort2() {
        final String property = System.getProperty(TAG_REST_SERVER_PORT2, Integer.toString(DEFAULT_PORT2));
        try {
            return Integer.parseInt(property);
        } catch (final NumberFormatException e) {
            LOGGER.atError().addArgument(TAG_REST_SERVER_PORT2).addArgument(property).addArgument(DEFAULT_PORT2).log("could not parse {}='{}' return default port {}");
            return DEFAULT_PORT2;
        }
    }

    public static Javalin getInstance() {
        if (instance == null) {
            startRestServer();
        }
        return instance;
    }

    public static URI getLocalURI() {
        try {
            return new URI("http://localhost:" + getHostPort());
        } catch (final URISyntaxException e) {
            LOGGER.atError().setCause(e).log("getLocalURL()");
        }
        return null;
    }

    public static String getName() {
        return serverName;
    }

    public static URI getPublicURI() {
        final String ip = getLocalHostName();
        try (DatagramSocket socket = new DatagramSocket()) {
            return new URI("https://" + ip + ":" + getHostPort2());
        } catch (final URISyntaxException | SocketException e) {
            LOGGER.atError().setCause(e).log("getPublicURL()");
        }
        return null;
    }

    public static MimeType getRequestedMimeProtocol(final Context ctx, final MimeType... defaultProtocol) {
        return MimeType.getEnum(getRequestedProtocol(ctx, defaultProtocol.length == 0 ? getDefaultProtocol().toString() : defaultProtocol[0].toString()));
    }

    public static String getRequestedProtocol(final Context ctx, final String... defaultProtocol) {
        String protocol = defaultProtocol.length == 0 ? getDefaultProtocol().toString() : defaultProtocol[0];
        String protocolHeader = ctx.header(Header.ACCEPT);
        String protocolQuery = ctx.queryParam(REST_PROTOCOL);

        if (protocolHeader != null && !protocolHeader.isBlank()) {
            protocol = protocolHeader;
        }
        if (protocolQuery != null && !protocolQuery.isBlank()) {
            protocol = protocolQuery;
        }

        return protocol;
    }

    public static Set<RbacRole> getSessionCurrentRoles(final Context ctx) {
        return LoginController.getSessionCurrentRoles(ctx);
    }

    public static String getSessionCurrentUser(final Context ctx) {
        return LoginController.getSessionCurrentUser(ctx);
    }

    public static String getSessionLocale(final Context ctx) {
        return LoginController.getSessionLocale(ctx);
    }

    public static RestUserHandler getUserHandler() {
        return userHandler;
    }

    public static String prefixPath(@NotNull final String path) {
        return ApiBuilder.prefixPath(path);
    }

    public static void setDefaultProtocol(MimeType defaultProtocol) {
        RestServer.defaultProtocol = defaultProtocol;
    }

    public static void setName(final String serverName) {
        RestServer.serverName = serverName;
    }

    /**
     * Sets a new user handler.
     *
     * N.B: This will issue a warning to remind system admins or security-minded people
     * that the default implementation may have been replaced with a better/worse/different implementation (e.g. based on
     * LDAP or another data base)
     *
     * @param newUserHandler the new implementation
     */
    public static void setUserHandler(final RestUserHandler newUserHandler) {
        LOGGER.atWarn().addArgument(newUserHandler.getClass().getCanonicalName()).log("replacing default user handler with '{}'");
        userHandler = newUserHandler;
    }

    public static void startRestServer() {
        JavalinJson.setFromJsonMapper(JsonIterator::deserialize);
        JavalinJson.setToJsonMapper(JsonStream::serialize);
        instance = Javalin.create(config -> {
                              config.enableCorsForAllOrigins();
                              config.addStaticFiles("/public");
                              config.showJavalinBanner = false;
                              config.defaultContentType = getDefaultProtocol().toString();
                              config.compressionStrategy(null, new Gzip(6));
                              config.server(RestServer::createHttp2Server);
                              // show all routes on specified path
                              config.registerPlugin(new RouteOverviewPlugin("/admin/endpoints", Collections.singleton(new RestRole(BasicRbacRole.ADMIN))));
                              config.registerPlugin(new MicrometerPlugin());
                              config.sessionHandler(getCustomSessionHandlerSupplier());
                              // add OpenAPI
                              config.registerPlugin(new OpenApiPlugin(getOpenApiOptions()));
                          })
                           .events(event -> event.handlerAdded(ENDPOINT_ADDED_HANDLER));
        instance.start();

        // add login management
        LoginController.register();

        // add basic RestServer admin interface
        RestServerAdmin.register();

        // some default error mappings
        instance.error(400, ctx -> ctx.render(TEMPLATE_BAD_REQUEST, MessageBundle.baseModel(ctx)));
        instance.error(401, ctx -> ctx.render(TEMPLATE_UNAUTHORISED, MessageBundle.baseModel(ctx)));
        instance.error(403, ctx -> ctx.render(TEMPLATE_ACCESS_DENIED, MessageBundle.baseModel(ctx)));
        instance.error(404, ctx -> ctx.render(TEMPLATE_NOT_FOUND, MessageBundle.baseModel(ctx)));
    }

    public static void startRestServer(final int hostPort, final int hostPort2) {
        System.setProperty(TAG_REST_SERVER_PORT, Integer.toString(hostPort));
        System.setProperty(TAG_REST_SERVER_PORT2, Integer.toString(hostPort2));
        startRestServer();
    }

    public static void startRestServer(final String hostName, final int hostPort, final int hostPort2) {
        System.setProperty(TAG_REST_SERVER_HOST_NAME, hostName);
        System.setProperty(TAG_REST_SERVER_PORT, Integer.toString(hostPort));
        System.setProperty(TAG_REST_SERVER_PORT2, Integer.toString(hostPort2));
        startRestServer();
    }

    public static void stopRestServer() {
        if (Objects.requireNonNull(RestServer.getInstance().server()).server().isRunning()) {
            RestServer.getInstance().stop();
        }
    }

    /**
     * Suppresses caching for this end point
     *
     * @param ctx end point context handler
     */
    public static void suppressCaching(final Context ctx) {
        // for for HTTP 1.1
        // https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Cache-Control
        ctx.res.addHeader("Cache-Control", "no-store");

        // for HTTP 1.0
        ctx.res.addHeader("Pragma", "no-cache");

        // for proxies: may need to check an appropriate value
        ctx.res.addHeader("Expires", "0");
    }

    public static void writeBytesToContext(@NotNull final Context ctx, final byte[] bytes, final int nSize) {
        // based on the suggestions at https://github.com/tipsy/javalin/issues/910
        try (ServletOutputStream outputStream = ctx.res.getOutputStream()) {
            outputStream.write(bytes, 0, nSize);
            outputStream.flush();
        } catch (final IOException e) {
            LOGGER.atError().setCause(e);
        }
    }

    private static Server createHttp2Server() {
        final Server server = new Server();

        // unencrypted HTTP 1 anchor
        try (ServerConnector connector = new ServerConnector(server)) {
            final String hostName = getHostName();
            final int hostPort = getHostPort();
            LOGGER.atInfo().addArgument(getLocalHostName()).log("local hostname = '{}'");
            LOGGER.atInfo().addArgument(hostName).addArgument(hostPort).log("create HTTP 1.x connector at 'http://{}:{}'");
            connector.setHost(hostName);
            connector.setPort(hostPort);
            server.addConnector(connector);
        }

        // HTTP Configuration
        final HttpConfiguration httpConfig = new HttpConfiguration();
        httpConfig.setSendServerVersion(false);
        httpConfig.setSecureScheme("https");
        httpConfig.setSecurePort(getHostPort2());

        // HTTPS Configuration
        final HttpConfiguration httpsConfig = new HttpConfiguration(httpConfig);
        httpsConfig.addCustomizer(new SecureRequestCustomizer());

        // HTTP/2 Connection Factory
        final HTTP2ServerConnectionFactory h2 = new HTTP2ServerConnectionFactory(httpsConfig);
        final ALPNServerConnectionFactory alpn = new ALPNServerConnectionFactory();
        alpn.setDefaultProtocol("h2");

        // SSL Connection Factory
        final SslContextFactory sslContextFactory = createSslContextFactory();
        final SslConnectionFactory ssl = new SslConnectionFactory(sslContextFactory, alpn.getProtocol());

        // HTTP/2 Connector
        try (ServerConnector http2Connector = new ServerConnector(server, ssl, alpn, h2, new HttpConnectionFactory(httpsConfig))) {
            final String hostName = getHostName();
            final int hostPort = getHostPort2();
            LOGGER.atInfo().addArgument(hostName).addArgument(hostPort).log("create HTTP/2 connector at 'http://{}:{}'");
            http2Connector.setHost(hostName);
            http2Connector.setPort(hostPort);
            server.addConnector(http2Connector);
        }

        return server;
    }

    private static SslContextFactory createSslContextFactory() {
        final String keyStoreFile = System.getProperty(REST_KEY_STORE, null); // replace default with your real keystore
        final String keyStorePwdFile = System.getProperty(REST_KEY_STORE_PASSWORD, null); // replace default with your real password
        if (keyStoreFile == null || keyStorePwdFile == null) {
            LOGGER.atInfo().addArgument(keyStoreFile).addArgument(keyStorePwdFile).log("using internal keyStore {} and/or keyStorePasswordFile {} -- PLEASE CHANGE FOR PRODUCTION -- THIS IS UNSAFE PRACTICE");
        }
        LOGGER.atInfo().addArgument(keyStoreFile).log("using keyStore at '{}'");
        LOGGER.atInfo().addArgument(keyStorePwdFile).log("using keyStorePasswordFile at '{}'");

        boolean readComplete = true;
        String keyStorePwd = null;
        KeyStore keyStore = null;

        // read keyStore password
        try (BufferedReader br = keyStorePwdFile == null ? new BufferedReader(new InputStreamReader(RestServer.class.getResourceAsStream("/keystore.pwd"), UTF_8)) //
                                                         : Files.newBufferedReader(Paths.get(keyStorePwdFile), UTF_8)) {
            keyStorePwd = br.readLine();
        } catch (final IOException e) {
            readComplete = false;
            LOGGER.atError().setCause(e).addArgument(keyStorePwdFile).log("error while reading key store password from '{}'");
        }

        if (readComplete && keyStorePwd != null) {
            // read the actual keyStore
            try (InputStream is = keyStoreFile == null ? RestServer.class.getResourceAsStream("/keystore.jks") //
                                                       : Files.newInputStream(Paths.get(keyStoreFile))) {
                keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
                keyStore.load(is, keyStorePwd.toCharArray());
            } catch (final NoSuchAlgorithmException | CertificateException | KeyStoreException | IOException e) {
                readComplete = false;
                LOGGER.atError().setCause(e).addArgument(keyStoreFile == null ? "internal" : keyStoreFile).log("error while reading key store from '{}'");
            }
        }

        // SSL Context Factory for HTTPS and HTTP/2
        final SslContextFactory sslContextFactory = new SslContextFactory(true) {}; // trust all certificates
        if (readComplete) {
            sslContextFactory.setKeyStore(keyStore);
            sslContextFactory.setKeyStorePassword(keyStorePwd);
        }
        sslContextFactory.setCipherComparator(HTTP2Cipher.COMPARATOR);
        sslContextFactory.setProvider("Conscrypt");

        return sslContextFactory;
    }

    /**
     * 
     * @return custom session handler that sets Jetty's JSESSIONID cookie to SameSite=strict
     * 
     *         N.B. to be used within Javalin's 'config.sessionHandler(getCustomSessionHandlerSupplier());'
     */
    private static Supplier<SessionHandler> getCustomSessionHandlerSupplier() {
        final SessionHandler sessionHandler = new SessionHandler();
        sessionHandler.getSessionCookieConfig().setHttpOnly(true);
        sessionHandler.getSessionCookieConfig().setSecure(true);
        sessionHandler.getSessionCookieConfig().setComment("__SAME_SITE_STRICT__");
        return () -> sessionHandler;
    }

    private static String getLocalHostName() {
        String ip;
        try (DatagramSocket socket = new DatagramSocket()) {
            socket.connect(InetAddress.getByName("8.8.8.8"), 10_002); // NOPMD - bogus hardcoded IP acceptable in this context
            if (socket.getLocalAddress() == null) {
                throw new UnknownHostException("bogus exception can be ignored");
            }
            ip = socket.getLocalAddress().getHostAddress();

            if (ip != null) {
                return ip;
            }
        } catch (final SocketException | UnknownHostException e) {
            LOGGER.atError().setCause(e).log("getLocalHostName()");
        }
        return "localhost";
    }

    private static OpenApiOptions getOpenApiOptions() {
        Info applicationInfo = new Info().version("1.0").description(serverName);
        return new OpenApiOptions(applicationInfo).path("/swagger-docs").ignorePath("/admin/endpoints", HttpMethod.GET) // Disable documentation
                .swagger(new SwaggerOptions("/swagger").title("My Swagger Documentation"))
                .reDoc(new ReDocOptions("/redoc").title("My ReDoc Documentation"));
    }
}
