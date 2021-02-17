package io.opencmw.server;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;

import io.opencmw.MimeType;
import io.opencmw.OpenCmwProtocol;
import io.opencmw.domain.BinaryData;
import io.opencmw.rbac.RbacRole;
import io.opencmw.server.domain.BasicCtx;
import io.opencmw.utils.Cache;

public class ClipboardWorker extends MajordomoWorker<BasicCtx, BinaryData, BinaryData> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClipboardWorker.class);
    private static final BasicCtx EMPTY_CONTEXT = new BasicCtx();
    private static final String PATH_UPLOAD = "upload";
    /* template engine tags */
    private static final String TAG_SERVICE_NAME = "serviceName";
    private static final String TAG_SERVICE_PATH = "servicePath";
    private static final String TAG_SERVING_SITE = "serviceType";
    private static final String TAG_RESOURCE_NAME = "resourceName";
    private static final String TAG_CATEGORY = "category";
    private static final String TAG_CATEGORIES = "categories";
    private static final String TAG_REPOSITORY_DATA = "clipboardData";
    private static final String TAG_REPOSITORY_DATA_BINARY = "clipboardDataBinary";
    private static final String TAG_POLLING_PERIOD = "updatePeriod";

    /**
     * additional query tag that provides the source/redirect address for the original source of the binary data
     * example: '/tree-sub-branch-a/tree-sub-branch-b/../mydata.png?redirect="http://ip:port/path/mydata.png'
     */
    public static final String REDIRECT_TAG = "redirect";
    private final Cache<String, DataContainer> repository;

    public ClipboardWorker(final @NotNull String propertyName, final @NotNull ZContext ctx, final Cache<String, DataContainer> repository, final @NotNull RbacRole<?>... rbacRoles) {
        super(ctx, propertyName, BasicCtx.class, BinaryData.class, BinaryData.class, rbacRoles);
        this.repository = Objects.requireNonNullElse(repository, Cache.<String, DataContainer>builder().build());
        setHtmlHandler(new DefaultHtmlHandler<>(this.getClass(), "/velocity/property/ClipboardLayout.vm", map -> map.put("propertyName", propertyName)));

        this.setHandler((rawCtx, reqCtx, in, repCtx, out) -> {
            switch (rawCtx.req.command) {
            case GET_REQUEST:
                handleGet(rawCtx, reqCtx, repCtx, out);
                break;
            case SET_REQUEST:
                handleSet(rawCtx, in);
                break;
            case UNKNOWN:
            default:
                throw new UnsupportedOperationException("command not implemented by service: '" + propertyName + "' request message: " + rawCtx.req);
            }
        });
        this.setDaemon(true);
        LOGGER.atDebug().addArgument(ClipboardWorker.class.getName()).addArgument(propertyName).log("initialised {} at '{}'");
    }

    private void handleGet(final OpenCmwProtocol.Context rawCtx, final BasicCtx reqCtx, final BasicCtx repCtx, final BinaryData out) { // NOPMD
        final String resource = StringUtils.stripStart(StringUtils.stripStart(URLDecoder.decode(rawCtx.req.topic.getPath(), UTF_8), "/"), getServiceName());

        final int dotPosition = resource.lastIndexOf('.');
        final boolean isFileResource = dotPosition >= 0 && dotPosition < resource.length() - 1;
        if (isFileResource && reqCtx.sse == null && (reqCtx.longPolling < 0)) {
            Objects.requireNonNull(repository.get(resource), "could not retrieve resource '" + resource + "'").binaryData.moveTo(out);
            repCtx.contentType = out.contentType;
            return;
        }
        final String resourceName = StringUtils.substringAfterLast(resource, "/");
        final String categoryName = StringUtils.strip(isFileResource ? StringUtils.substringBeforeLast(resource, "/") : resource, "/");
        // ensure that html meta data map is initialised
        rawCtx.htmlData = Objects.requireNonNullElse(rawCtx.htmlData, new HashMap<>()); // NOPMD - PMD.UseConcurrentHashMap
        rawCtx.htmlData.put(TAG_SERVICE_NAME, getServiceName());
        rawCtx.htmlData.put(TAG_RESOURCE_NAME, resourceName);
        rawCtx.htmlData.put(TAG_SERVICE_PATH, resource);

        if (resource.endsWith(PATH_UPLOAD)) {
            // serve upload page
            rawCtx.htmlData.put(TAG_CATEGORY, StringUtils.removeEnd(categoryName, '/' + PATH_UPLOAD));
            rawCtx.htmlData.put(TAG_SERVING_SITE, PATH_UPLOAD);
            return;
        }
        rawCtx.htmlData.put(TAG_CATEGORY, categoryName);
        if (reqCtx.sse != null) {
            rawCtx.htmlData.put(TAG_RESOURCE_NAME, resource);
            rawCtx.htmlData.put(TAG_SERVING_SITE, "oneSSE");
            return;
        }

        if (reqCtx.longPolling >= 0) {
            rawCtx.htmlData.put(TAG_RESOURCE_NAME, resource);
            rawCtx.htmlData.put(TAG_SERVING_SITE, "onePolling");
            rawCtx.htmlData.put(TAG_POLLING_PERIOD, Math.min(Math.max(reqCtx.longPolling, 20), 10_000));
            return;
        }

        final Set<String> subCategories = //
                repository.keySet().stream() //
                        .filter(s -> s.startsWith('/' + categoryName))
                        .map(s -> StringUtils.removeStart(s, '/' + categoryName))
                        .map(s -> StringUtils.substringBefore(StringUtils.strip(StringUtils.substringBeforeLast(s, "/"), "/"), "/"))
                        .collect(Collectors.toSet());

        final List<DataContainer> dataInSubCategory = //
                repository.entrySet().stream() //
                        .filter(e -> StringUtils.substringBeforeLast(e.getKey(), "/").equals('/' + categoryName))
                        .sorted(Comparator.comparing(s -> s.getValue().getFileName()))
                        .map(Map.Entry::getValue)
                        .collect(Collectors.toList());

        rawCtx.htmlData.put(TAG_CATEGORIES, subCategories);
        rawCtx.htmlData.put(TAG_SERVING_SITE, "other");
        rawCtx.htmlData.put(TAG_REPOSITORY_DATA, dataInSubCategory.stream().filter(d -> isDisplayableContent(d.binaryData.contentType)).collect(Collectors.toList()));
        rawCtx.htmlData.put(TAG_REPOSITORY_DATA_BINARY, dataInSubCategory.stream().filter(d -> !isDisplayableContent(d.binaryData.contentType)).collect(Collectors.toList()));
    }

    protected static boolean isDisplayableContent(final @NotNull MimeType contentType) {
        return contentType == MimeType.PDF || !contentType.getMediaType().startsWith("application");
    }

    private void handleSet(final OpenCmwProtocol.Context rawCtx, final BinaryData in) { // NOPMD
        // change redirect path
        final String redirectPath = StringUtils.prependIfMissing(StringUtils.removeEnd(rawCtx.req.topic.getPath(), '/' + PATH_UPLOAD), "/");
        rawCtx.rep.topic = URI.create(redirectPath);

        final String resource = StringUtils.stripStart(StringUtils.removeEnd(StringUtils.stripStart(rawCtx.req.topic.getPath(), "/"), PATH_UPLOAD), getServiceName());

        uploadAndNotifyData(in, resource);
    }

    public void uploadAndNotifyData(final BinaryData in, final String resource) {
        synchronized (repository) {
            final String cleanedResourceName = StringUtils.removeEnd(StringUtils.prependIfMissing(resource, "/"), "/") + StringUtils.prependIfMissing(in.resourceName, "/");

            // add binary data to repository
            in.resourceName = cleanedResourceName;
            repository.put(cleanedResourceName, new DataContainer(in));

            // notify others that might be listening always as <serviceName>/resourceName ...
            // N.B. we do not cross-notify for other properties
            notify(cleanedResourceName, EMPTY_CONTEXT, in);
        }
    }

    public static class DataContainer {
        private final BinaryData binaryData;
        private final long updateTime;

        public DataContainer(final @NotNull BinaryData binaryData) {
            this.updateTime = System.currentTimeMillis();
            this.binaryData = binaryData;
        }

        public String getResourceName() {
            return binaryData.resourceName;
        }

        public String getEncodedResourceName() {
            return URLEncoder.encode(binaryData.resourceName, UTF_8);
        }

        public String getFileName() {
            return URLDecoder.decode(StringUtils.substringAfterLast(binaryData.resourceName, "/"), UTF_8);
        }

        /**
         * @return HTML resource tag 'data:<MIME type>;base64,<base65 encoded data>'
         */
        public String getMimeData() {
            return "data:" + binaryData.contentType.getMediaType() + ";base64," + Base64.getEncoder().encodeToString(binaryData.data);
        }

        public String getTimeStamp() {
            final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.UK);
            sdf.setTimeZone(TimeZone.getDefault());
            return sdf.format(updateTime);
        }
    }
}
