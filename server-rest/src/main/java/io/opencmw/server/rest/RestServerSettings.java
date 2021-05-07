package io.opencmw.server.rest;

import io.opencmw.serialiser.annotations.MetaInfo;
import io.opencmw.utils.Settings;

import com.google.auto.service.AutoService;

@AutoService(Settings.class)
public class RestServerSettings implements Settings {
    @MetaInfo(description = "restKeyStorePassword")
    public static final String REST_KEY_STORE_PASSWORD = "";
    @MetaInfo(description = "restKeyStore")
    public static final String REST_KEY_STORE = "";
    @MetaInfo(description = "rest server hostname")
    public static final String DEFAULT_HOST_NAME = "0";
    @MetaInfo(description = "rest server plain http port")
    public static final Integer DEFAULT_PORT = 8080;
    @MetaInfo(description = "rest server ssl port")
    public static final Integer DEFAULT_PORT2 = 8443;
    @MetaInfo(description = "password store file location, defaults to (insecure) internal store")
    public static final String USER_PASSWORD_STORE = "";
    @MetaInfo(description = "scheduled thread count")
    public static final Integer SCHEDULED_THREAD_COUNT = 32;
    @MetaInfo(description = "thread count")
    public static final Integer THREAD_COUNT = 64;
}
