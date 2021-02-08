module io.opencmw.client {
    requires disruptor;
    requires io.opencmw;
    requires io.opencmw.serialiser;
    requires io.opencmw.server;
    requires it.unimi.dsi.fastutil;
    requires java.management;
    requires jeromq;
    requires kotlin.stdlib;
    requires okhttp3;
    requires okhttp3.sse;
    requires org.apache.commons.lang3;
    requires org.jetbrains.annotations;
    requires org.slf4j;

    exports io.opencmw.client;
    exports io.opencmw.client.cmwlight;
    exports io.opencmw.client.rest;

    opens io.opencmw.client to io.opencmw.serialiser;
}