module io.opencmw.client {
    requires java.management;
    requires io.opencmw;
    requires io.opencmw.serialiser;
    requires org.slf4j;
    requires jeromq;
    requires disruptor;
    requires org.jetbrains.annotations;
    requires okhttp3;
    requires okhttp3.sse;
    requires kotlin.stdlib;
    requires it.unimi.dsi.fastutil;

    exports io.opencmw.client.cmwlight;
    exports io.opencmw.client.rest;
    exports io.opencmw.client;

    opens io.opencmw.client to io.opencmw.serialiser;
}