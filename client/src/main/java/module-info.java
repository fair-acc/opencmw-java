module io.opencmw.client {
    requires com.lmax.disruptor;
    requires io.opencmw;
    requires io.opencmw.serialiser;
    requires it.unimi.dsi.fastutil.core;
    requires java.management;
    requires jeromq;
    requires kotlin.stdlib;
    requires okhttp3;
    requires okhttp3.sse;
    requires org.apache.commons.lang3;
    requires org.jetbrains.annotations;
    requires org.slf4j;
    requires micrometer.core;

    exports io.opencmw.client;
    exports io.opencmw.client.cmwlight;
    exports io.opencmw.client.rest;

    opens io.opencmw.client to io.opencmw.serialiser;
}