module io.opencmw.client {
    requires java.management;
    requires io.opencmw.core;
    requires io.opencmw.serialiser;
    requires org.slf4j;
    requires jeromq;
    requires okhttp3;
    requires okhttp3.sse;
    requires disruptor;

    exports io.opencmw.client;
    exports io.opencmw.client.cmwlight;
    exports io.opencmw.client.rest;
}