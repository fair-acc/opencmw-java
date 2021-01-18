module io.opencmw.concepts {
    requires io.opencmw.core;
    requires io.opencmw.serialiser;
    //requires io.opencmw.client;
    requires it.unimi.dsi.fastutil;
    requires io.opencmw.server;
    requires org.slf4j;
    requires disruptor;
    requires jeromq;
    requires java.management;

    exports io.opencmw.concepts.cmwlight;
    exports io.opencmw.concepts.aggregate;
    exports io.opencmw.concepts.majordomo;
    exports io.opencmw.concepts.majordomo.legacy;
}