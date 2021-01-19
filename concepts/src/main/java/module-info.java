module io.opencmw.concepts {
    requires io.opencmw.serialiser;
    requires it.unimi.dsi.fastutil;
    requires org.slf4j;
    requires disruptor;
    requires jeromq;
    requires java.management;
    requires io.opencmw;

    exports io.opencmw.concepts.cmwlight;
    exports io.opencmw.concepts.aggregate;
    exports io.opencmw.concepts.majordomo;
    exports io.opencmw.concepts.majordomo.legacy;
}