module io.opencmw.serialiser {
    requires static de.gsi.chartfx.dataset;
    requires org.slf4j;
    requires jsoniter;
    requires it.unimi.dsi.fastutil;
    requires jdk.unsupported;

    exports io.opencmw.serialiser;
    exports io.opencmw.serialiser.spi;
    exports io.opencmw.serialiser.spi.iobuffer;
    exports io.opencmw.serialiser.annotations;
    exports io.opencmw.serialiser.utils;
}