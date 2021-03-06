module io.opencmw.serialiser {
    requires static de.gsi.chartfx.dataset;
    requires org.slf4j;
    requires jsoniter;
    requires jdk.unsupported;
    requires org.jetbrains.annotations;
    requires it.unimi.dsi.fastutil.core;

    exports io.opencmw.serialiser;
    exports io.opencmw.serialiser.annotations;
    exports io.opencmw.serialiser.spi;
    exports io.opencmw.serialiser.spi.iobuffer;
    exports io.opencmw.serialiser.utils;
}