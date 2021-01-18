module io.opencmw.core {
    requires disruptor;
    requires org.slf4j;
    requires de.gsi.chartfx.dataset;
    requires jeromq;
    requires io.opencmw.serialiser;
    requires org.apache.commons.lang3;

    opens io.opencmw.core.datasource to io.opencmw.serialiser;

    exports io.opencmw.core;
    exports io.opencmw.core.utils;
    exports io.opencmw.core.datasource;
    exports io.opencmw.core.filter;
}