open module io.opencmw {
    requires disruptor;
    requires org.slf4j;
    requires jeromq;
    requires io.opencmw.serialiser;
    requires java.validation;
    requires org.apache.commons.lang3;
    requires org.jetbrains.annotations;

    exports io.opencmw;
    exports io.opencmw.filter;
    exports io.opencmw.rbac;
    exports io.opencmw.utils;
}