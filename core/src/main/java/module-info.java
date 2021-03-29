open module io.opencmw {
    uses io.opencmw.utils.Settings;
    requires com.google.auto.service;
    requires disruptor;
    requires org.slf4j;
    requires jeromq;
    requires io.opencmw.serialiser;
    requires org.apache.commons.lang3;
    requires org.jetbrains.annotations;
    requires jsoniter;
    requires docopt;

    exports io.opencmw;
    exports io.opencmw.domain;
    exports io.opencmw.filter;
    exports io.opencmw.rbac;
    exports io.opencmw.utils;
}