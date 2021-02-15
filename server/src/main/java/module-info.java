open module io.opencmw.server {
    requires org.slf4j;
    requires io.opencmw.serialiser;
    requires io.opencmw;
    requires jeromq;
    requires java.management;
    requires java.validation;
    requires org.apache.commons.lang3;
    requires velocity.engine.core;

    exports io.opencmw.server;
}