open module io.opencmw.server {
    requires org.slf4j;
    requires io.opencmw.serialiser;
    requires io.opencmw;
    requires jeromq;
    requires java.management;
    requires java.validation;

    exports io.opencmw.server;
}