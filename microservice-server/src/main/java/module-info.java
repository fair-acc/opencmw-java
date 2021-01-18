module io.opencmw.server {
    requires org.slf4j;
    requires io.opencmw.serialiser;
    requires io.opencmw.core;
    requires jeromq;
    requires io.javalin;

    exports io.opencmw.server.rbac;
}