open module io.opencmw.server {
    requires org.slf4j;
    requires io.opencmw.serialiser;
    requires io.opencmw;
    requires jeromq;
    requires java.management;
    requires it.unimi.dsi.fastutil;
    requires org.apache.commons.lang3;
    requires velocity.engine.core;
    requires org.jetbrains.annotations;

    exports io.opencmw.server;
}