module io.opencmw.concepts {
    requires org.slf4j;
    requires com.lmax.disruptor;
    requires jeromq;
    requires java.management;
    requires io.opencmw;

    exports io.opencmw.concepts.aggregate;
}