package io.opencmw.server.rest.helper;

import io.opencmw.MimeType;
import io.opencmw.filter.TimingCtx;
import io.opencmw.serialiser.annotations.MetaInfo;

public class TestContext {
    @MetaInfo(description = "FAIR timing context selector, e.g. FAIR.SELECTOR.C=0, ALL, ...")
    public TimingCtx ctx = TimingCtx.get("FAIR.SELECTOR.ALL");
    @MetaInfo(unit = "a.u.", description = "random test parameter")
    public String testValue = "default value";
    @MetaInfo(description = "requested MIME content type, eg. 'application/binary', 'text/html','text/json', ..")
    public MimeType contentType = MimeType.BINARY;
    @MetaInfo(description = "when set property is notified/updated (usually) on data change by the service")
    public boolean subscribe;

    public TestContext() {
        // needs default constructor
    }

    @Override
    public String toString() {
        return "TestContext{ctx=" + ctx + ", testValue='" + testValue + "', contentType=" + contentType + '}';
    }
}
