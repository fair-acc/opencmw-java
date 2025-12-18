package io.opencmw.server.rest.helper;

import io.opencmw.MimeType;
import io.opencmw.filter.TimingCtx;
import io.opencmw.serialiser.annotations.MetaInfo;

@MetaInfo(description = "request type class description")
public class RequestDataType {
    @MetaInfo(description = " RequestDataType name to show up in the OpenAPI docs")
    public String name = "";
    @MetaInfo(description = "FAIR timing context selector, e.g. FAIR.SELECTOR.C=0, ALL, ...")
    public TimingCtx ctx = TimingCtx.getStatic("FAIR.SELECTOR.ALL");
    @MetaInfo(description = "custom filter")
    public String customFilter = "";
    @MetaInfo(description = "requested MIME content type, eg. 'application/binary', 'text/html','text/json', ..")
    public MimeType contentType = MimeType.BINARY;

    public RequestDataType() {
        // needs default constructor
    }

    @Override
    public String toString() {
        return "RequestDataType{name='" + name + "', ctx=" + ctx + ", customFilter='" + customFilter + "'}";
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (!(o instanceof RequestDataType))
            return false;
        final RequestDataType that = (RequestDataType) o;
        if (!name.equals(that.name))
            return false;
        if (!ctx.equals(that.ctx))
            return false;
        return customFilter.equals(that.customFilter);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + ctx.hashCode();
        result = 31 * result + customFilter.hashCode();
        return result;
    }
}
