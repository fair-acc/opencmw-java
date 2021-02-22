package io.opencmw.server.domain;

import io.opencmw.MimeType;
import io.opencmw.serialiser.annotations.MetaInfo;

/**
 * basic context description containing only MIME type
 */
@MetaInfo(description = "basic context description containing only MIME type")
public class BasicCtx {
    public MimeType contentType = MimeType.UNKNOWN;
    public Object sse;
    public long longPolling = -1;

    @Override
    public String toString() {
        return "BasicCtx{contentType=" + contentType + ", sse=" + sse + ", longPolling=" + longPolling + '}';
    }
}
