package io.opencmw.domain;

import io.opencmw.MimeType;
import io.opencmw.serialiser.annotations.MetaInfo;

/**
 * basic domain object definition for receiving or sending generic binary data
 */
@MetaInfo(description = "domain object definition for receiving/sending generic binary data")
public class BinaryData {
    public String resourceName = "default";
    public MimeType contentType = MimeType.BINARY;
    public byte[] data;
}
