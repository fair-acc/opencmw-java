package io.opencmw.domain;

import java.util.Arrays;
import java.util.Objects;

import org.jetbrains.annotations.NotNull;
import org.zeromq.util.ZData;

import io.opencmw.MimeType;
import io.opencmw.serialiser.annotations.MetaInfo;

/**
 * basic domain object definition for receiving or sending generic binary data
 */
@MetaInfo(description = "domain object definition for receiving/sending generic binary data")
public class BinaryData {
    public String resourceName = "default";
    public MimeType contentType = MimeType.UNKNOWN;
    public byte[] data = {};
    public int dataSize = -1;

    public BinaryData() {
        // default constructor
    }

    public BinaryData(final String resourceName, final MimeType contentType, final byte[] data) {
        this(resourceName, contentType, data, -1);
    }

    @SuppressWarnings("PMD.ArrayIsStoredDirectly")
    public BinaryData(final String resourceName, final MimeType contentType, final byte[] data, final int dataSize) {
        this.resourceName = resourceName;
        this.contentType = contentType;
        this.data = data;
        this.dataSize = dataSize;
        checkConsistency(); // NOPMD
    }

    public void checkConsistency() {
        if (resourceName == null || resourceName.isBlank()) {
            throw new IllegalArgumentException("resourceName must not be blank");
        }
        if (contentType == null) {
            throw new IllegalArgumentException("mimeType must not be blank");
        }
        if (data == null || (dataSize >= 0 && data.length < dataSize)) {
            throw new IllegalArgumentException("data[" + (data == null ? "null" : data.length) + "] must be larger than dataSize=" + dataSize);
        }
    }

    public void moveTo(final @NotNull BinaryData other) {
        other.resourceName = this.resourceName;
        other.contentType = this.contentType;
        other.data = this.data;
        other.dataSize = this.dataSize;
        other.checkConsistency(); // NOPMD
    }

    @Override
    public String toString() {
        return "BinaryData{resourceName='" + resourceName + "', contentType=" + (contentType == null ? "null" : contentType.name()) + "(\"" + contentType + "\"), dataSize=" + dataSize + ", data=" + ZData.toString(data) + '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BinaryData)) {
            return false;
        }
        final BinaryData that = (BinaryData) o;

        if (!Objects.equals(resourceName, that.resourceName) || contentType != that.contentType
                || (data == null && that.data != null) || (data != null && that.data == null)) {
            return false;
        }
        if (data == null) {
            return true;
        }
        final int minSize = dataSize >= 0 ? Math.min(data.length, dataSize) : data.length;
        return Arrays.equals(data, 0, minSize, that.data, 0, that.data.length);
    }

    @Override
    public int hashCode() {
        int result = resourceName == null ? 0 : resourceName.hashCode();
        result = 31 * result + (contentType == null ? 0 : contentType.hashCode());
        result = 31 * result + Arrays.hashCode(data);
        return result;
    }

    protected static String fixPreAndPost(final String name) {
        final String nonNullName = name == null ? "/" : name.trim();
        final String fixedPrefix = (nonNullName.startsWith("/") ? nonNullName : '/' + nonNullName);
        return fixedPrefix.endsWith("/") ? fixedPrefix : fixedPrefix + '/';
    }

    protected static String genExportName(final String name) {
        checkField("genExportName(name)", name);
        int p = name.lastIndexOf('/');
        if (p < 0) {
            p = 0;
        }
        int e = name.lastIndexOf('.');
        if (e < 0) {
            e = name.length();
        }
        return name.substring(p, e).replace("/", "");
    }

    protected static String genExportNameData(final String name) {
        checkField("genExportNameData(name)", name);
        int p = name.lastIndexOf('/');
        if (p < 0) {
            p = 0;
        }
        return name.substring(p).replace("/", "");
    }

    protected static String getCategory(final String name) {
        checkField("getCategory(name)", name);
        final int p = name.lastIndexOf('/');
        if (p < 0) {
            return fixPreAndPost("");
        }
        return fixPreAndPost(name.substring(0, p + 1));
    }

    private static void checkField(final String field, final String category) {
        if (category == null) {
            throw new IllegalArgumentException(field + "category not be null");
        }
        if (category.isBlank()) {
            throw new IllegalArgumentException(field + "must not be blank");
        }
    }
}
