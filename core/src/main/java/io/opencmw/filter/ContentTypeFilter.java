package io.opencmw.filter;

import io.opencmw.Filter;
import io.opencmw.MimeType;

/**
 * Simple contentType/MIME-type filter
 *
 * @author rstein
 */
class ContentTypeFilter implements Filter {
    public static final String KEY = "contentType";
    public MimeType contentType = MimeType.UNKNOWN;

    public ContentTypeFilter() {
        // public constructor
    }

    public ContentTypeFilter(final String ctxValue) {
        this();
        contentType = MimeType.getEnum(ctxValue);
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ContentTypeFilter)) {
            return false;
        }
        return contentType == ((ContentTypeFilter) other).contentType;
    }

    @Override
    public int hashCode() {
        return contentType == null ? 0 : contentType.hashCode();
    }

    @Override
    public void clear() {
        contentType = MimeType.UNKNOWN;
    }

    @Override
    public void copyTo(final Filter other) {
        if (!(other instanceof ContentTypeFilter)) {
            return;
        }
        ((ContentTypeFilter) other).contentType = contentType;
    }

    @Override
    public String getKey() {
        return KEY;
    }

    @Override
    public String getValue() {
        return contentType.getMediaType();
    }

    @Override
    public ContentTypeFilter get(final String value) {
        return new ContentTypeFilter(value);
    }

    @Override
    public boolean matches(final Filter other) {
        if (!(other instanceof ContentTypeFilter)) {
            return false;
        }
        return this.contentType == ((ContentTypeFilter) other).contentType;
    }

    @Override
    public String toString() {
        return contentType.toString();
    }
}
