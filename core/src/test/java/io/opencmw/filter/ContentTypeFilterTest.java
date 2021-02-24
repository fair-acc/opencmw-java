package io.opencmw.filter;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import io.opencmw.MimeType;

class ContentTypeFilterTest {
    @Test
    void clear() {
        final ContentTypeFilter filter = new ContentTypeFilter(MimeType.HTML.getMediaType());
        assertEquals(MimeType.HTML, filter.contentType);
        filter.clear();
        assertEquals(MimeType.UNKNOWN, filter.contentType);
    }

    @Test
    void copyTo() {
        final ContentTypeFilter filter1 = new ContentTypeFilter(MimeType.HTML.getMediaType());
        final ContentTypeFilter filter2 = new ContentTypeFilter(MimeType.BINARY.toString());
        filter1.copyTo(filter2);
        assertEquals(filter1, filter2);
    }

    @Test
    void getKey() {
        assertEquals("contentType", new ContentTypeFilter().getKey());
    }

    @Test
    void getValue() {
        final ContentTypeFilter filter = new ContentTypeFilter(MimeType.HTML.getMediaType());
        assertEquals("text/html", filter.getValue());
    }

    @Test
    void get() {
        final ContentTypeFilter filter1 = new ContentTypeFilter(MimeType.BINARY.getMediaType());
        final ContentTypeFilter filter2 = filter1.get(MimeType.BINARY.toString());
        assertEquals(filter1, filter2);
    }

    @Test
    void equalsHashCode() {
        final ContentTypeFilter filter1 = new ContentTypeFilter(MimeType.BINARY.getMediaType());
        final ContentTypeFilter filter2 = filter1.get(MimeType.BINARY.toString());
        assertEquals(filter1, filter1);
        assertEquals(filter1, filter2);
        assertNotEquals(filter1, null);
        assertEquals(filter1.hashCode(), filter2.hashCode());
        assertNotEquals(filter1.hashCode(), filter2.get("text/html"));
    }

    @Test
    void matches() {
        final ContentTypeFilter filter1 = new ContentTypeFilter(MimeType.BINARY.getMediaType());
        final ContentTypeFilter filter2 = filter1.get(MimeType.BINARY.toString());
        assertTrue(filter1.matches(filter2));
        assertTrue(filter2.matches(filter1));
        assertFalse(filter1.matches(null));
    }

    @Test
    void testToString() {
        final ContentTypeFilter filter1 = new ContentTypeFilter(MimeType.HTML.getMediaType());
        assertNotNull(filter1.toString());
        assertFalse(filter1.toString().isBlank());
    }
}