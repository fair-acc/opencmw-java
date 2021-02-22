package io.opencmw.domain;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.ThrowingSupplier;

import io.opencmw.MimeType;

class BinaryDataTest {
    @Test
    void testFixPreAndPost() {
        Assertions.assertDoesNotThrow(() -> BinaryData.fixPreAndPost("test"));
        assertEquals("/", BinaryData.fixPreAndPost(null));
        assertEquals("/", BinaryData.fixPreAndPost(""));
        assertEquals("/", BinaryData.fixPreAndPost("/"));
        assertEquals("/test/", BinaryData.fixPreAndPost("test"));
        assertEquals("/test/", BinaryData.fixPreAndPost("/test"));
        assertEquals("/test/", BinaryData.fixPreAndPost("test/"));
        assertEquals("/test/", BinaryData.fixPreAndPost("/test/"));
    }

    @Test
    void testGenExportName() {
        Assertions.assertDoesNotThrow(() -> BinaryData.genExportName("test"));
        assertThrows(IllegalArgumentException.class, () -> BinaryData.genExportName(null));
        assertThrows(IllegalArgumentException.class, () -> BinaryData.genExportName(""));
        assertEquals("test", BinaryData.genExportName("test.png"));
        assertEquals("test", BinaryData.genExportName("test/test.png"));
        assertEquals("test", BinaryData.genExportName("/test/test.png"));
        assertEquals("test", BinaryData.genExportName("testA/testB/test.png"));
        assertEquals("test", BinaryData.genExportName("testA/testB/test"));

        Assertions.assertDoesNotThrow(() -> BinaryData.genExportNameData("test.png"));
        assertThrows(IllegalArgumentException.class, () -> BinaryData.genExportNameData(null));
        assertThrows(IllegalArgumentException.class, () -> BinaryData.genExportNameData(""));
        assertEquals("test.png", BinaryData.genExportNameData("test.png"));
        assertEquals("test.png", BinaryData.genExportNameData("test/test.png"));
        assertEquals("test.png", BinaryData.genExportNameData("/test/test.png"));
        assertEquals("test.png", BinaryData.genExportNameData("testA/testB/test.png"));
        assertEquals("test", BinaryData.genExportNameData("testA/testB/test"));
    }

    @Test
    void testConstructor() {
        assertDoesNotThrow((ThrowingSupplier<BinaryData>) BinaryData::new);
        assertDoesNotThrow(() -> new BinaryData("name2", MimeType.BINARY, new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> new BinaryData(null, MimeType.BINARY, new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> new BinaryData("name2", null, new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> new BinaryData("name2", MimeType.BINARY, null));
        assertThrows(IllegalArgumentException.class, () -> new BinaryData("name2", MimeType.BINARY, new byte[0], 1));
    }

    @Test
    void testToString() {
        final BinaryData testData = new BinaryData("name1", MimeType.TEXT, "testText".getBytes(StandardCharsets.UTF_8));
        assertNotNull(testData.toString(), "not null");
        // assert that sub-components are part of the description message
        assertThat(testData.toString(), containsString(BinaryData.class.getSimpleName()));
        assertThat(testData.toString(), containsString("name1"));
        assertThat(testData.toString(), containsString("text/plain"));
        assertThat(testData.toString(), containsString("testText"));
    }

    @Test
    void testEquals() {
        final BinaryData reference = new BinaryData("name1", MimeType.TEXT, "testText".getBytes(StandardCharsets.UTF_8));
        final BinaryData test = new BinaryData("name2", MimeType.BINARY, "otherText".getBytes(StandardCharsets.UTF_8));
        assertThat("equality of identity", reference, is(equalTo(reference)));
        assertThat("inequality for different object", reference, not(equalTo(new Object())));
        assertThat("inequality for resourceName differences", test, not(equalTo(reference)));
        test.resourceName = reference.resourceName;
        assertThat("inequality for MimeType differences", test, not(equalTo(reference)));
        test.contentType = reference.contentType;
        assertThat("inequality for data differences", test, not(equalTo(reference)));
        test.data = reference.data;
        assertThat("equality for content-effective copy", test, is(equalTo(reference)));

        final BinaryData dummy = new BinaryData();
        reference.moveTo(dummy);
        assertEquals(reference, test, "object equality after moveTo(..)");
    }

    @Test
    void testHashCode() {
        final BinaryData reference = new BinaryData("name1", MimeType.TEXT, "testText".getBytes(StandardCharsets.UTF_8));
        final BinaryData test = new BinaryData("name2", MimeType.BINARY, "otherText".getBytes(StandardCharsets.UTF_8));
        assertThat("uninitialised hashCode()", reference.hashCode(), is(not(equalTo(0))));
        assertThat("simple hashCode inequality", reference.hashCode(), is(not(equalTo(test.hashCode()))));
    }

    @Test
    void testGetCategory() {
        Assertions.assertDoesNotThrow(() -> BinaryData.getCategory("test"));
        assertThrows(IllegalArgumentException.class, () -> BinaryData.getCategory(null));
        assertThrows(IllegalArgumentException.class, () -> BinaryData.getCategory(""));
        assertEquals("/", BinaryData.getCategory("test.png"));
        assertEquals("/test/", BinaryData.getCategory("test/test.png"));
        assertEquals("/test/", BinaryData.getCategory("/test/test.png"));
        assertEquals("/testA/testB/", BinaryData.getCategory("testA/testB/test.png"));
        assertEquals("/testA/testB/", BinaryData.getCategory("testA/testB/test"));
    }
}