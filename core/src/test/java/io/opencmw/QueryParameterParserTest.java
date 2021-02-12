package io.opencmw;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.opencmw.filter.TimingCtx;

class QueryParameterParserTest {
    private static class TestQueryClass {
        public String param1;
        public int param2;
        public MimeType mimeType;
        public Object specialClass;
        public UnknownClass unknownClass;
        public TimingCtx ctx;
    }

    private static class UnknownClass {
        // empty unknown class
    }

    private static class TestQueryClass2 {
        protected boolean dummyBoolean;
        protected byte dummyByte;
        protected short dummyShort;
        protected int dummyInt;
        protected long dummyLong;
        protected float dummyFloat;
        protected double dummyDouble;
        protected Boolean dummyBoxedBoolean = Boolean.FALSE;
        protected Byte dummyBoxedByte = (byte) 0;
        protected Short dummyBoxedShort = (short) 0;
        protected Integer dummyBoxedInt = 0;
        protected Long dummyBoxedLong = 0L;
        protected Float dummyBoxedFloat = 0f;
        protected Double dummyBoxedDouble = 0.0;
        protected String dummyString1 = "nope";
        protected String dummyString2 = "nope";
        protected String dummyString3;
        protected String dummyString4;
    }

    @Test
    void testMapStringToClass() throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        URI uri = URI.create("https://opencmw.io?param1=Hello&param2=42&mimeType=text/html&specialClass");
        final TestQueryClass ctx = QueryParameterParser.parseQueryParameter(TestQueryClass.class, uri.getQuery());
        assertEquals("Hello", ctx.param1);
        assertEquals(42, ctx.param2);
        assertEquals(MimeType.HTML, ctx.mimeType);
        assertNotNull(ctx.specialClass);

        assertThrows(IllegalArgumentException.class, () -> QueryParameterParser.parseQueryParameter(TestQueryClass.class, "param1=b;param2=c"));
    }

    @Test
    void testMapStringToClassWithMissingParameter() throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        URI uri = URI.create("https://opencmw.io?param1=Hello&param2=42&specialClass");
        final TestQueryClass ctx = QueryParameterParser.parseQueryParameter(TestQueryClass.class, uri.getQuery());
        assertEquals("Hello", ctx.param1);
        assertEquals(42, ctx.param2);
        assertNull(ctx.mimeType); // was missing in parameters
        assertNotNull(ctx.specialClass);

        assertThrows(IllegalArgumentException.class, () -> QueryParameterParser.parseQueryParameter(TestQueryClass.class, "param1=b;param2=c"));
    }

    @Test
    void testMapClassToString() {
        TestQueryClass ctx = new TestQueryClass();
        ctx.param1 = "Hello";
        ctx.param2 = 42;
        ctx.mimeType = MimeType.HTML;
        ctx.ctx = TimingCtx.get("FAIR.SELECTOR.C=2");
        ctx.specialClass = new Object();

        String result = QueryParameterParser.generateQueryParameter(ctx);
        // System.err.println("result = " + result);
        assertNotNull(result);
        assertTrue(result.contains(ctx.param1));
        assertTrue(result.contains("" + ctx.param2));
        assertEquals("param1=Hello&param2=42&mimeType=HTML&specialClass=&unknownClass=&ctx=FAIR.SELECTOR.C%3D2", result);
    }

    @Test
    void testClassToStringFunctions() throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        final String testString = "dummyBoolean=true;dummyBoxedBoolean=true;"
                                  + "dummyByte=2&dummyBoxedByte=2;"
                                  + "dummyShort=3&dummyBoxedShort=3;"
                                  + "dummyInt=4&dummyBoxedInt=4;"
                                  + "dummyLong=5&dummyBoxedLong=5;"
                                  + "dummyFloat=6.&dummyBoxedFloat=6.0;"
                                  + "dummyDouble=7.0&dummyBoxedDouble=7.0;"
                                  + "dummyString1=TestA;dummyString2=\"TestA \";dummyString=;dummyString4=\"equation =%3D5\"";
        final TestQueryClass2 ctx = QueryParameterParser.parseQueryParameter(TestQueryClass2.class, testString);
        assertTrue(ctx.dummyBoolean);
        assertTrue(ctx.dummyBoxedBoolean);
        assertEquals((byte) 2, ctx.dummyByte);
        assertEquals((byte) 2, ctx.dummyBoxedByte);
        assertEquals((short) 3, ctx.dummyShort);
        assertEquals((short) 3, ctx.dummyBoxedShort);
        assertEquals(4, ctx.dummyInt);
        assertEquals(4, ctx.dummyBoxedInt);
        assertEquals(5L, ctx.dummyLong);
        assertEquals(5L, ctx.dummyBoxedLong);
        assertEquals(6.f, ctx.dummyFloat);
        assertEquals(6.f, ctx.dummyBoxedFloat);
        assertEquals(7., ctx.dummyDouble);
        assertEquals(7., ctx.dummyBoxedDouble);
        assertEquals("TestA", ctx.dummyString1);
        assertEquals("\"TestA \"", ctx.dummyString2);
        assertNull(ctx.dummyString3);
        assertEquals("\"equation ==5\"", ctx.dummyString4);
    }

    @Test
    void testMapFunction() {
        URI uri = URI.create("https://opencmw.io?param1=value1&param2=&param3=value3&param3");
        final Map<String, List<String>> map = QueryParameterParser.getMap(uri.getQuery());

        assertNotNull(map.get("param1"));
        assertEquals(1, map.get("param1").size());
        assertEquals("value1", map.get("param1").get(0));

        assertNotNull(map.get("param2"));
        assertEquals(1, map.get("param2").size());
        assertNull(map.get("param2").get(0));

        assertNotNull(map.get("param3"));
        assertEquals(2, map.get("param3").size());
        assertEquals("value3", map.get("param3").get(0));
        assertNull(map.get("param3").get(1));

        assertNull(map.get("param4"), "null for non-existent parameter");

        assertEquals(0, QueryParameterParser.getMap("").size(), "empty map for empty query string");
    }

    @Test
    void testMimetype() {
        assertEquals(MimeType.HTML, QueryParameterParser.getMimeType("contentType=text/html"));
        assertEquals(MimeType.HTML, QueryParameterParser.getMimeType("contentType=HTML"));
        assertEquals(MimeType.HTML, QueryParameterParser.getMimeType("contentType=html"));
        assertEquals(MimeType.HTML, QueryParameterParser.getMimeType("contentType=text/HtmL"));
    }

    @Test
    void testIdentity() {
        final String queryString = "param1=value1a&param2=value2&param3=value3&param1=value1b";
        final Map<String, List<String>> parsedMap = QueryParameterParser.getMap(queryString);
        assertEquals(3, parsedMap.size(), "number of unique parameter");
        assertEquals(2, parsedMap.get("param1").size());
        assertEquals(1, parsedMap.get("param2").size());
        assertEquals(1, parsedMap.get("param3").size());
        assertEquals(List.of("value1a", "value1b"), parsedMap.get("param1"));
        assertEquals(List.of("value2"), parsedMap.get("param2"));
        assertEquals(List.of("value3"), parsedMap.get("param3"));
        final Map<String, Object> returnMap = new HashMap<>(parsedMap);
        returnMap.put("param4", "value4");
        final String returnString = QueryParameterParser.generateQueryParameter(returnMap);

        // test second generated map
        final Map<String, List<String>> parsedMap2 = QueryParameterParser.getMap(returnString);
        assertEquals(4, parsedMap2.size(), "number of unique parameter");
        assertEquals(2, parsedMap2.get("param1").size());
        assertEquals(1, parsedMap2.get("param2").size());
        assertEquals(1, parsedMap2.get("param3").size());
        assertEquals(1, parsedMap2.get("param4").size());
        assertEquals(List.of("value1a", "value1b"), parsedMap2.get("param1"));
        assertEquals(List.of("value2"), parsedMap2.get("param2"));
        assertEquals(List.of("value3"), parsedMap2.get("param3"));
        assertEquals(List.of("value4"), parsedMap2.get("param4"));

        // generate special cases
        final Map<String, Object> testMap1 = new HashMap<>();
        testMap1.put("param1", null);
        assertEquals("param1", QueryParameterParser.generateQueryParameter(testMap1));
        testMap1.put("param2", null);
        assertEquals("param1&param2", QueryParameterParser.generateQueryParameter(testMap1));
        testMap1.put("param3", Arrays.asList(null, null));
        assertEquals("param3&param3&param1&param2", QueryParameterParser.generateQueryParameter(testMap1));
    }

    @Test
    void testAddQueryParameter() throws URISyntaxException {
        final URI baseUri = URI.create("basePath");
        final URI extUri = URI.create("basePath?param1");
        assertEquals(baseUri, QueryParameterParser.appendQueryParameter(baseUri, null));
        assertEquals(baseUri, QueryParameterParser.appendQueryParameter(baseUri, ""));
        assertEquals(URI.create("basePath?test"), QueryParameterParser.appendQueryParameter(baseUri, "test"));
        assertEquals(URI.create("basePath?param1&test"), QueryParameterParser.appendQueryParameter(extUri, "test"));
    }

    @Test
    void testRemoveQueryParameter() throws URISyntaxException {
        final URI origURI = URI.create("basePath?param1=value1a&param2=value2&param3=value3&param1=value1b");

        assertEquals(origURI, QueryParameterParser.removeQueryParameter(origURI, null));
        assertEquals(origURI, QueryParameterParser.removeQueryParameter(origURI, ""));
        assertEquals(URI.create("basePath"), QueryParameterParser.removeQueryParameter(URI.create("basePath"), "bla"));

        // remove 'param1' with specific value 'value1a' - > value1b should remain
        final Map<String, List<String>> reference1 = QueryParameterParser.getMap("param2=value2&param3=value3&param1=value1b");
        final Map<String, List<String>> result1 = QueryParameterParser.getMap(QueryParameterParser.removeQueryParameter(origURI, "param1=value1a").getQuery());
        assertEquals(reference1, result1);

        // remove all 'param1' by key, only 'param2' and 'param3' should remain
        final Map<String, List<String>> reference2 = QueryParameterParser.getMap("param2=value2&param3=value3");
        final Map<String, List<String>> result2 = QueryParameterParser.getMap(QueryParameterParser.removeQueryParameter(origURI, "param1").getQuery());
        assertEquals(reference2, result2);

        // remove 'param1' with specific value 'value1a' and then 'value1b' - > only 'param2' and 'param3' should remain
        final Map<String, List<String>> result3 = QueryParameterParser.getMap(QueryParameterParser.removeQueryParameter(QueryParameterParser.removeQueryParameter(origURI, "param1=value1a"), "param1=value1b").getQuery());
        assertEquals(reference2, result3);
    }
}
