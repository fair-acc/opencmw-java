package io.opencmw.server;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.opencmw.MimeType;
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
        assertEquals("param1=Hello&param2=42&mimeType=HTML&specialClass=&unknownClass=&ctx=FAIR.SELECTOR.C=2", result);
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
                                  + "dummyString1=TestA;dummyString2=\"TestA \";dummyString=";
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
}
