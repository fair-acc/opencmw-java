package io.opencmw.serialiser.spi;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.InvocationTargetException;
import java.util.*;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opencmw.serialiser.DataType;
import io.opencmw.serialiser.IoBuffer;

/**
 *
 * @author rstein
 */
@SuppressWarnings("PMD.ExcessiveMethodLength")
class JsonSerialiserTests {
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonSerialiser.class);
    private static final int BUFFER_SIZE = 2000;

    @DisplayName("basic tests")
    @ParameterizedTest(name = "IoBuffer class - {0}")
    @ValueSource(classes = { ByteBuffer.class, FastByteBuffer.class })
    void testHeaderAndSpecialItems(final Class<? extends IoBuffer> bufferClass) throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
        assertNotNull(bufferClass, "bufferClass being not null");
        assertNotNull(bufferClass.getConstructor(int.class), "Constructor(Integer) present");
        final IoBuffer buffer = bufferClass.getConstructor(int.class).newInstance(BUFFER_SIZE);
        final JsonSerialiser ioSerialiser = new JsonSerialiser(buffer);

        // add header info
        ioSerialiser.putHeaderInfo();
        // add start marker
        final String dataStartMarkerName = "StartMarker";
        final WireDataFieldDescription dataStartMarker = new WireDataFieldDescription(ioSerialiser, null, dataStartMarkerName, DataType.START_MARKER, -1, -1, -1);
        ioSerialiser.putStartMarker(dataStartMarker);
        // add Collection - List<E>
        final List<Integer> list = Arrays.asList(1, 2, 3);
        ioSerialiser.put("collection", list, Integer.class);
        // add Collection - Set<E>
        final Set<Integer> set = Set.of(1, 2, 3);
        ioSerialiser.put("set", set, Integer.class);
        // add Collection - Queue<E>
        final Queue<Integer> queue = new LinkedList<>(Arrays.asList(1, 2, 3));
        ioSerialiser.put("queue", queue, Integer.class);
        // add Map
        final Map<Integer, String> map = new HashMap<>();
        list.forEach(item -> map.put(item, "Item#" + item.toString()));
        ioSerialiser.put("map", map, Integer.class, String.class);
        // add Enum
        ioSerialiser.put("enum", DataType.ENUM);
        // add end marker
        final String dataEndMarkerName = "EndMarker";
        final WireDataFieldDescription dataEndMarker = new WireDataFieldDescription(ioSerialiser, null, dataEndMarkerName, DataType.START_MARKER, -1, -1, -1);
        ioSerialiser.putEndMarker(dataEndMarker); // end start marker
        ioSerialiser.putEndMarker(dataEndMarker); // end header info

        buffer.flip();

        final String result = new String(Arrays.copyOfRange(buffer.elements(), 0, buffer.limit()));
        LOGGER.atDebug().addArgument(result).log("serialised:\n{}");

        final Iterator<String> lines = result.lines().iterator();
        assertEquals("{", lines.next());
        assertEquals("  \"StartMarker\": {", lines.next());
        assertEquals("    \"collection\": [1, 2, 3],", lines.next());
        assertTrue(lines.next().matches(" {4}\"set\": \\[[123], [123], [123]],"));
        assertEquals("    \"queue\": [1, 2, 3],", lines.next());
        assertEquals("    \"map\": {\"1\": \"Item#1\", \"2\": \"Item#2\", \"3\": \"Item#3\"},", lines.next());
        assertEquals("    \"enum\": \"ENUM\"", lines.next());
        assertEquals("  }", lines.next());
        assertEquals("", lines.next());
        assertEquals("}", lines.next());
        assertFalse(lines.hasNext());
        // check types
        assertEquals(0, buffer.position(), "initial buffer position");

        // header info
        ProtocolInfo headerInfo = ioSerialiser.checkHeaderInfo();
        assertNotEquals(new Object(), headerInfo); // silly comparison for coverage reasons
        assertNotNull(headerInfo);
        assertEquals(JsonSerialiser.class.getCanonicalName(), headerInfo.getProducerName());
        assertEquals(1, headerInfo.getVersionMajor());
        assertEquals(0, headerInfo.getVersionMinor());
        assertEquals(0, headerInfo.getVersionMicro());
    }

    @DisplayName("basic primitive array writer tests")
    @ParameterizedTest(name = "IoBuffer class - {0}")
    @ValueSource(classes = { ByteBuffer.class, FastByteBuffer.class })
    void testParseIoStream(final Class<? extends IoBuffer> bufferClass) throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException { // NOSONAR NOPMD
        assertNotNull(bufferClass, "bufferClass being not null");
        assertNotNull(bufferClass.getConstructor(int.class), "Constructor(Integer) present");
        final IoBuffer buffer = bufferClass.getConstructor(int.class).newInstance(2 * BUFFER_SIZE); // a bit larger buffer since we test more cases at once
        final JsonSerialiser ioSerialiser = new JsonSerialiser(buffer);

        ioSerialiser.putHeaderInfo(); // add header info

        // add some primitives
        ioSerialiser.put("boolean", true);
        ioSerialiser.put("byte", (byte) 42);
        ioSerialiser.put("char", (char) 40d);
        ioSerialiser.put("short", (short) 42);
        ioSerialiser.put("int", 42);
        ioSerialiser.put("long", 42L);
        ioSerialiser.put("float", 42f);
        ioSerialiser.put("double", 42);
        ioSerialiser.put("string", "string");

        ioSerialiser.put("boolean[]", new boolean[] { true }, 1);
        ioSerialiser.put("byte[]", new byte[] { (byte) 42 }, 1);
        ioSerialiser.put("char[]", new char[] { (char) 40 }, 1);
        ioSerialiser.put("short[]", new short[] { (short) 42 }, 1);
        ioSerialiser.put("int[]", new int[] { 42 }, 1);
        ioSerialiser.put("long[]", new long[] { 42L }, 1);
        ioSerialiser.put("float[]", new float[] { (float) 42 }, 1);
        ioSerialiser.put("double[]", new double[] { (double) 42 }, 1);
        ioSerialiser.put("string[]", new String[] { "string" }, 1);

        final Collection<Integer> collection = Arrays.asList(1, 2, 3);
        ioSerialiser.put("collection", collection, Integer.class); // add Collection - List<E>

        final List<Integer> list = Arrays.asList(1, 2, 3);
        ioSerialiser.put("list", list, Integer.class); // add Collection - List<E>

        final Set<Integer> set = Set.of(1, 2, 3);
        ioSerialiser.put("set", set, Integer.class); // add Collection - Set<E>

        final Queue<Integer> queue = new LinkedList<>(Arrays.asList(1, 2, 3));
        ioSerialiser.put("queue", queue, Integer.class); // add Collection - Queue<E>

        final Map<Integer, String> map = new HashMap<>();
        list.forEach(item -> map.put(item, "Item#" + item.toString()));
        ioSerialiser.put("map", map, Integer.class, String.class); // add Map

        // ioSerialiser.put("enum", DataType.ENUM); // enums cannot be read back in, because there is not type information

        // start nested data
        final String nestedContextName = "nested context";
        final WireDataFieldDescription nestedContextMarker = new WireDataFieldDescription(ioSerialiser, null, nestedContextName, DataType.START_MARKER, -1, -1, -1);
        ioSerialiser.putStartMarker(nestedContextMarker); // add start marker
        ioSerialiser.put("booleanArray", new boolean[] { true }, 1);
        ioSerialiser.put("byteArray", new byte[] { (byte) 0x42 }, 1);

        ioSerialiser.putEndMarker(nestedContextMarker); // add end marker
        // end nested data

        final String dataEndMarkerName = "Life is good!";
        final WireDataFieldDescription dataEndMarker = new WireDataFieldDescription(ioSerialiser, null, dataEndMarkerName, DataType.START_MARKER, -1, -1, -1);
        ioSerialiser.putEndMarker(dataEndMarker); // add end marker

        buffer.flip();

        final String result = new String(Arrays.copyOfRange(buffer.elements(), 0, buffer.limit()));
        LOGGER.atDebug().addArgument(result).log("serialised:\n{}");

        final Iterator<String> lines = result.lines().iterator();
        assertEquals("{", lines.next());
        assertEquals("  \"boolean\": true,", lines.next());
        assertEquals("  \"byte\": 42,", lines.next());
        assertEquals("  \"char\": 40,", lines.next());
        assertEquals("  \"short\": 42,", lines.next());
        assertEquals("  \"int\": 42,", lines.next());
        assertEquals("  \"long\": 42,", lines.next());
        assertEquals("  \"float\": 42.0,", lines.next());
        assertEquals("  \"double\": 42,", lines.next());
        assertEquals("  \"string\": \"string\",", lines.next());
        assertEquals("  \"boolean[]\": [true],", lines.next());
        assertEquals("  \"byte[]\": [42],", lines.next());
        assertEquals("  \"char[]\": [40],", lines.next());
        assertEquals("  \"short[]\": [42],", lines.next());
        assertEquals("  \"int[]\": [42],", lines.next());
        assertEquals("  \"long[]\": [42],", lines.next());
        assertEquals("  \"float[]\": [42.0],", lines.next());
        assertEquals("  \"double[]\": [42.0],", lines.next());
        assertEquals("  \"string[]\": [\"string\"],", lines.next());
        assertEquals("  \"collection\": [1, 2, 3],", lines.next());
        assertEquals("  \"list\": [1, 2, 3],", lines.next());
        assertTrue(lines.next().matches(" {2}\"set\": \\[[123], [123], [123]],"));
        assertEquals("  \"queue\": [1, 2, 3],", lines.next());
        assertEquals("  \"map\": {\"1\": \"Item#1\", \"2\": \"Item#2\", \"3\": \"Item#3\"},", lines.next());
        // assertEquals("  \"enum\": ENUM,", lines.next());
        assertEquals("  \"nested context\": {", lines.next());
        assertEquals("    \"booleanArray\": [true],", lines.next());
        assertEquals("    \"byteArray\": [66]", lines.next());
        assertEquals("  }", lines.next());
        assertEquals("", lines.next());
        assertEquals("}", lines.next());
        assertFalse(lines.hasNext());

        // and read back streamed items. Note that types get widened, arrays -> list etc due to type info lost
        final WireDataFieldDescription objectRoot = ioSerialiser.parseIoStream(true);
        assertNotNull(objectRoot);
        objectRoot.printFieldStructure();
    }
    @Test
    @DisplayName("Simple Object in List")
    void testObjectAlongPrimitives() {
        final JsonSerialiser ioSerialiser = new JsonSerialiser(new FastByteBuffer(1000, true, null));

        ioSerialiser.putHeaderInfo();

        final SimpleClass simpleObj = new SimpleClass();
        simpleObj.setValues();
        final SimpleClass simpleObj2 = new SimpleClass();
        simpleObj2.foo = "baz";
        simpleObj2.integer = 42;
        simpleObj2.switches = Collections.emptyList();
        ioSerialiser.put("SimpleObjects", List.of(simpleObj, simpleObj2), SimpleClass.class);

        ioSerialiser.putEndMarker(new WireDataFieldDescription(ioSerialiser, null, "end marker", DataType.END_MARKER, -1, -1, -1));

        ioSerialiser.getBuffer().flip();

        final String result = new String(Arrays.copyOfRange(ioSerialiser.getBuffer().elements(), 0, ioSerialiser.getBuffer().limit()));
        LOGGER.atDebug().addArgument(result).log("serialised:\n{}");

        final Iterator<String> lines = result.lines().iterator();
        assertEquals("{", lines.next());
        assertEquals("  \"SimpleObjects\": [{\"integer\":1337,\"foo\":\"bar\",\"switches\":[true,true,false]}, {\"integer\":42,\"foo\":\"baz\",\"switches\":[]}]", lines.next());
        assertEquals("}", lines.next());
        assertFalse(lines.hasNext());

        // ensures that it is valid json
        final WireDataFieldDescription header = ioSerialiser.parseIoStream(true);

        // reading back is not possible, because we cannot specify the type of the list and get a list of maps
        ioSerialiser.setQueryFieldName("SimpleObjects", -1);
        final List<Map<String, ?>> recovered = ioSerialiser.getList(new ArrayList<>());
        assertThat(recovered, Matchers.contains(Matchers.aMapWithSize(3), Matchers.aMapWithSize(3)));
    }

    @Test
    @DisplayName("Simple object (de)serialisation")
    void testSimpleObjectSerDe() {
        final SimpleClass toSerialise = new SimpleClass();
        toSerialise.setValues();

        final JsonSerialiser ioSerialiser = new JsonSerialiser(new FastByteBuffer(1000, true, null));

        ioSerialiser.serialiseObject(toSerialise);

        ioSerialiser.getBuffer().flip();

        final String result = new String(Arrays.copyOfRange(ioSerialiser.getBuffer().elements(), 0, ioSerialiser.getBuffer().limit()));
        LOGGER.atDebug().addArgument(result).log("serialised:\n{}");

        assertEquals("{\"integer\":1337,\"foo\":\"bar\",\"switches\":[true,true,false]}", result);

        final SimpleClass deserialized = ioSerialiser.deserialiseObject(new SimpleClass());
        assertEquals(toSerialise, deserialized);
    }

    @Test
    @DisplayName("Null object (de)serialisation")
    void testNullObjectSerDe() {
        final JsonSerialiser ioSerialiser = new JsonSerialiser(new FastByteBuffer(1000, true, null));

        ioSerialiser.serialiseObject(null);

        ioSerialiser.getBuffer().flip();

        final String result = new String(Arrays.copyOfRange(ioSerialiser.getBuffer().elements(), 0, ioSerialiser.getBuffer().limit()));
        LOGGER.atDebug().addArgument(result).log("serialised:\n{}");

        assertEquals("null", result);

        final SimpleClass deserialized = ioSerialiser.deserialiseObject(new SimpleClass());
        assertNull(deserialized);
    }

    public static class SimpleClass {
        public int integer = -1;
        public String foo = null;
        public List<Boolean> switches = null;

        public void setValues() {
            integer = 1337;
            foo = "bar";
            switches = List.of(true, true, false);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o)
                return true;
            if (!(o instanceof SimpleClass that))
                return false;
            return integer == that.integer && Objects.equals(foo, that.foo) && Objects.equals(switches, that.switches);
        }

        @Override
        public int hashCode() {
            return Objects.hash(integer, foo, switches);
        }
    }
}