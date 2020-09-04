package io.opencmw.serialiser.spi;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.opencmw.serialiser.IoBuffer;
import io.opencmw.serialiser.utils.ByteArrayCache;

/**
 * @author rstein
 */
class IoBufferTests {
    protected static final boolean[] booleanTestArray = { true, false, true, false };
    protected static final byte[] byteTestArray = { 100, 101, 102, 103, -100, -101, -102, -103 };
    protected static final short[] shortTestArray = { -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5 }; // NOPMD by rstein
    protected static final int[] intTestArray = { 5, 4, 3, 2, 1, 0, -1, -2, -3, -4, -5 };
    protected static final long[] longTestArray = { Integer.MAX_VALUE, Integer.MAX_VALUE - 1, -Integer.MAX_VALUE + 2 };
    protected static final float[] floatTestArray = { 1.1e9f, 1.2e9f, 1.3e9f, -1.1e9f, -1.2e9f, -1.3e9f };
    protected static final double[] doubleTestArray = { Float.MAX_VALUE + 1.1e9, Float.MAX_VALUE + 1.2e9, Float.MAX_VALUE + 1.3e9f, -Float.MAX_VALUE - 1.1e9f, -Float.MAX_VALUE - 1.2e9f, Float.MAX_VALUE - 1.3e9f };
    protected static final char[] charTestArray = { 'a', 'b', 'c', 'd' };
    protected static final String[] stringTestArray = { "Is", "this", "the", "real", "life?", "Is", "this", "just", "fantasy?", "", null };
    protected static final String[] stringTestArrayNullAsEmpty = Arrays.stream(stringTestArray).map(s -> s == null ? "" : s).toArray(String[]::new);
    private static final int BUFFER_SIZE = 1000;

    @ParameterizedTest(name = "IoBuffer class - {0}")
    @ValueSource(classes = { FastByteBuffer.class }) // trim is not implemented for ByteBuffer
    @SuppressWarnings("PMD.ExcessiveMethodLength")
    void trimTest(final Class<? extends IoBuffer> bufferClass) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        assertNotNull(bufferClass, "bufferClass being not null");
        assertNotNull(bufferClass.getConstructor(int.class), "Constructor(Integer) present");
        final IoBuffer buffer = bufferClass.getConstructor(int.class).newInstance(10);

        buffer.position(5);
        buffer.trim(12);
        assertEquals(10, buffer.capacity());
        buffer.trim(7);
        assertEquals(7, buffer.capacity());
        buffer.trim();
        assertEquals(5, buffer.capacity());
        buffer.trim(3);
        assertEquals(5, buffer.capacity());
    }

    @ParameterizedTest(name = "IoBuffer class - {0}")
    @ValueSource(classes = { ByteBuffer.class, FastByteBuffer.class })
    @SuppressWarnings("PMD.ExcessiveMethodLength")
    void primitivesArrays(final Class<? extends IoBuffer> bufferClass) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        assertNotNull(bufferClass, "bufferClass being not null");
        assertNotNull(bufferClass.getConstructor(int.class), "Constructor(Integer) present");
        final IoBuffer buffer = bufferClass.getConstructor(int.class).newInstance(BUFFER_SIZE);

        assertNotNull(buffer.toString());

        {
            buffer.reset();
            assertDoesNotThrow(() -> buffer.putBooleanArray(booleanTestArray, booleanTestArray.length));
            assertDoesNotThrow(() -> buffer.putBooleanArray(booleanTestArray, -1));
            assertDoesNotThrow(() -> buffer.putBooleanArray(null, 5));
            buffer.flip();
            assertArrayEquals(booleanTestArray, buffer.getBooleanArray());
            assertArrayEquals(booleanTestArray, buffer.getBooleanArray());
            assertEquals(0, buffer.getBooleanArray().length);
        }

        {
            buffer.reset();
            assertDoesNotThrow(() -> buffer.putByteArray(byteTestArray, byteTestArray.length));
            assertDoesNotThrow(() -> buffer.putByteArray(byteTestArray, -1));
            assertDoesNotThrow(() -> buffer.putByteArray(null, 5));
            buffer.flip();
            assertArrayEquals(byteTestArray, buffer.getByteArray());
            assertArrayEquals(byteTestArray, buffer.getByteArray());
            assertEquals(0, buffer.getByteArray().length);
        }

        {
            buffer.reset();
            assertDoesNotThrow(() -> buffer.putShortArray(shortTestArray, shortTestArray.length));
            assertDoesNotThrow(() -> buffer.putShortArray(shortTestArray, -1));
            assertDoesNotThrow(() -> buffer.putShortArray(null, 5));
            buffer.flip();
            assertArrayEquals(shortTestArray, buffer.getShortArray());
            assertArrayEquals(shortTestArray, buffer.getShortArray());
            assertEquals(0, buffer.getShortArray().length);
        }

        {
            buffer.reset();
            assertDoesNotThrow(() -> buffer.putIntArray(intTestArray, intTestArray.length));
            assertDoesNotThrow(() -> buffer.putIntArray(intTestArray, -1));
            assertDoesNotThrow(() -> buffer.putIntArray(null, 5));
            buffer.flip();
            assertArrayEquals(intTestArray, buffer.getIntArray());
            assertArrayEquals(intTestArray, buffer.getIntArray());
            assertEquals(0, buffer.getIntArray().length);
        }

        {
            buffer.reset();
            assertDoesNotThrow(() -> buffer.putLongArray(longTestArray, longTestArray.length));
            assertDoesNotThrow(() -> buffer.putLongArray(longTestArray, -1));
            assertDoesNotThrow(() -> buffer.putLongArray(null, 5));
            buffer.flip();
            assertArrayEquals(longTestArray, buffer.getLongArray());
            assertArrayEquals(longTestArray, buffer.getLongArray());
            assertEquals(0, buffer.getLongArray().length);
        }

        {
            buffer.reset();
            assertDoesNotThrow(() -> buffer.putFloatArray(floatTestArray, floatTestArray.length));
            assertDoesNotThrow(() -> buffer.putFloatArray(floatTestArray, -1));
            assertDoesNotThrow(() -> buffer.putFloatArray(null, 5));
            buffer.flip();
            assertArrayEquals(floatTestArray, buffer.getFloatArray());
            assertArrayEquals(floatTestArray, buffer.getFloatArray());
            assertEquals(0, buffer.getFloatArray().length);
        }

        {
            buffer.reset();
            assertDoesNotThrow(() -> buffer.putDoubleArray(doubleTestArray, doubleTestArray.length));
            assertDoesNotThrow(() -> buffer.putDoubleArray(doubleTestArray, -1));
            assertDoesNotThrow(() -> buffer.putDoubleArray(null, 5));
            buffer.flip();
            assertArrayEquals(doubleTestArray, buffer.getDoubleArray());
            assertArrayEquals(doubleTestArray, buffer.getDoubleArray());
            assertEquals(0, buffer.getDoubleArray().length);
        }

        {
            buffer.reset();
            assertDoesNotThrow(() -> buffer.putCharArray(charTestArray, charTestArray.length));
            assertDoesNotThrow(() -> buffer.putCharArray(charTestArray, -1));
            assertDoesNotThrow(() -> buffer.putCharArray(null, 5));
            buffer.flip();
            assertArrayEquals(charTestArray, buffer.getCharArray());
            assertArrayEquals(charTestArray, buffer.getCharArray());
            assertEquals(0, buffer.getCharArray().length);
        }

        {
            buffer.reset();
            assertDoesNotThrow(() -> buffer.putStringArray(stringTestArray, stringTestArray.length));
            assertDoesNotThrow(() -> buffer.putStringArray(stringTestArray, -1));
            assertDoesNotThrow(() -> buffer.putStringArray(null, 5));
            buffer.flip();
            assertArrayEquals(stringTestArrayNullAsEmpty, buffer.getStringArray());
            assertArrayEquals(stringTestArrayNullAsEmpty, buffer.getStringArray());
            assertEquals(0, buffer.getStringArray().length);
        }
    }

    @Test
    void primitivesArraysASCII() {
        FastByteBuffer buffer = new FastByteBuffer(BUFFER_SIZE);

        {
            final char[] chars = Character.toChars(0x1F701);
            final String fourByteCharacter = new String(chars);
            String utf8TestString = "Γειά σου Κόσμε! - " + fourByteCharacter + " 語 \u00ea \u00f1 \u00fc + some normal ASCII character";
            buffer.reset();
            assertDoesNotThrow(() -> buffer.putStringArray(stringTestArray, stringTestArray.length));
            assertDoesNotThrow(() -> buffer.putStringArray(stringTestArray, -1));
            assertDoesNotThrow(() -> buffer.putStringArray(null, 5));
            buffer.putString(utf8TestString);
            buffer.flip();
            assertArrayEquals(stringTestArrayNullAsEmpty, buffer.getStringArray());
            assertArrayEquals(stringTestArrayNullAsEmpty, buffer.getStringArray());
            assertEquals(0, buffer.getStringArray().length);
            assertEquals(utf8TestString, buffer.getString());
        }

        buffer.setEnforceSimpleStringEncoding(true);
        {
            buffer.reset();
            assertDoesNotThrow(() -> buffer.putStringArray(stringTestArray, stringTestArray.length));
            assertDoesNotThrow(() -> buffer.putStringArray(stringTestArray, -1));
            assertDoesNotThrow(() -> buffer.putStringArray(null, 5));
            buffer.putString("Hello World!");
            buffer.flip();
            assertArrayEquals(stringTestArrayNullAsEmpty, buffer.getStringArray());
            assertArrayEquals(stringTestArrayNullAsEmpty, buffer.getStringArray());
            assertEquals(0, buffer.getStringArray().length);
            assertEquals("Hello World!", buffer.getString());
        }
    }

    @ParameterizedTest(name = "IoBuffer class - {0}")
    @ValueSource(classes = { ByteBuffer.class, FastByteBuffer.class })
    void primitivesMixed(final Class<? extends IoBuffer> bufferClass) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        assertNotNull(bufferClass, "bufferClass being not null");
        assertNotNull(bufferClass.getConstructor(int.class), "Constructor(Integer) present");
        final IoBuffer buffer = bufferClass.getConstructor(int.class).newInstance(BUFFER_SIZE);
        final long largeLong = (long) Integer.MAX_VALUE + (long) 10;

        buffer.reset();
        buffer.putBoolean(true);
        buffer.putBoolean(false);
        buffer.putByte((byte) 0xFE);
        buffer.putShort((short) 43);
        buffer.putInt(1025);
        buffer.putLong(largeLong);
        buffer.putFloat(1.3e10f);
        buffer.putDouble(1.3e10f);
        buffer.putChar('@');
        buffer.putChar((char) 513);
        buffer.putStringISO8859("Hello World!");
        buffer.putString("Γειά σου Κόσμε!");
        final long position = buffer.position();

        // return to start position
        buffer.flip();
        assertTrue(buffer.getBoolean());
        assertFalse(buffer.getBoolean());
        assertEquals(buffer.getByte(), (byte) 0xFE);
        assertEquals(buffer.getShort(), (short) 43);
        assertEquals(1025, buffer.getInt());
        assertEquals(buffer.getLong(), largeLong);
        assertEquals(1.3e10f, buffer.getFloat());
        assertEquals(1.3e10f, buffer.getDouble());
        assertEquals('@', buffer.getChar());
        assertEquals((char) 513, buffer.getChar());
        assertEquals("Hello World!", buffer.getStringISO8859());
        assertEquals("Γειά σου Κόσμε!", buffer.getString());
        assertEquals(buffer.position(), position);
    }

    @ParameterizedTest(name = "IoBuffer class - {0}")
    @ValueSource(classes = { ByteBuffer.class, FastByteBuffer.class })
    void primitivesSimple(final Class<? extends IoBuffer> bufferClass) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        assertNotNull(bufferClass, "bufferClass being not null");
        assertNotNull(bufferClass.getConstructor(int.class), "Constructor(Integer) present");
        final IoBuffer buffer = bufferClass.getConstructor(int.class).newInstance(BUFFER_SIZE);
        buffer.reset();
        buffer.putBoolean(true);
        buffer.flip();
        assertTrue(buffer.getBoolean());

        buffer.reset();
        buffer.putBoolean(false);
        buffer.flip();
        assertFalse(buffer.getBoolean());

        buffer.reset();
        buffer.putByte((byte) 0xFE);
        buffer.flip();
        assertEquals(buffer.getByte(), (byte) 0xFE);

        buffer.reset();
        buffer.putShort((short) 43);
        buffer.flip();
        assertEquals(buffer.getShort(), (short) 43);

        buffer.reset();
        buffer.putInt(1025);
        buffer.flip();
        assertEquals(1025, buffer.getInt());

        buffer.reset();
        final long largeLong = (long) Integer.MAX_VALUE + (long) 10;
        buffer.putLong(largeLong);
        buffer.flip();
        assertEquals(buffer.getLong(), largeLong);

        buffer.reset();
        buffer.putFloat(1.3e10f);
        buffer.flip();
        assertEquals(1.3e10f, buffer.getFloat());

        buffer.reset();
        buffer.putDouble(1.3e10f);
        buffer.flip();
        assertEquals(1.3e10f, buffer.getDouble());

        buffer.reset();
        buffer.putChar('@');
        buffer.flip();
        assertEquals('@', buffer.getChar());

        buffer.reset();
        buffer.putChar((char) 513);
        buffer.flip();
        assertEquals((char) 513, buffer.getChar());

        buffer.reset();
        buffer.putString("Hello World!");
        buffer.flip();
        assertEquals("Hello World!", buffer.getString());
    }

    @ParameterizedTest(name = "IoBuffer class - {0}")
    @ValueSource(classes = { ByteBuffer.class, FastByteBuffer.class })
    void primitivesSimpleInPlace(final Class<? extends IoBuffer> bufferClass) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        assertNotNull(bufferClass, "bufferClass being not null");
        assertNotNull(bufferClass.getConstructor(int.class), "Constructor(Integer) present");
        final IoBuffer buffer = bufferClass.getConstructor(int.class).newInstance(BUFFER_SIZE);
        buffer.reset();

        buffer.putBoolean(0, true);
        assertTrue(buffer.getBoolean(0));

        buffer.reset();
        buffer.putBoolean(0, false);
        assertFalse(buffer.getBoolean(0));

        buffer.putByte(1, (byte) 0xFE);
        assertEquals(buffer.getByte(1), (byte) 0xFE);

        buffer.putShort(2, (short) 43);
        assertEquals(buffer.getShort(2), (short) 43);

        buffer.putInt(3, 1025);
        assertEquals(1025, buffer.getInt(3));

        final long largeLong = (long) Integer.MAX_VALUE + (long) 10;
        buffer.putLong(4, largeLong);
        assertEquals(buffer.getLong(4), largeLong);

        buffer.putFloat(5, 1.3e10f);
        assertEquals(1.3e10f, buffer.getFloat(5));

        buffer.putDouble(6, 1.3e10f);
        assertEquals(1.3e10f, buffer.getDouble(6));

        buffer.putChar(7, '@');
        assertEquals('@', buffer.getChar(7));

        buffer.putChar(7, (char) 513);
        assertEquals((char) 513, buffer.getChar(7));

        buffer.putString(8, "Hello World!");
        assertEquals("Hello World!", buffer.getString(8));
    }

    @ParameterizedTest(name = "IoBuffer class - {0}")
    @ValueSource(classes = { ByteBuffer.class, FastByteBuffer.class })
    void indexManipulations(final Class<? extends IoBuffer> bufferClass) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        assertNotNull(bufferClass, "bufferClass being not null");
        assertNotNull(bufferClass.getConstructor(int.class), "Constructor(Integer) present");
        final IoBuffer buffer = bufferClass.getConstructor(int.class).newInstance(10);

        assertEquals(0, buffer.position());
        assertEquals(10, buffer.limit());
        assertEquals(10, buffer.capacity());

        assertThrows(IllegalArgumentException.class, () -> buffer.limit(11)); // limit > capacity
        buffer.position(5);
        buffer.limit(8);
        assertEquals(5, buffer.position());
        assertEquals(8, buffer.limit());
        assertEquals(10, buffer.capacity());
        assertTrue(buffer.hasRemaining());
        assertEquals(3, buffer.remaining());

        buffer.flip();
        assertEquals(0, buffer.position());
        assertEquals(5, buffer.limit());
        assertEquals(10, buffer.capacity());

        assertThrows(IllegalArgumentException.class, () -> buffer.position(6)); // pos > limit
        buffer.position(4);
        buffer.limit(3);
        assertEquals(3, buffer.position());
        assertEquals(3, buffer.limit());
        assertEquals(10, buffer.capacity());
        assertFalse(buffer.hasRemaining());

        buffer.reset();
    }

    @Test
    void testFastByteBufferAllocators() {
        {
            FastByteBuffer buffer = new FastByteBuffer();
            assertTrue(buffer.capacity() > 0);
            assertEquals(0, buffer.position());
            assertEquals(buffer.limit(), buffer.capacity());
            buffer.limit(buffer.capacity() - 2);
            assertEquals(buffer.limit(), (buffer.capacity() - 2));
            assertFalse(buffer.isReadOnly());
        }

        {
            FastByteBuffer buffer = new FastByteBuffer(500);
            assertEquals(500, buffer.capacity());
        }

        {
            FastByteBuffer buffer = new FastByteBuffer(new byte[1000], 500);
            assertEquals(1000, buffer.capacity());
            assertEquals(500, buffer.limit());
            assertThrows(IllegalArgumentException.class, () -> new FastByteBuffer(new byte[5], 10));
        }

        {
            FastByteBuffer buffer = FastByteBuffer.wrap(byteTestArray);
            assertArrayEquals(byteTestArray, buffer.elements());
        }
    }

    @Test
    void testFastByteBufferResizing() {
        FastByteBuffer buffer = new FastByteBuffer(300, true, new ByteArrayCache());
        assertTrue(buffer.isAutoResize());
        assertNotNull(buffer.getByteArrayCache());
        assertEquals(0, buffer.getByteArrayCache().size());
        assertEquals(300, buffer.capacity());

        buffer.limit(200); // shift limit to index 200
        assertEquals(200, buffer.remaining()); // N.B. == 200 - pos (0);

        buffer.ensureAdditionalCapacity(200); // should be NOP
        assertEquals(200, buffer.remaining());
        assertEquals(300, buffer.capacity());

        buffer.forceCapacity(300, 0); // does no reallocation but moves limit to end
        assertEquals(300, buffer.remaining()); // N.B. == 200 - pos (0);

        buffer.ensureCapacity(400);
        assertThat(buffer.capacity(), Matchers.greaterThan(400));

        buffer.putByteArray(new byte[100], 100);
        // N.B. int (4 bytes) for array size, n*4 Bytes for actual array
        final long sizeArray = (FastByteBuffer.SIZE_OF_INT + 100 * FastByteBuffer.SIZE_OF_BYTE);
        assertEquals(104, sizeArray);
        assertEquals(sizeArray, buffer.position());

        assertThat(buffer.capacity(), Matchers.greaterThan(400));
        buffer.trim();
        assertEquals(buffer.capacity(), buffer.position());

        buffer.ensureCapacity(500);
        buffer.trim(333);
        assertEquals(333, buffer.capacity());

        buffer.position(0);
        assertEquals(0, buffer.position());

        buffer.trim();
        assertFalse(buffer.hasRemaining());
        buffer.ensureAdditionalCapacity(100);
        assertTrue(buffer.hasRemaining());
        assertThat(buffer.capacity(), Matchers.greaterThan(1124));

        buffer.limit(50);
        buffer.clear();
        assertEquals(0, buffer.position());
        assertEquals(buffer.limit(), buffer.capacity());

        // test resize related getters and setters
        buffer.setAutoResize(false);
        assertFalse(buffer.isAutoResize());
        buffer.setByteArrayCache(null);
        assertNull(buffer.getByteArrayCache());
    }

    @Test
    void testFastByteBufferOutOfBounds() {
        final FastByteBuffer buffer = FastByteBuffer.wrap(new byte[50]);
        // test single getters
        buffer.position(47);
        assertThrows(IndexOutOfBoundsException.class, buffer::getInt);
        assertThrows(IndexOutOfBoundsException.class, () -> buffer.getInt(47));
        assertThrows(IndexOutOfBoundsException.class, buffer::getLong);
        assertThrows(IndexOutOfBoundsException.class, () -> buffer.getLong(44));
        assertThrows(IndexOutOfBoundsException.class, buffer::getDouble);
        assertThrows(IndexOutOfBoundsException.class, () -> buffer.getDouble(47));
        assertThrows(IndexOutOfBoundsException.class, buffer::getFloat);
        assertThrows(IndexOutOfBoundsException.class, () -> buffer.getFloat(47));
        buffer.position(49);
        assertThrows(IndexOutOfBoundsException.class, buffer::getShort);
        assertThrows(IndexOutOfBoundsException.class, () -> buffer.getShort(49));
        buffer.position(50);
        assertThrows(IndexOutOfBoundsException.class, buffer::getBoolean);
        assertThrows(IndexOutOfBoundsException.class, () -> buffer.getBoolean(50));
        assertThrows(IndexOutOfBoundsException.class, buffer::getByte);
        assertThrows(IndexOutOfBoundsException.class, () -> buffer.getByte(50));
        // test array getters
        // INT
        buffer.reset();
        assertThrows(IndexOutOfBoundsException.class, () -> buffer.putIntArray(new int[50], 50));
        assertEquals(0, buffer.position());
        buffer.putIntArray(new int[5], 5);
        buffer.flip();
        buffer.limit(FastByteBuffer.SIZE_OF_INT * 4);
        assertThrows(IndexOutOfBoundsException.class, () -> buffer.getIntArray(null, 5));
        // LONG
        buffer.reset();
        assertThrows(IndexOutOfBoundsException.class, () -> buffer.putLongArray(new long[50], 50));
        assertEquals(0, buffer.position());
        buffer.putLongArray(new long[5], 5);
        buffer.flip();
        buffer.limit(FastByteBuffer.SIZE_OF_LONG * 4);
        assertThrows(IndexOutOfBoundsException.class, () -> buffer.getLongArray(null, 5));
        // SHORT
        buffer.reset();
        assertThrows(IndexOutOfBoundsException.class, () -> buffer.putShortArray(new short[50], 50));
        assertEquals(0, buffer.position());
        buffer.putShortArray(new short[5], 5);
        buffer.flip();
        buffer.limit(FastByteBuffer.SIZE_OF_SHORT * 4);
        assertThrows(IndexOutOfBoundsException.class, () -> buffer.getShortArray(null, 5));
        // CHAR
        buffer.reset();
        assertThrows(IndexOutOfBoundsException.class, () -> buffer.putCharArray(new char[50], 50));
        assertEquals(0, buffer.position());
        buffer.putCharArray(new char[5], 5);
        buffer.flip();
        buffer.limit(FastByteBuffer.SIZE_OF_CHAR * 4);
        assertThrows(IndexOutOfBoundsException.class, () -> buffer.getCharArray(null, 5));
        // BOOLEAN
        buffer.reset();
        assertThrows(IndexOutOfBoundsException.class, () -> buffer.putBooleanArray(new boolean[51], 51));
        assertEquals(0, buffer.position());
        buffer.putBooleanArray(new boolean[5], 5);
        buffer.flip();
        buffer.limit(FastByteBuffer.SIZE_OF_BOOLEAN * 4);
        assertThrows(IndexOutOfBoundsException.class, () -> buffer.getBooleanArray(null, 5));
        // BYTE
        buffer.reset();
        assertThrows(IndexOutOfBoundsException.class, () -> buffer.putByteArray(new byte[51], 51));
        assertEquals(0, buffer.position());
        buffer.putByteArray(new byte[5], 5);
        buffer.flip();
        buffer.limit(FastByteBuffer.SIZE_OF_BYTE * 4);
        assertThrows(IndexOutOfBoundsException.class, () -> buffer.getByteArray(null, 5));
        // DOUBLE
        buffer.reset();
        assertThrows(IndexOutOfBoundsException.class, () -> buffer.putDoubleArray(new double[50], 50));
        assertEquals(0, buffer.position());
        buffer.putDoubleArray(new double[5], 5);
        buffer.flip();
        buffer.limit(FastByteBuffer.SIZE_OF_DOUBLE * 4);
        assertThrows(IndexOutOfBoundsException.class, () -> buffer.getDoubleArray(null, 5));
        // FLOAT
        buffer.reset();
        assertThrows(IndexOutOfBoundsException.class, () -> buffer.putFloatArray(new float[50], 50));
        assertEquals(0, buffer.position());
        buffer.putFloatArray(new float[5], 5);
        buffer.flip();
        buffer.limit(FastByteBuffer.SIZE_OF_FLOAT * 4);
        assertThrows(IndexOutOfBoundsException.class, () -> buffer.getFloatArray(null, 5));
        // STRING
        buffer.reset();
        assertThrows(IndexOutOfBoundsException.class, () -> buffer.putStringArray(new String[50], 50));
        assertEquals(0, buffer.position());
        buffer.putStringArray(new String[5], 5);
        buffer.flip();
        buffer.limit(10);
        assertThrows(IndexOutOfBoundsException.class, () -> buffer.getStringArray(null, 4));
    }
}
