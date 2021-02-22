package io.opencmw.serialiser.spi;

import static sun.misc.Unsafe.*; // NOSONAR NOPMD not an issue: contained and performance-related use

import java.lang.reflect.Field;
import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import io.opencmw.serialiser.IoBuffer;
import io.opencmw.serialiser.utils.AssertUtils;
import io.opencmw.serialiser.utils.ByteArrayCache;

import sun.misc.Unsafe; // NOPMD - there is still nothing better under the Sun

// import static jdk.internal.misc.Unsafe; // NOPMD by rstein TODO replaces sun in JDK11

/**
 * FastByteBuffer implementation based on JVM 'Unsafe' Class. based on:
 * https://mechanical-sympathy.blogspot.com/2012/07/native-cc-like-performance-for-java.html
 * http://java-performance.info/various-methods-of-binary-serialization-in-java/
 *
 * All accesses are range checked, because the performance impact was determined to be negligible.
 *
 * Read operations return "IndexOutOfBoundsException" if there are not enough bytes left in the buffer.
 * For primitive types, the check can be done before, but for arrays and strings the size field has to be read first.
 * Therefore, the position after a failed non-primitive read is not necessarily the position before the read attempt.
 *
 * When there is not enough space for a write operation, the behaviour depends on the autoRange and byteArrayCache
 * variables. If autoRange is false, the operation returns an IndexOutOfBounds exception and the position is set to the
 * position before the operation. For Strings there is a worst case space estimate being done, so an operation might
 * fail with enough space left. If autoRange is true, the underlying byte array is replaced by a bigger one which is at
 * least 1 KiB byte or 12,5% , but at max 100 KiB bigger than the requested size. When a byteArrayCache is provided, the
 * next biggest array in that cache is used and the old buffer is returned, otherwise a new byte[] array is allocated.
 *
 * @author rstein
 */
@SuppressWarnings({ "restriction", "PMD.TooManyMethods", "PMD.ExcessivePublicCount", "PMD.ExcessiveClassLength" }) // unavoidable: each primitive type needs to handled individually (no templates)
public class FastByteBuffer implements IoBuffer {
    public static final int SIZE_OF_BOOLEAN = 1;
    public static final int SIZE_OF_BYTE = 1;
    public static final int SIZE_OF_SHORT = 2;
    public static final int SIZE_OF_CHAR = 2;
    public static final int SIZE_OF_INT = 4;
    public static final int SIZE_OF_LONG = 8;
    public static final int SIZE_OF_FLOAT = 4;
    public static final int SIZE_OF_DOUBLE = 8;
    public static final String INVALID_UTF_8 = "Invalid UTF-8";
    private static final int DEFAULT_INITIAL_CAPACITY = 1 << 10;
    private static final int DEFAULT_MIN_CAPACITY_INCREASE = 1 << 10;
    private static final int DEFAULT_MAX_CAPACITY_INCREASE = 100 * (1 << 10);
    private static final Unsafe unsafe; // NOPMD
    static {
        // get an instance of the otherwise private 'Unsafe' class
        try {
            @SuppressWarnings("Java9ReflectionClassVisibility")
            Class<?> cls = Class.forName("jdk.internal.module.IllegalAccessLogger"); // NOSONAR NOPMD
            Field logger = cls.getDeclaredField("logger");

            final Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true); //NOSONAR
            unsafe = (Unsafe) field.get(null);
            unsafe.putObjectVolatile(cls, unsafe.staticFieldOffset(logger), null);

        } catch (NoSuchFieldException | SecurityException | IllegalAccessException | ClassNotFoundException e) { // NOPMD
            throw new SecurityException(e); // NOPMD
        }
    }

    private final ReadWriteLock internalLock = new ReentrantReadWriteLock();
    private final StringBuilder builder = new StringBuilder(100); // NOPMD
    private ByteArrayCache byteArrayCache;
    private int intPos;
    private int intLimit;
    private byte[] buffer;
    private boolean enforceSimpleStringEncoding;
    private boolean autoResize;

    /**
     * construct new FastByteBuffer backed by a default length array
     */
    public FastByteBuffer() {
        this(DEFAULT_INITIAL_CAPACITY);
    }
    /**
     * construct new FastByteBuffer
     *
     * @param buffer buffer to initialise/re-use (stored directly)
     * @param limit position until buffer is filled
     */
    @SuppressWarnings("PMD.ArrayIsStoredDirectly")
    public FastByteBuffer(final byte[] buffer, final int limit) {
        Objects.requireNonNull(buffer, "buffer");
        if (buffer.length < limit) {
            throw new IllegalArgumentException(String.format("limit %d >= capacity %d", limit, buffer.length));
        }
        this.buffer = buffer;
        this.intLimit = limit;
        intPos = 0;
    }

    /**
     * construct new FastByteBuffer
     *
     * @param size initial capacity of the buffer
     */
    public FastByteBuffer(final int size) {
        this(size, false, null);
    }

    /**
     * construct new FastByteBuffer
     *
     * @param size initial capacity of the buffer
     * @param autoResize whether the buffer should be resized automatically when trying to write past capacity
     * @param byteArrayCache a ByteArrayCache from which arrays are obtained and returned to on resize
     */
    public FastByteBuffer(final int size, final boolean autoResize, final ByteArrayCache byteArrayCache) {
        AssertUtils.gtEqThanZero("size", size);
        buffer = new byte[size];
        intPos = 0;
        intLimit = buffer.length;
        this.autoResize = autoResize;
        this.byteArrayCache = byteArrayCache;
    }

    @Override
    public int capacity() {
        return buffer.length;
    }

    public void checkAvailable(final int bytes) {
        if (intPos + bytes > intLimit) {
            throw new IndexOutOfBoundsException("read unavailable " + bytes + " bytes at position " + intPos + " (limit: " + intLimit + ")");
        }
    }

    public void checkAvailableAbsolute(final int position) {
        if (position > intLimit) {
            throw new IndexOutOfBoundsException("read unavailable bytes at end position " + position + " (limit: " + intLimit + ")");
        }
    }

    @Override
    public void clear() {
        intPos = 0;
        intLimit = capacity();
    }

    /**
     *
     * @return access to internal storage array (N.B. this is volatile and may be overwritten in case of auto-grow, or external sets)
     */
    @Override
    public byte[] elements() {
        return buffer; // NOPMD -- allow public access to internal array
    }

    @Override
    public void ensureAdditionalCapacity(final int capacity) {
        ensureCapacity(this.position() + capacity);
    }

    @Override
    public void ensureCapacity(final int newCapacity) {
        if (newCapacity <= capacity()) {
            return;
        }
        if (intPos > capacity()) { // invalid state, should never occur
            throw new IllegalStateException("position " + intPos + " is beyond buffer capacity " + capacity());
        }
        if (!autoResize) {
            throw new IndexOutOfBoundsException("required capacity: " + newCapacity + " out of bounds: " + capacity() + " and autoResize is disabled");
        }
        //TODO: add smarter enlarging algorithm (ie. increase fast for small arrays, + n% for medium sized arrays, byte-by-byte for large arrays)
        final int addCapacity = Math.min(Math.max(DEFAULT_MIN_CAPACITY_INCREASE, newCapacity >> 3), DEFAULT_MAX_CAPACITY_INCREASE); // min, +12.5%, max
        // if we are reading, limit() marks valid data, when writing, position() marks end of valid data, limit() is safe bet because position <= limit
        forceCapacity(newCapacity + addCapacity, limit());
    }

    @Override
    public void flip() {
        intLimit = intPos;
        intPos = 0;
    }

    /**
     * Forces FastByteBuffer to contain the given number of entries, preserving just a part of the array.
     *
     * @param length the new minimum length for this array.
     * @param preserve the number of elements of the old buffer that shall be preserved in case a new allocation is necessary.
     */
    @Override
    public void forceCapacity(final int length, final int preserve) {
        if (length == capacity()) {
            intLimit = length;
            return;
        }
        final byte[] newBuffer = byteArrayCache == null ? new byte[length] : byteArrayCache.getArray(length);
        final int bytesToCopy = preserve * SIZE_OF_BYTE;
        copyMemory(buffer, ARRAY_BYTE_BASE_OFFSET, newBuffer, ARRAY_BYTE_BASE_OFFSET, bytesToCopy);
        intPos = Math.min(intPos, newBuffer.length);
        if (byteArrayCache != null) {
            byteArrayCache.add(buffer);
        }
        buffer = newBuffer;
        intLimit = buffer.length;
    }

    @Override
    public boolean getBoolean() { // NOPMD by rstein
        checkAvailable(SIZE_OF_BOOLEAN);
        final boolean value = unsafe.getBoolean(buffer, (long) ARRAY_BYTE_BASE_OFFSET + intPos);
        intPos += SIZE_OF_BOOLEAN;

        return value;
    }

    @Override
    public boolean getBoolean(final int position) {
        checkAvailableAbsolute(position + SIZE_OF_BOOLEAN);
        return unsafe.getBoolean(buffer, (long) ARRAY_BYTE_BASE_OFFSET + position);
    }

    @Override
    public boolean[] getBooleanArray(final boolean[] dst, final int length) {
        final int arraySize = getInt(); // strided-array size
        final boolean initNeeded = dst == null || length < 0 || dst.length != arraySize;
        final boolean[] values = initNeeded ? new boolean[arraySize] : dst;

        checkAvailable(arraySize * SIZE_OF_BOOLEAN);
        copyMemory(buffer, ARRAY_BYTE_BASE_OFFSET + intPos, values, ARRAY_BOOLEAN_BASE_OFFSET, arraySize);
        intPos += arraySize;

        return values;
    }

    @Override
    public byte getByte() {
        checkAvailable(SIZE_OF_BYTE);
        final byte value = unsafe.getByte(buffer, (long) ARRAY_BYTE_BASE_OFFSET + intPos);
        intPos += SIZE_OF_BYTE;

        return value;
    }

    @Override
    public byte getByte(final int position) {
        checkAvailableAbsolute(position + SIZE_OF_BYTE);
        return unsafe.getByte(buffer, (long) ARRAY_BYTE_BASE_OFFSET + position);
    }

    @Override
    public byte[] getByteArray(final byte[] dst, final int length) {
        final int arraySize = getInt(); // strided-array size
        final boolean initNeeded = dst == null || length < 0 || dst.length != arraySize;
        final byte[] values = initNeeded ? new byte[arraySize] : dst;

        checkAvailable(arraySize);
        copyMemory(buffer, ARRAY_BYTE_BASE_OFFSET + intPos, values, ARRAY_BYTE_BASE_OFFSET, arraySize);
        intPos += arraySize;

        return values;
    }

    public ByteArrayCache getByteArrayCache() {
        return byteArrayCache;
    }

    @Override
    public char getChar() {
        checkAvailable(SIZE_OF_CHAR);
        final char value = unsafe.getChar(buffer, (long) ARRAY_CHAR_BASE_OFFSET + intPos);
        intPos += SIZE_OF_CHAR;

        return value;
    }

    @Override
    public char getChar(final int position) {
        checkAvailableAbsolute(position + SIZE_OF_CHAR);
        return unsafe.getChar(buffer, (long) ARRAY_BYTE_BASE_OFFSET + position);
    }

    @Override
    public char[] getCharArray(final char[] dst, final int length) {
        final int arraySize = getInt(); // strided-array size
        final boolean initNeeded = dst == null || length < 0 || dst.length != arraySize;
        final char[] values = initNeeded ? new char[arraySize] : dst;

        final int bytesToCopy = arraySize * SIZE_OF_CHAR;
        checkAvailable(bytesToCopy);
        copyMemory(buffer, ARRAY_BYTE_BASE_OFFSET + intPos, values, ARRAY_SHORT_BASE_OFFSET, bytesToCopy);
        intPos += bytesToCopy;

        return values;
    }

    @Override
    public double getDouble() {
        checkAvailable(SIZE_OF_DOUBLE);
        final double value = unsafe.getDouble(buffer, (long) ARRAY_BYTE_BASE_OFFSET + intPos);
        intPos += SIZE_OF_DOUBLE;

        return value;
    }

    @Override
    public double getDouble(final int position) {
        checkAvailableAbsolute(position + SIZE_OF_DOUBLE);
        return unsafe.getDouble(buffer, (long) ARRAY_BYTE_BASE_OFFSET + position);
    }

    @Override
    public double[] getDoubleArray(final double[] dst, final int length) {
        final int arraySize = getInt(); // strided-array size
        final boolean initNeeded = dst == null || length < 0 || dst.length != arraySize;
        final double[] values = initNeeded ? new double[arraySize] : dst;

        final int bytesToCopy = arraySize * SIZE_OF_DOUBLE;
        checkAvailable(bytesToCopy);
        copyMemory(buffer, ARRAY_BYTE_BASE_OFFSET + intPos, values, ARRAY_DOUBLE_BASE_OFFSET, bytesToCopy);
        intPos += bytesToCopy;

        return values;
    }

    @Override
    public float getFloat() {
        checkAvailable(SIZE_OF_FLOAT);
        final float value = unsafe.getFloat(buffer, (long) ARRAY_BYTE_BASE_OFFSET + intPos);
        intPos += SIZE_OF_FLOAT;

        return value;
    }

    @Override
    public float getFloat(final int position) {
        checkAvailableAbsolute(position + SIZE_OF_FLOAT);
        return unsafe.getFloat(buffer, (long) ARRAY_BYTE_BASE_OFFSET + position);
    }

    @Override
    public float[] getFloatArray(final float[] dst, final int length) {
        final int arraySize = getInt(); // strided-array size
        final boolean initNeeded = dst == null || length < 0 || dst.length != arraySize;
        final float[] values = initNeeded ? new float[arraySize] : dst;

        final int bytesToCopy = arraySize * SIZE_OF_FLOAT;
        checkAvailable(bytesToCopy);
        copyMemory(buffer, ARRAY_BYTE_BASE_OFFSET + intPos, values, ARRAY_FLOAT_BASE_OFFSET, bytesToCopy);
        intPos += bytesToCopy;

        return values;
    }

    @Override
    public int getInt() {
        checkAvailable(SIZE_OF_INT);
        final int value = unsafe.getInt(buffer, (long) ARRAY_BYTE_BASE_OFFSET + intPos);
        intPos += SIZE_OF_INT;

        return value;
    }

    @Override
    public int getInt(final int position) {
        checkAvailableAbsolute(position + SIZE_OF_INT);
        return unsafe.getInt(buffer, (long) ARRAY_BYTE_BASE_OFFSET + position);
    }

    @Override
    public int[] getIntArray(final int[] dst, final int length) {
        final int arraySize = getInt(); // strided-array size
        final boolean initNeeded = dst == null || length < 0 || dst.length != arraySize;
        final int[] values = initNeeded ? new int[arraySize] : dst;

        final int bytesToCopy = arraySize * SIZE_OF_INT;
        checkAvailable(bytesToCopy);
        copyMemory(buffer, ARRAY_BYTE_BASE_OFFSET + intPos, values, ARRAY_INT_BASE_OFFSET, bytesToCopy);
        intPos += bytesToCopy;

        return values;
    }

    @Override
    public long getLong() {
        checkAvailable(SIZE_OF_LONG);
        final long value = unsafe.getLong(buffer, (long) ARRAY_BYTE_BASE_OFFSET + intPos);
        intPos += SIZE_OF_LONG;

        return value;
    }

    @Override
    public long getLong(final int position) {
        checkAvailableAbsolute(position + SIZE_OF_LONG);
        return unsafe.getLong(buffer, (long) ARRAY_BYTE_BASE_OFFSET + position);
    }

    @Override
    public long[] getLongArray(final long[] dst, final int length) {
        final int arraySize = getInt(); // strided-array size
        final boolean initNeeded = dst == null || length < 0 || dst.length != arraySize;
        final long[] values = initNeeded ? new long[arraySize] : dst;

        final int bytesToCopy = arraySize * SIZE_OF_LONG;
        checkAvailable(bytesToCopy);
        copyMemory(buffer, ARRAY_BYTE_BASE_OFFSET + intPos, values, ARRAY_LONG_BASE_OFFSET, bytesToCopy);
        intPos += bytesToCopy;

        return values;
    }

    @Override
    public short getShort() { // NOPMD by rstein
        checkAvailable(SIZE_OF_SHORT);
        final short value = unsafe.getShort(buffer, (long) ARRAY_BYTE_BASE_OFFSET + intPos); // NOPMD
        intPos += SIZE_OF_SHORT;

        return value;
    }

    @Override
    public short getShort(final int position) {
        checkAvailableAbsolute(position + SIZE_OF_SHORT);
        return unsafe.getShort(buffer, (long) ARRAY_BYTE_BASE_OFFSET + position);
    }

    @Override
    public short[] getShortArray(final short[] dst, final int length) { // NOPMD by rstein
        final int arraySize = getInt(); // strided-array size
        final boolean initNeeded = dst == null || length < 0 || dst.length != arraySize;
        final short[] values = initNeeded ? new short[arraySize] : dst; // NOPMD by rstein

        final int bytesToCopy = arraySize * SIZE_OF_SHORT;
        checkAvailable(bytesToCopy);
        copyMemory(buffer, ARRAY_BYTE_BASE_OFFSET + intPos, values, ARRAY_SHORT_BASE_OFFSET, bytesToCopy);
        intPos += bytesToCopy;

        return values;
    }

    @Override
    public String getString() {
        if (isEnforceSimpleStringEncoding()) {
            return this.getStringISO8859();
        }
        final int arraySize = getInt(); // for C++ zero terminated string
        checkAvailable(arraySize);
        // alt: final String str = new String(buffer, position, arraySize - 1, StandardCharsets.UTF_8)
        decodeUTF8(buffer, intPos, arraySize - 1, builder);

        intPos += arraySize; // N.B. +1 larger to be compatible with C++ zero terminated string
        // alt: return str
        return builder.toString();
    }

    @Override
    public String getString(final int position) {
        final int oldPosition = position();
        position(position);
        final String ret = getString();
        position(oldPosition);
        return ret;
    }

    @Override
    public String[] getStringArray(final String[] dst, final int length) {
        final int arraySize = getInt(); // strided-array size
        checkAvailable(arraySize);
        final boolean initNeeded = dst == null || length < 0 || dst.length != arraySize;
        final String[] ret = initNeeded ? new String[arraySize] : dst;
        for (int k = 0; k < arraySize; k++) {
            ret[k] = getString();
        }
        return ret;
    }

    @Override
    public String getStringISO8859() {
        final int arraySize = getInt(); // for C++ zero terminated string
        checkAvailable(arraySize);
        //alt safe-fallback final String str = new String(buffer,  position, arraySize - 1, StandardCharsets.ISO_8859_1)
        @SuppressWarnings("deprecation")
        final String str = new String(buffer, 0, intPos, arraySize - 1); // NOSONAR NOPMD fastest alternative that is public API
        // final String str = FastStringBuilder.iso8859BytesToString(buffer, position, arraySize - 1)
        intPos += arraySize; // N.B. +1 larger to be compatible with C++ zero terminated string
        return str;
    }

    @Override
    public boolean hasRemaining() {
        return (this.position() < limit());
    }

    /**
     * @return True if the underlying byte array will be replaced by a bigger one if there is not enough space left
     */
    public boolean isAutoResize() {
        return autoResize;
    }

    @Override
    public boolean isEnforceSimpleStringEncoding() {
        return enforceSimpleStringEncoding;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public int limit() {
        return intLimit;
    }

    @Override
    public void limit(final int newLimit) {
        if ((newLimit > capacity()) || (newLimit < 0)) {
            throw new IllegalArgumentException(String.format("invalid newLimit: [0, position: %d, newLimit:%d, %d]", intPos, newLimit, capacity()));
        }
        intLimit = newLimit;
        if (intPos > intLimit) {
            intPos = intLimit;
        }
    }

    @Override
    public ReadWriteLock lock() {
        return internalLock;
    }

    @Override
    public int position() {
        return intPos;
    }

    @Override
    public void position(final int newPosition) {
        if ((newPosition > intLimit) || (newPosition < 0) || (newPosition > capacity())) {
            throw new IllegalArgumentException(String.format("invalid newPosition: %d vs. [0, position=%d, limit:%d, capacity:%d]", newPosition, intPos, intLimit, capacity()));
        }
        intPos = newPosition;
    }

    @Override
    public void putBoolean(final boolean value) {
        ensureAdditionalCapacity(SIZE_OF_BOOLEAN);
        unsafe.putBoolean(buffer, (long) ARRAY_BYTE_BASE_OFFSET + intPos, value);
        intPos += SIZE_OF_BOOLEAN;
    }

    @Override
    public void putBoolean(final int position, final boolean value) {
        ensureCapacity(position + SIZE_OF_BOOLEAN);
        unsafe.putBoolean(buffer, (long) ARRAY_BYTE_BASE_OFFSET + position, value);
    }

    @Override
    public void putBooleanArray(final boolean[] values, final int n) {
        final int valuesSize = values == null ? 0 : values.length;
        final int nElements = n >= 0 ? Math.min(n, valuesSize) : valuesSize;
        ensureAdditionalCapacity(nElements);
        putInt(nElements); // strided-array size
        copyMemory(values, ARRAY_BOOLEAN_BASE_OFFSET, buffer, ARRAY_BYTE_BASE_OFFSET + intPos, nElements);
        intPos += nElements;
    }

    @Override
    public void putByte(final byte value) {
        ensureAdditionalCapacity(SIZE_OF_BYTE);
        unsafe.putByte(buffer, (long) ARRAY_BYTE_BASE_OFFSET + intPos, value);
        intPos += SIZE_OF_BYTE;
    }

    @Override
    public void putByte(final int position, final byte value) {
        ensureCapacity(position + SIZE_OF_BYTE);
        unsafe.putByte(buffer, (long) ARRAY_BYTE_BASE_OFFSET + position, value);
    }

    @Override
    public void putByteArray(final byte[] values, final int n) {
        final int valuesSize = values == null ? 0 : values.length;
        final int nElements = (n >= 0 ? Math.min(n, valuesSize) : valuesSize);
        ensureAdditionalCapacity(nElements + SIZE_OF_INT);
        putInt(nElements); // strided-array size
        copyMemory(values, ARRAY_BOOLEAN_BASE_OFFSET, buffer, ARRAY_BYTE_BASE_OFFSET + intPos, nElements);
        intPos += nElements;
    }

    @Override
    public void putChar(final char value) {
        ensureAdditionalCapacity(SIZE_OF_CHAR);
        unsafe.putChar(buffer, (long) ARRAY_BYTE_BASE_OFFSET + intPos, value);
        intPos += SIZE_OF_CHAR;
    }

    @Override
    public void putChar(final int position, final char value) {
        ensureCapacity(position + SIZE_OF_CHAR);
        unsafe.putChar(buffer, (long) ARRAY_BYTE_BASE_OFFSET + position, value);
    }

    @Override
    public void putCharArray(final char[] values, final int n) {
        final int arrayOffset = ARRAY_CHAR_BASE_OFFSET;
        final int valuesSize = values == null ? 0 : values.length;
        final int nElements = (n >= 0 ? Math.min(n, valuesSize) : valuesSize);
        final int bytesToCopy = nElements * SIZE_OF_CHAR;
        ensureAdditionalCapacity(bytesToCopy + SIZE_OF_INT);
        putInt(nElements); // strided-array size
        copyMemory(values, arrayOffset, buffer, arrayOffset + intPos, bytesToCopy);
        intPos += bytesToCopy;
    }

    @Override
    public void putDouble(final double value) {
        ensureAdditionalCapacity(SIZE_OF_DOUBLE);
        unsafe.putDouble(buffer, (long) ARRAY_BYTE_BASE_OFFSET + intPos, value);
        intPos += SIZE_OF_DOUBLE;
    }

    @Override
    public void putDouble(final int position, final double value) {
        ensureCapacity(position + SIZE_OF_DOUBLE);
        unsafe.putDouble(buffer, (long) ARRAY_BYTE_BASE_OFFSET + position, value);
    }

    @Override
    public void putDoubleArray(final double[] values, final int n) {
        final int arrayOffset = ARRAY_DOUBLE_BASE_OFFSET;
        final int valuesSize = values == null ? 0 : values.length;
        final int nElements = (n >= 0 ? Math.min(n, valuesSize) : valuesSize);
        final int bytesToCopy = nElements * SIZE_OF_DOUBLE;
        ensureAdditionalCapacity(bytesToCopy + SIZE_OF_INT);
        putInt(nElements); // strided-array size
        copyMemory(values, arrayOffset, buffer, arrayOffset + intPos, bytesToCopy);
        intPos += bytesToCopy;
    }

    @Override
    public void putFloat(final float value) {
        ensureAdditionalCapacity(SIZE_OF_FLOAT);
        unsafe.putFloat(buffer, (long) ARRAY_BYTE_BASE_OFFSET + intPos, value);
        intPos += SIZE_OF_FLOAT;
    }

    @Override
    public void putFloat(final int position, final float value) {
        ensureCapacity(position + SIZE_OF_FLOAT);
        unsafe.putFloat(buffer, (long) ARRAY_BYTE_BASE_OFFSET + position, value);
    }

    @Override
    public void putFloatArray(final float[] values, final int n) {
        final int arrayOffset = ARRAY_FLOAT_BASE_OFFSET;
        final int valuesSize = values == null ? 0 : values.length;
        final int nElements = (n >= 0 ? Math.min(n, valuesSize) : valuesSize);
        final int bytesToCopy = nElements * SIZE_OF_FLOAT;
        ensureAdditionalCapacity(bytesToCopy + SIZE_OF_INT);
        putInt(nElements); // strided-array size
        copyMemory(values, arrayOffset, buffer, arrayOffset + intPos, bytesToCopy);
        intPos += bytesToCopy;
    }

    @Override
    public void putInt(final int value) {
        ensureAdditionalCapacity(SIZE_OF_INT);
        unsafe.putInt(buffer, (long) ARRAY_BYTE_BASE_OFFSET + intPos, value);
        intPos += SIZE_OF_INT;
    }

    @Override
    public void putInt(final int position, final int value) {
        ensureCapacity(position + SIZE_OF_INT);
        unsafe.putInt(buffer, (long) ARRAY_BYTE_BASE_OFFSET + position, value);
    }

    @Override
    public void putIntArray(final int[] values, final int n) {
        final int arrayOffset = ARRAY_INT_BASE_OFFSET;
        final int valuesSize = values == null ? 0 : values.length;
        final int nElements = (n >= 0 ? Math.min(n, valuesSize) : valuesSize);
        final int bytesToCopy = nElements * SIZE_OF_INT;
        ensureAdditionalCapacity(bytesToCopy + SIZE_OF_INT);
        putInt(nElements); // strided-array size
        copyMemory(values, arrayOffset, buffer, arrayOffset + intPos, bytesToCopy);
        intPos += bytesToCopy;
    }

    @Override
    public void putLong(final long value) {
        ensureAdditionalCapacity(SIZE_OF_LONG);
        unsafe.putLong(buffer, (long) ARRAY_BYTE_BASE_OFFSET + intPos, value);
        intPos += SIZE_OF_LONG;
    }

    @Override
    public void putLong(final int position, final long value) {
        ensureCapacity(position + SIZE_OF_LONG);
        unsafe.putLong(buffer, (long) ARRAY_BYTE_BASE_OFFSET + position, value);
    }

    @Override
    public void putLongArray(final long[] values, final int n) {
        final int arrayOffset = ARRAY_LONG_BASE_OFFSET;
        final int valuesSize = values == null ? 0 : values.length;
        final int nElements = (n >= 0 ? Math.min(n, valuesSize) : valuesSize);
        final int bytesToCopy = nElements * SIZE_OF_LONG;
        ensureAdditionalCapacity(bytesToCopy + SIZE_OF_INT);
        putInt(nElements); // strided-array size
        copyMemory(values, arrayOffset, buffer, arrayOffset + intPos, bytesToCopy);
        intPos += bytesToCopy;
    }

    @Override
    public void putShort(final short value) { // NOPMD by rstein
        ensureAdditionalCapacity(SIZE_OF_SHORT);
        unsafe.putShort(buffer, (long) ARRAY_BYTE_BASE_OFFSET + intPos, value);
        intPos += SIZE_OF_SHORT;
    }

    @Override
    public void putShort(final int position, final short value) {
        ensureCapacity(position + SIZE_OF_SHORT);
        unsafe.putShort(buffer, (long) ARRAY_BYTE_BASE_OFFSET + position, value);
    }

    @Override
    public void putShortArray(final short[] values, final int n) { // NOPMD by rstein
        final int arrayOffset = ARRAY_SHORT_BASE_OFFSET;
        final int valuesSize = values == null ? 0 : values.length;
        final int nElements = (n >= 0 ? Math.min(n, valuesSize) : valuesSize);
        final int bytesToCopy = nElements * SIZE_OF_SHORT;
        ensureAdditionalCapacity(bytesToCopy + SIZE_OF_INT);
        putInt(nElements); // strided-array size
        copyMemory(values, arrayOffset, buffer, arrayOffset + intPos, bytesToCopy);
        intPos += bytesToCopy;
    }

    @Override
    public void putString(final String string) {
        if (string == null) {
            putString("");
            return;
        }
        if (isEnforceSimpleStringEncoding()) {
            putStringISO8859(string);
            return;
        }
        final int utf16StringLength = string.length();
        final int initialPos = intPos;
        intPos += SIZE_OF_INT;
        // write string-to-byte (in-place)
        ensureAdditionalCapacity(3 * utf16StringLength + SIZE_OF_INT);
        final int strLength = encodeUTF8(string, buffer, intPos, 3 * utf16StringLength);
        final int endPos = intPos + strLength;

        // write length of string byte representation
        putInt(initialPos, strLength + 1);
        intPos = endPos;

        putByte((byte) 0); // For C++ zero terminated string
    }

    @Override
    public void putString(final int position, final String value) {
        final int oldPosition = position();
        position(position);
        putString(value);
        position(oldPosition);
    }

    @Override
    public void putStringArray(final String[] values, final int n) {
        final int valuesSize = values == null ? 0 : values.length;
        final int nElements = n >= 0 ? Math.min(n, valuesSize) : valuesSize;
        final int originalPos = intPos; // NOPMD
        putInt(nElements); // strided-array size
        if (values == null) {
            return;
        }
        try {
            if (isEnforceSimpleStringEncoding()) {
                for (int k = 0; k < nElements; k++) {
                    putStringISO8859(values[k]);
                }
                return;
            }
            for (int k = 0; k < nElements; k++) {
                putString(values[k]);
            }
        } catch (IndexOutOfBoundsException e) {
            intPos = originalPos; // reset the position to the original position before any strings where added
            throw e; // rethrow the exception
        }
    }

    @Override
    public void putStringISO8859(final String string) {
        if (string == null) {
            putStringISO8859("");
            return;
        }
        final int initialPos = intPos;
        intPos += SIZE_OF_INT;
        // write string-to-byte (in-place)
        final int strLength = encodeISO8859(string, buffer, intPos, string.length());
        final int endPos = intPos + strLength;

        // write length of string byte representation
        intPos = initialPos;
        putInt(strLength + 1);
        intPos = endPos;

        putByte((byte) 0); // For C++ zero terminated string
    }

    @Override
    public int remaining() {
        return intLimit - intPos;
    }

    @Override
    public void reset() {
        intPos = 0;
        intLimit = buffer.length;
    }

    /**
     * @param autoResize {@code true} to automatically increase the size of the underlying buffer if necessary
     */
    public void setAutoResize(final boolean autoResize) {
        this.autoResize = autoResize;
    }

    public void setByteArrayCache(final ByteArrayCache byteArrayCache) {
        this.byteArrayCache = byteArrayCache;
    }

    @Override
    public void setEnforceSimpleStringEncoding(final boolean state) {
        this.enforceSimpleStringEncoding = state;
    }

    @Override
    public String toString() {
        return super.toString() + String.format(" - [0, position=%d, limit:%d, capacity:%d]", intPos, intLimit, capacity());
    }

    /**
     * Trims the internal buffer array so that the capacity is equal to the size.
     *
     * @see java.util.ArrayList#trimToSize()
     */
    @Override
    public void trim() {
        trim(position());
    }

    /**
     * Trims the internal buffer array if it is too large. If the current array length is smaller than or equal to
     * {@code requestedCapacity}, this method does nothing. Otherwise, it trims the array length to the maximum between
     * {@code requestedCapacity} and {@link #capacity()}.
     * <p>
     * This method is useful when reusing FastBuffers. {@linkplain #reset() Clearing a list} leaves the array length
     * untouched. If you are reusing a list many times, you can call this method with a typical size to avoid keeping
     * around a very large array just because of a few large transient lists.
     *
     * @param requestedCapacity the threshold for the trimming.
     */
    @Override
    public void trim(final int requestedCapacity) {
        if ((requestedCapacity >= capacity()) || (this.position() > requestedCapacity)) {
            return;
        }
        final int bytesToCopy = Math.min(Math.max(requestedCapacity, position()), capacity()) * SIZE_OF_BYTE;
        final byte[] newBuffer = byteArrayCache == null ? new byte[requestedCapacity] : byteArrayCache.getArrayExact(requestedCapacity);
        copyMemory(buffer, ARRAY_BYTE_BASE_OFFSET, newBuffer, ARRAY_BYTE_BASE_OFFSET, bytesToCopy);
        if (byteArrayCache != null) {
            byteArrayCache.add(buffer);
        }
        buffer = newBuffer;
        intLimit = newBuffer.length;
    }

    /**
     * Wraps a given byte array into FastByteBuffer
     * <p>
     * Note it is guaranteed that the type of the array returned by {@link #elements()} will be the same.
     *
     * @param byteArray an array to wrap.
     * @return a new FastByteBuffer of the given size, wrapping the given array.
     */
    public static FastByteBuffer wrap(final byte[] byteArray) {
        return wrap(byteArray, byteArray.length);
    }

    /**
     * Wraps a given byte array into FastByteBuffer
     * <p>
     * Note it is guaranteed that the type of the array returned by {@link #elements()} will be the same.
     *
     * @param byteArray an array to wrap.
     * @param length the length of the resulting array list.
     * @return a new FastByteBuffer of the given size, wrapping the given array.
     */
    public static FastByteBuffer wrap(final byte[] byteArray, final int length) {
        return new FastByteBuffer(byteArray, length);
    }

    private static void copyMemory(final Object srcBase, final int srcOffset, final Object destBase, final int destOffset, final int nBytes) {
        unsafe.copyMemory(srcBase, srcOffset, destBase, destOffset, nBytes);
    }

    // Fast UTF-8 byte-array to String(Builder) decode - code originally based on Google's ProtoBuffer implementation and since modified
    @SuppressWarnings("PMD")
    private static void decodeUTF8(byte[] bytes, int offset, int size, StringBuilder result) { //NOSONAR
        // Bitwise OR combines the sign bits so any negative value fails the check.
        // N.B. many code snippets are in-lined for performance reasons (~10% performance improvement) ... this is a JIT hot spot.
        if ((offset | size | bytes.length - offset - size) < 0) {
            throw new ArrayIndexOutOfBoundsException(String.format("buffer length=%d, offset=%d, size=%d", bytes.length, offset, size));
        }

        // The longest possible resulting String is the same as the number of input bytes, when it is
        // all ASCII. For other cases, this over-allocates and we will truncate in the end.
        result.setLength(size);

        // keep separate int/long counters so we don't have to convert types at every call
        int remaining = size;
        long readPos = (long) ARRAY_BYTE_BASE_OFFSET + offset;
        int resultPos = 0;

        // Optimize for 100% ASCII (Hotspot loves small simple top-level loops like this).
        // This simple loop stops when we encounter a byte >= 0x80 (i.e. non-ASCII).
        while (remaining > 0) {
            final byte byte1 = unsafe.getByte(bytes, readPos);
            if (byte1 < 0) {
                // is more than one byte ie. non-ASCII (unsigned byte value larger > 127 <-> negative number for signed byte
                break;
            }
            readPos++;
            remaining--;
            result.setCharAt(resultPos++, (char) byte1);
        }

        while (remaining > 0) {
            final byte byte1 = unsafe.getByte(bytes, readPos++);
            remaining--;
            if (byte1 >= 0) { // is one byte (ie. ASCII-type encoding)
                result.setCharAt(resultPos++, (char) byte1);
                // It's common for there to be multiple ASCII characters in a run mixed in, so add an extra optimized loop to take care of these runs.
                while (remaining > 0) {
                    final byte b = unsafe.getByte(bytes, readPos);
                    if (b < 0) { // is not one byte
                        break;
                    }
                    readPos++;
                    remaining--;
                    result.setCharAt(resultPos++, (char) b);
                }
            } else if (byte1 < (byte) 0xE0) { // is two bytes
                if (remaining < 1) {
                    throw new IllegalArgumentException(INVALID_UTF_8);
                }
                final byte byte2 = unsafe.getByte(bytes, readPos++);
                remaining--;
                final int resultPos1 = resultPos++;
                // Simultaneously checks for illegal trailing-byte in leading position (<= '11000000') and overlong 2-byte, '11000001'.
                if (byte1 < (byte) 0xC2) {
                    throw new IllegalArgumentException(INVALID_UTF_8 + ": Illegal leading byte in 2 bytes UTF");
                }
                if (byte2 > (byte) 0xBF) { // is not trailing byte
                    throw new IllegalArgumentException(INVALID_UTF_8 + ": Illegal trailing byte in 2 bytes UTF");
                }
                result.setCharAt(resultPos1, (char) (((byte1 & 0x1F) << 6) | byte2 & 0x3F));
            } else if (byte1 < (byte) 0xF0) { // is three bytes
                if (remaining < 2) {
                    throw new IllegalArgumentException(INVALID_UTF_8);
                }
                /* byte2 */
                /* byte3 */
                final byte byte2 = unsafe.getByte(bytes, readPos++);
                final byte byte3 = unsafe.getByte(bytes, readPos++);
                final int resultPos1 = resultPos++;
                if (byte2 > (byte) 0xBF // is not trailing byte
                        // overlong? 5 most significant bits must not all be zero
                        || (byte1 == (byte) 0xE0 && byte2 < (byte) 0xA0)
                        // check for illegal surrogate codepoints
                        || (byte1 == (byte) 0xED && byte2 >= (byte) 0xA0)
                        || byte3 > (byte) 0xBF) { // is not trailing byte
                    throw new IllegalArgumentException(INVALID_UTF_8);
                }
                result.setCharAt(resultPos1, (char) (((byte1 & 0x0F) << 12) | ((byte2 & 0x3F) << 6) | byte3 & 0x3F));
                remaining -= 2;
            } else { // is four bytes
                if (remaining < 3) {
                    throw new IllegalArgumentException(INVALID_UTF_8);
                }
                // handle four byte UTF
                /* byte2 */
                /* byte3 */
                /* byte4 */
                final byte byte2 = unsafe.getByte(bytes, readPos++);
                final byte byte3 = unsafe.getByte(bytes, readPos++);
                final byte byte4 = unsafe.getByte(bytes, readPos++);
                final int resultPos1 = resultPos++;
                if (byte2 > (byte) 0xBF
                        // Check that 1 <= plane <= 16.  Tricky optimized form of:
                        //   valid 4-byte leading byte?
                        // if (byte1 > (byte) 0xF4 ||
                        //   overlong? 4 most significant bits must not all be zero
                        //     byte1 == (byte) 0xF0 && byte2 < (byte) 0x90 ||
                        //   codepoint larger than the highest code point (U+10FFFF)? byte1 == (byte) 0xF4 && byte2 > (byte) 0x8F)
                        || (((byte1 << 28) + (byte2 - (byte) 0x90)) >> 30) != 0
                        || byte3 > (byte) 0xBF
                        || byte4 > (byte) 0xBF) {
                    throw new IllegalArgumentException(INVALID_UTF_8);
                }
                final int codepoint = ((byte1 & 0x07) << 18) | ((byte2 & 0x3F) << 12) | ((byte3 & 0x3F) << 6) | byte4 & 0x3F;
                result.setCharAt(resultPos1, (char) ((Character.MIN_HIGH_SURROGATE - (Character.MIN_SUPPLEMENTARY_CODE_POINT >>> 10)) + (codepoint >>> 10)));
                result.setCharAt(resultPos1 + 1, (char) (Character.MIN_LOW_SURROGATE + (codepoint & 0x3ff)));
                remaining -= 3;
                // 4-byte case requires two chars.
                resultPos++;
            }
        }

        result.setLength(resultPos);
    }

    private static int encodeISO8859(final String sequence, final byte[] bytes, final int offset, final int length) {
        // encode to ISO_8859_1
        final int base = ARRAY_BYTE_BASE_OFFSET + offset;
        for (int i = 0; i < length; i++) {
            unsafe.putByte(bytes, (long) base + i, (byte) (sequence.charAt(i) & 0xFF));
        }
        return length;
    }

    // Fast UTF-8 String (CharSequence) to byte-array encoder - code originally based on Google's ProtoBuffer implementation and since modified
    @SuppressWarnings("PMD")
    private static int encodeUTF8(final CharSequence sequence, final byte[] bytes, final int offset, final int length) { //NOSONAR
        int utf16Length = sequence.length();
        int base = ARRAY_BYTE_BASE_OFFSET + offset;
        int i = 0;
        int limit = base + length;
        // Designed to take advantage of https://wiki.openjdk.java.net/display/HotSpot/RangeCheckElimination
        for (char c; i < utf16Length && base + i < limit && (c = sequence.charAt(i)) < 0x80; i++) {
            unsafe.putByte(bytes, (long) base + i, (byte) c);
        }
        if (i == utf16Length) {
            return utf16Length;
        }
        base += i;
        for (; i < utf16Length; i++) {
            final char c = sequence.charAt(i);
            if (c < 0x80 && base < limit) {
                unsafe.putByte(bytes, base++, (byte) c);
            } else if (c < 0x800 && base <= limit - 2) { // 11 bits, two UTF-8 bytes
                unsafe.putByte(bytes, base++, (byte) ((0xF << 6) | (c >>> 6)));
                unsafe.putByte(bytes, base++, (byte) (0x80 | (0x3F & c)));
            } else if ((c < Character.MIN_SURROGATE || Character.MAX_SURROGATE < c) && base <= limit - 3) {
                // Maximum single-char code point is 0xFFFF, 16 bits, three UTF-8 bytes
                unsafe.putByte(bytes, base++, (byte) ((0xF << 5) | (c >>> 12)));
                unsafe.putByte(bytes, base++, (byte) (0x80 | (0x3F & (c >>> 6))));
                unsafe.putByte(bytes, base++, (byte) (0x80 | (0x3F & c)));
            } else if (base <= limit - 4) {
                // Minimum code point represented by a surrogate pair is 0x10000, 17 bits, four UTF-8 bytes
                final char low;
                if (i + 1 == sequence.length() || !Character.isSurrogatePair(c, (low = sequence.charAt(++i)))) {
                    throw new IllegalArgumentException("Unpaired surrogate at index " + (i - 1));
                }
                final int codePoint = Character.toCodePoint(c, low);
                unsafe.putByte(bytes, base++, (byte) ((0xF << 4) | (codePoint >>> 18)));
                unsafe.putByte(bytes, base++, (byte) (0x80 | (0x3F & (codePoint >>> 12))));
                unsafe.putByte(bytes, base++, (byte) (0x80 | (0x3F & (codePoint >>> 6))));
                unsafe.putByte(bytes, base++, (byte) (0x80 | (0x3F & codePoint)));
            } else {
                throw new ArrayIndexOutOfBoundsException("Failed writing " + c + " at index " + base);
            }
        }
        return base - ARRAY_BYTE_BASE_OFFSET - offset;
    }
}
