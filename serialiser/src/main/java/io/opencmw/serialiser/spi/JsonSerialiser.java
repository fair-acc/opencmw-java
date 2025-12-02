package io.opencmw.serialiser.spi;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.BiFunction;

import io.opencmw.serialiser.DataType;
import io.opencmw.serialiser.FieldDescription;
import io.opencmw.serialiser.FieldSerialiser;
import io.opencmw.serialiser.IoBuffer;
import io.opencmw.serialiser.IoSerialiser;

import com.jsoniter.JsonIterator;
import com.jsoniter.JsonIteratorPool;
import com.jsoniter.any.Any;
import com.jsoniter.extra.PreciseFloatSupport;
import com.jsoniter.output.EncodingMode;
import com.jsoniter.output.JsonStream;
import com.jsoniter.spi.DecodingMode;
import com.jsoniter.spi.JsonException;

import de.gsi.dataset.utils.ByteBufferOutputStream;

@SuppressWarnings({ "PMD.ExcessivePublicCount", "PMD.TooManyMethods" }) // unavoidable: Java does not support templates and primitive types need to be handled one-by-one
public class JsonSerialiser implements IoSerialiser {
    public static final String NOT_A_JSON_COMPATIBLE_PROTOCOL = "Not a JSON compatible protocol";
    public static final String JSON_ROOT = "JSON_ROOT";
    private static final int DEFAULT_INITIAL_CAPACITY = 10_000;
    private static final int DEFAULT_INDENTATION = 2;
    private static final char BRACKET_OPEN = '{';
    private static final char BRACKET_CLOSE = '}';
    public static final char QUOTE = '\"';
    private static final String NULL = "null";
    private static final String ASSIGN = ": ";
    private static final String LINE_BREAK = System.lineSeparator();
    public static final String UNCHECKED = "unchecked";
    private final StringBuilder builder = new StringBuilder(DEFAULT_INITIAL_CAPACITY); // NOPMD
    private IoBuffer buffer;
    private Any root;
    private Any tempRoot;
    private WireDataFieldDescription parent;
    private WireDataFieldDescription lastFieldHeader;
    private String queryFieldName;
    private boolean hasFieldBefore;
    private String indentation = "";
    private BiFunction<Type, Type[], FieldSerialiser<Object>> fieldSerialiserLookupFunction;

    /**
     * @param buffer the backing IoBuffer (see e.g. {@link FastByteBuffer} or{@link ByteBuffer}
     */
    public JsonSerialiser(final IoBuffer buffer) {
        super();
        this.buffer = buffer;

        // JsonStream.setIndentionStep(DEFAULT_INDENTATION)
        JsonStream.setMode(EncodingMode.REFLECTION_MODE); // enable as a fall-back
        JsonIterator.setMode(DecodingMode.REFLECTION_MODE); // enable as a fall-back
        // JsonStream.setMode(EncodingMode.DYNAMIC_MODE);
        // JsonIterator.setMode(DecodingMode.DYNAMIC_MODE_AND_MATCH_FIELD_WITH_HASH);

        try {
            PreciseFloatSupport.enable();
        } catch (JsonException e) {
            // swallow subsequent enabling exceptions (function is guarded and supposed to be called only once)
        }
    }

    @Override
    public ProtocolInfo checkHeaderInfo() {
        // make coarse check (ie. check if first non-null character is a '{' bracket
        int count = buffer.position();
        while (buffer.getByte(count) != BRACKET_OPEN && (buffer.getByte(count) == 0 || buffer.getByte(count) == ' ' || buffer.getByte(count) == '\t' || buffer.getByte(count) == '\n')) {
            count++;
        }
        if (buffer.getByte(count) != BRACKET_OPEN) {
            throw new IllegalStateException(NOT_A_JSON_COMPATIBLE_PROTOCOL);
        }

        try (JsonIterator iter = JsonIteratorPool.borrowJsonIterator()) {
            iter.reset(buffer.elements(), 0, buffer.limit());
            tempRoot = root = iter.readAny();
        } catch (IOException e) {
            throw new IllegalStateException(NOT_A_JSON_COMPATIBLE_PROTOCOL, e);
        }

        final WireDataFieldDescription headerStartField = new WireDataFieldDescription(this, null, JSON_ROOT, DataType.OTHER, buffer.position(), count - 1, -1);
        final ProtocolInfo header = new ProtocolInfo(this, headerStartField, JsonSerialiser.class.getCanonicalName(), (byte) 1, (byte) 0, (byte) 0);
        parent = lastFieldHeader = headerStartField;
        queryFieldName = JSON_ROOT;
        return header;
    }

    public <T> T deserialiseObject(final T obj) {
        try (JsonIterator iter = JsonIterator.parse(buffer.elements(), 0, buffer.limit())) {
            return iter.read(obj);
        } catch (IOException | JsonException e) {
            throw new IllegalStateException(NOT_A_JSON_COMPATIBLE_PROTOCOL, e);
        }
    }

    @Override
    public int[] getArraySizeDescriptor() {
        return new int[0];
    }

    @Override
    public boolean getBoolean() {
        return tempRoot.get(queryFieldName).toBoolean();
    }

    @Override
    public boolean[] getBooleanArray(final boolean[] dst, final int length) {
        return tempRoot.get(queryFieldName).as(boolean[].class);
    }

    @Override
    public IoBuffer getBuffer() {
        return buffer;
    }

    @Override
    public byte getByte() {
        return (byte) tempRoot.get(queryFieldName).toInt();
    }

    @Override
    public byte[] getByteArray(final byte[] dst, final int length) {
        return tempRoot.get(queryFieldName).as(byte[].class);
    }

    @Override
    public char getChar() {
        return (char) tempRoot.get(queryFieldName).toInt();
    }

    @Override
    public char[] getCharArray(final char[] dst, final int length) {
        return tempRoot.get(queryFieldName).as(char[].class);
    }

    @Override
    @SuppressWarnings(UNCHECKED)
    public <E> Collection<E> getCollection(final Collection<E> collection) {
        return tempRoot.get(queryFieldName).as(ArrayList.class);
    }

    @Override
    @SuppressWarnings(UNCHECKED)
    public <E> E getCustomData(final FieldSerialiser<E> serialiser) {
        return (E) tempRoot.get(queryFieldName);
    }

    @Override
    public double getDouble() {
        return tempRoot.get(queryFieldName).toDouble();
    }

    @Override
    public double[] getDoubleArray(final double[] dst, final int length) {
        return tempRoot.get(queryFieldName).as(double[].class);
    }

    @Override
    public <E extends Enum<E>> Enum<E> getEnum(final Enum<E> enumeration) {
        return null;
    }

    @Override
    public String getEnumTypeList() {
        return null;
    }

    @Override
    public WireDataFieldDescription getFieldHeader() {
        return null;
    }

    @Override
    public float getFloat() {
        return tempRoot.get(queryFieldName).toFloat();
    }

    @Override
    public float[] getFloatArray(final float[] dst, final int length) {
        return tempRoot.get(queryFieldName).as(float[].class);
    }

    @Override
    public int getInt() {
        return tempRoot.get(queryFieldName).toInt();
    }

    @Override
    public int[] getIntArray(final int[] dst, final int length) {
        return tempRoot.get(queryFieldName).as(int[].class);
    }

    @Override
    @SuppressWarnings(UNCHECKED)
    public <E> List<E> getList(final List<E> collection) {
        return tempRoot.get(queryFieldName).as(List.class);
    }

    @Override
    public long getLong() {
        return tempRoot.get(queryFieldName).toLong();
    }

    @Override
    public long[] getLongArray(final long[] dst, final int length) {
        return tempRoot.get(queryFieldName).as(long[].class);
    }

    @Override
    public <K, V> Map<K, V> getMap(final Map<K, V> map) {
        return null;
    }

    public WireDataFieldDescription getParent() {
        return parent;
    }

    @Override
    @SuppressWarnings(UNCHECKED)
    public <E> Queue<E> getQueue(final Queue<E> collection) {
        return tempRoot.get(queryFieldName).as(ArrayDeque.class);
    }

    @Override
    @SuppressWarnings(UNCHECKED)
    public <E> Set<E> getSet(final Set<E> collection) {
        return tempRoot.get(queryFieldName).as(HashSet.class);
    }

    @Override
    public short getShort() {
        return (short) tempRoot.get(queryFieldName).toLong();
    }

    @Override
    public short[] getShortArray(final short[] dst, final int length) {
        return tempRoot.get(queryFieldName).as(short[].class);
    }

    @Override
    public String getString() {
        return tempRoot.get(queryFieldName).toString();
    }

    @Override
    public String[] getStringArray(final String[] dst, final int length) {
        return tempRoot.get(queryFieldName).as(String[].class);
    }

    @Override
    public String getStringISO8859() {
        return tempRoot.get(queryFieldName).toString();
    }

    @Override
    public boolean isPutFieldMetaData() {
        return false;
    }

    @Override
    public WireDataFieldDescription parseIoStream(final boolean readHeader) {
        try (JsonIterator iter = JsonIteratorPool.borrowJsonIterator()) {
            iter.reset(buffer.elements(), 0, buffer.limit());
            tempRoot = root = iter.readAny();

            final WireDataFieldDescription fieldRoot = new WireDataFieldDescription(this, null, "ROOT", DataType.OTHER, buffer.position(), -1, -1);
            parseIoStream(fieldRoot, tempRoot, "");

            return fieldRoot;
        } catch (IOException e) {
            throw new IllegalStateException(NOT_A_JSON_COMPATIBLE_PROTOCOL, e);
        }
    }

    @Override
    public <E> void put(final FieldDescription fieldDescription, final Collection<E> collection, final Type valueType) {
        put(fieldDescription.getFieldName(), collection, valueType);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final Enum<?> enumeration) {
        put(fieldDescription.getFieldName(), enumeration);
    }

    @Override
    public <K, V> void put(final FieldDescription fieldDescription, final Map<K, V> map, final Type keyType, final Type valueType) {
        put(fieldDescription.getFieldName(), map, keyType, valueType);
    }

    @Override
    public <E> void put(final String fieldName, final Collection<E> collection, final Type valueType) {
        lineBreak();
        builder.append(QUOTE).append(fieldName).append(QUOTE).append(ASSIGN).append('[');
        if (collection == null || collection.isEmpty()) {
            builder.append(']');
            return;
        }
        final Iterator<E> iter = collection.iterator();
        serialiseObject(iter.next());
        while (iter.hasNext()) {
            builder.append(", ");
            serialiseObject(iter.next());
        }
        builder.append(']');
        hasFieldBefore = true;
    }

    @Override
    public void put(final String fieldName, final Enum<?> enumeration) {
        lineBreak();
        builder.append(QUOTE).append(fieldName).append(QUOTE + ASSIGN + QUOTE).append(enumeration).append(QUOTE);
        hasFieldBefore = true;
    }

    @Override
    public <K, V> void put(final String fieldName, final Map<K, V> map, final Type keyType, final Type valueType) {
        lineBreak();
        builder.append(QUOTE).append(fieldName).append(QUOTE + ASSIGN + '{');
        if (map == null || map.isEmpty()) {
            builder.append('}');
            return;
        }
        final Set<Map.Entry<K, V>> entrySet = map.entrySet();
        boolean isFirst = true;
        for (Map.Entry<K, V> entry : entrySet) {
            final V value = entry.getValue();
            if (isFirst) {
                isFirst = false;
            } else {
                builder.append(", ");
            }
            builder.append(QUOTE).append(entry.getKey()).append(QUOTE + ASSIGN);

            switch (DataType.fromClassType(value.getClass())) {
            case CHAR:
                builder.append((int) value);
                break;
            case STRING:
                builder.append(QUOTE).append(entry.getValue()).append(QUOTE);
                break;
            default:
                builder.append(value);
                break;
            }
        }

        builder.append('}');
        hasFieldBefore = true;
    }

    @Override
    public void put(final FieldDescription fieldDescription, final boolean value) {
        put(fieldDescription.getFieldName(), value);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final boolean[] values, final int n) {
        put(fieldDescription.getFieldName(), values, n);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final boolean[] values, final int[] dims) {
        put(fieldDescription.getFieldName(), values, dims);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final byte value) {
        put(fieldDescription.getFieldName(), value);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final byte[] values, final int n) {
        put(fieldDescription.getFieldName(), values, n);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final byte[] values, final int[] dims) {
        put(fieldDescription.getFieldName(), values, dims);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final char value) {
        put(fieldDescription.getFieldName(), value);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final char[] values, final int n) {
        put(fieldDescription.getFieldName(), values, n);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final char[] values, final int[] dims) {
        put(fieldDescription.getFieldName(), values, dims);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final double value) {
        put(fieldDescription.getFieldName(), value);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final double[] values, final int n) {
        put(fieldDescription.getFieldName(), values, n);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final double[] values, final int[] dims) {
        put(fieldDescription.getFieldName(), values, dims);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final float value) {
        put(fieldDescription.getFieldName(), value);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final float[] values, final int n) {
        put(fieldDescription.getFieldName(), values, n);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final float[] values, final int[] dims) {
        put(fieldDescription.getFieldName(), values, dims);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final int value) {
        put(fieldDescription.getFieldName(), value);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final int[] values, final int n) {
        put(fieldDescription.getFieldName(), values, n);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final int[] values, final int[] dims) {
        put(fieldDescription.getFieldName(), values, dims);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final long value) {
        put(fieldDescription.getFieldName(), value);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final long[] values, final int n) {
        put(fieldDescription.getFieldName(), values, n);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final long[] values, final int[] dims) {
        put(fieldDescription.getFieldName(), values, dims);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final short value) {
        put(fieldDescription.getFieldName(), value);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final short[] values, final int n) {
        put(fieldDescription.getFieldName(), values, n);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final short[] values, final int[] dims) {
        put(fieldDescription.getFieldName(), values, dims);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final String string) {
        put(fieldDescription.getFieldName(), string);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final String[] values, final int n) {
        put(fieldDescription.getFieldName(), values, n);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final String[] values, final int[] dims) {
        put(fieldDescription.getFieldName(), values, dims);
    }

    @Override
    public void put(final String fieldName, final boolean value) {
        lineBreak();
        builder.append(QUOTE).append(fieldName).append(QUOTE + ASSIGN).append(value);
        hasFieldBefore = true;
    }

    @Override
    public void put(final String fieldName, final boolean[] values, final int n) {
        lineBreak();
        builder.append(QUOTE).append(fieldName).append(QUOTE + ASSIGN + '[');
        if (values == null || values.length <= 0) {
            builder.append(']');
            return;
        }
        builder.append(values[0]);
        final int valuesSize = values.length;
        final int nElements = n >= 0 ? Math.min(n, valuesSize) : valuesSize;
        for (int i = 1; i < nElements; i++) {
            builder.append(", ").append(values[i]);
        }
        builder.append(']');
        hasFieldBefore = true;
    }

    @Override
    public void put(final String fieldName, final boolean[] values, final int[] dims) {
        put(fieldName, values, getNumberElements(dims));
    }

    @Override
    public void put(final String fieldName, final byte value) {
        lineBreak();
        builder.append(QUOTE).append(fieldName).append(QUOTE + ASSIGN).append(value);
        hasFieldBefore = true;
    }

    @Override
    public void put(final String fieldName, final byte[] values, final int n) {
        lineBreak();
        builder.append(QUOTE).append(fieldName).append(QUOTE + ASSIGN + '[');
        if (values == null || values.length <= 0) {
            builder.append(']');
            return;
        }
        builder.append(values[0]);
        final int valuesSize = values.length;
        final int nElements = n >= 0 ? Math.min(n, valuesSize) : valuesSize;
        for (int i = 1; i < nElements; i++) {
            builder.append(", ").append(values[i]);
        }
        builder.append(']');
        hasFieldBefore = true;
    }

    @Override
    public void put(final String fieldName, final byte[] values, final int[] dims) {
        put(fieldName, values, getNumberElements(dims));
    }

    @Override
    public void put(final String fieldName, final char value) {
        lineBreak();
        builder.append(QUOTE).append(fieldName).append(QUOTE + ASSIGN).append((int) value);
        hasFieldBefore = true;
    }

    @Override
    public void put(final String fieldName, final char[] values, final int n) {
        lineBreak();
        builder.append(QUOTE).append(fieldName).append(QUOTE + ASSIGN + '[');
        if (values == null || values.length <= 0) {
            builder.append(']');
            return;
        }
        builder.append((int) values[0]);
        final int valuesSize = values.length;
        final int nElements = n >= 0 ? Math.min(n, valuesSize) : valuesSize;
        for (int i = 1; i < nElements; i++) {
            builder.append(", ").append((int) values[i]);
        }
        builder.append(']');
        hasFieldBefore = true;
    }

    @Override
    public void put(final String fieldName, final char[] values, final int[] dims) {
        put(fieldName, values, getNumberElements(dims));
    }

    @Override
    public void put(final String fieldName, final double value) {
        lineBreak();
        builder.append(QUOTE).append(fieldName).append(QUOTE + ASSIGN).append(value);
        hasFieldBefore = true;
    }

    @Override
    public void put(final String fieldName, final double[] values, final int n) {
        lineBreak();
        builder.append(QUOTE).append(fieldName).append(QUOTE + ASSIGN + '[');
        if (values == null || values.length <= 0) {
            builder.append(']');
            return;
        }
        builder.append(values[0]);
        final int valuesSize = values.length;
        final int nElements = n >= 0 ? Math.min(n, valuesSize) : valuesSize;
        for (int i = 1; i < nElements; i++) {
            builder.append(", ").append(values[i]);
        }
        builder.append(']');
        hasFieldBefore = true;
    }

    @Override
    public void put(final String fieldName, final double[] values, final int[] dims) {
        put(fieldName, values, getNumberElements(dims));
    }

    @Override
    public void put(final String fieldName, final float value) {
        lineBreak();
        builder.append(QUOTE).append(fieldName).append(QUOTE + ASSIGN).append(value);
        hasFieldBefore = true;
    }

    @Override
    public void put(final String fieldName, final float[] values, final int n) {
        lineBreak();
        builder.append(QUOTE).append(fieldName).append(QUOTE).append(ASSIGN).append('[');
        if (values == null || values.length <= 0) {
            builder.append(']');
            return;
        }
        builder.append(values[0]);
        final int valuesSize = values.length;
        final int nElements = n >= 0 ? Math.min(n, valuesSize) : valuesSize;
        for (int i = 1; i < nElements; i++) {
            builder.append(", ").append(values[i]);
        }
        builder.append(']');
        hasFieldBefore = true;
    }

    @Override
    public void put(final String fieldName, final float[] values, final int[] dims) {
        put(fieldName, values, getNumberElements(dims));
    }

    @Override
    public void put(final String fieldName, final int value) {
        lineBreak();
        builder.append(QUOTE).append(fieldName).append(QUOTE + ASSIGN).append(value);
        hasFieldBefore = true;
    }

    @Override
    public void put(final String fieldName, final int[] values, final int n) {
        lineBreak();
        builder.append(QUOTE).append(fieldName).append(QUOTE + ASSIGN).append("[");
        if (values == null || values.length <= 0) {
            builder.append(']');
            return;
        }
        builder.append(values[0]);
        final int valuesSize = values.length;
        final int nElements = n >= 0 ? Math.min(n, valuesSize) : valuesSize;
        for (int i = 1; i < nElements; i++) {
            builder.append(", ").append(values[i]);
        }
        builder.append(']');
        hasFieldBefore = true;
    }

    @Override
    public void put(final String fieldName, final int[] values, final int[] dims) {
        put(fieldName, values, getNumberElements(dims));
    }

    @Override
    public void put(final String fieldName, final long value) {
        lineBreak();
        builder.append(QUOTE).append(fieldName).append(QUOTE + ASSIGN).append(value);
        hasFieldBefore = true;
    }

    @Override
    public void put(final String fieldName, final long[] values, final int n) {
        lineBreak();
        builder.append(QUOTE).append(fieldName).append(QUOTE + ASSIGN).append("[");
        if (values == null || values.length <= 0) {
            builder.append(']');
            return;
        }
        builder.append(values[0]);
        final int valuesSize = values.length;
        final int nElements = n >= 0 ? Math.min(n, valuesSize) : valuesSize;
        for (int i = 1; i < nElements; i++) {
            builder.append(", ").append(values[i]);
        }
        builder.append(']');
        hasFieldBefore = true;
    }

    @Override
    public void put(final String fieldName, final long[] values, final int[] dims) {
        put(fieldName, values, getNumberElements(dims));
    }

    @Override
    public void put(final String fieldName, final short value) {
        lineBreak();
        builder.append(QUOTE).append(fieldName).append(QUOTE + ASSIGN).append(value);
        hasFieldBefore = true;
    }

    @Override
    public void put(final String fieldName, final short[] values, final int n) {
        lineBreak();
        builder.append(QUOTE).append(fieldName).append(QUOTE + ASSIGN).append("[");
        if (values == null || values.length <= 0) {
            builder.append(']');
            return;
        }
        builder.append(values[0]);
        final int valuesSize = values.length;
        final int nElements = n >= 0 ? Math.min(n, valuesSize) : valuesSize;
        for (int i = 1; i < nElements; i++) {
            builder.append(", ").append(values[i]);
        }
        builder.append(']');
        hasFieldBefore = true;
    }

    @Override
    public void put(final String fieldName, final short[] values, final int[] dims) {
        put(fieldName, values, getNumberElements(dims));
    }

    @Override
    public void put(final String fieldName, final String string) {
        lineBreak();
        builder.append(QUOTE).append(fieldName).append("\": \"").append(string).append(QUOTE);
        hasFieldBefore = true;
    }

    @Override
    public void put(final String fieldName, final String[] values, final int n) {
        lineBreak();
        builder.append(QUOTE).append(fieldName).append(QUOTE + ASSIGN).append("[");
        if (values == null || values.length <= 0) {
            builder.append(']');
            return;
        }
        builder.append(QUOTE).append(values[0]).append(QUOTE);
        final int valuesSize = values.length;
        final int nElements = n >= 0 ? Math.min(n, valuesSize) : valuesSize;
        for (int i = 1; i < nElements; i++) {
            builder.append(", \"").append(values[i]).append(QUOTE);
        }
        builder.append(']');
        hasFieldBefore = true;
    }

    @Override
    public void put(final String fieldName, final String[] values, final int[] dims) {
        put(fieldName, values, getNumberElements(dims));
    }

    @Override
    public int putArraySizeDescriptor(final int n) {
        return 0;
    }

    @Override
    public int putArraySizeDescriptor(final int[] dims) {
        return 0;
    }

    @Override
    public <E> WireDataFieldDescription putCustomData(final FieldDescription fieldDescription, final E obj, final Class<? extends E> type, final FieldSerialiser<E> serialiser) {
        return null;
    }

    @Override
    public void putEndMarker(final FieldDescription fieldDescription) {
        indentation = indentation.substring(0, Math.max(indentation.length() - DEFAULT_INDENTATION, 0));
        builder.append(LINE_BREAK).append(indentation).append(BRACKET_CLOSE).append(LINE_BREAK);
        hasFieldBefore = true;
        final byte[] outputStrBytes = builder.toString().getBytes(StandardCharsets.UTF_8);
        buffer.ensureAdditionalCapacity(outputStrBytes.length);
        System.arraycopy(outputStrBytes, 0, buffer.elements(), buffer.position(), outputStrBytes.length);
        buffer.position(buffer.position() + outputStrBytes.length);
        builder.setLength(0);
    }

    @Override
    public WireDataFieldDescription putFieldHeader(final FieldDescription fieldDescription) {
        return putFieldHeader(fieldDescription.getFieldName(), fieldDescription.getDataType());
    }

    @Override
    public WireDataFieldDescription putFieldHeader(final String fieldName, final DataType dataType) {
        lastFieldHeader = new WireDataFieldDescription(this, parent, fieldName, dataType, -1, 1, -1);
        queryFieldName = fieldName;
        return lastFieldHeader;
    }

    @Override
    public void putHeaderInfo(final FieldDescription... field) {
        if (!builder.isEmpty()) {
            final byte[] outputStrBytes = builder.toString().getBytes(StandardCharsets.UTF_8);
            buffer.ensureAdditionalCapacity(outputStrBytes.length);
            System.arraycopy(outputStrBytes, 0, buffer.elements(), buffer.position(), outputStrBytes.length);
            buffer.position(buffer.position() + outputStrBytes.length);
        } else {
            hasFieldBefore = false;
            indentation = "";
        }
        builder.setLength(0);
        putStartMarker(null);
    }

    @Override
    public void putStartMarker(final FieldDescription fieldDescription) {
        lineBreak();
        if (fieldDescription != null) {
            builder.append(QUOTE).append(fieldDescription.getFieldName()).append(QUOTE + ASSIGN);
        }
        builder.append(BRACKET_OPEN);
        indentation = indentation + " ".repeat(DEFAULT_INDENTATION);
        builder.append(LINE_BREAK);
        builder.append(indentation);
        hasFieldBefore = false;
    }

    public void serialiseObject(final Object obj) {
        if (!builder.isEmpty()) {
            final byte[] outputStrBytes = builder.toString().getBytes(StandardCharsets.UTF_8);
            buffer.ensureAdditionalCapacity(outputStrBytes.length);
            System.arraycopy(outputStrBytes, 0, buffer.elements(), buffer.position(), outputStrBytes.length);
            buffer.position(buffer.position() + outputStrBytes.length);
            builder.setLength(0);
        } else {
            hasFieldBefore = false;
            indentation = "";
        }
        if (obj == null) {
            // serialise null object
            builder.append(NULL);
            byte[] bytes = builder.toString().getBytes(Charset.defaultCharset());
            System.arraycopy(bytes, 0, buffer.elements(), buffer.position(), bytes.length);
            buffer.position(buffer.position() + bytes.length);
            builder.setLength(0);
            return;
        }
        try (ByteBufferOutputStream byteOutputStream = new ByteBufferOutputStream(java.nio.ByteBuffer.wrap(buffer.elements()), false)) {
            byteOutputStream.position(buffer.position());
            JsonStream.serialize(obj, byteOutputStream);
            buffer.position(byteOutputStream.position());
        } catch (IOException e) {
            throw new IllegalStateException(NOT_A_JSON_COMPATIBLE_PROTOCOL, e);
        }
    }

    @Override
    public void setBuffer(final IoBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public void setPutFieldMetaData(final boolean putFieldMetaData) {
        // json does not support metadata
    }

    @Override
    public void setQueryFieldName(final String fieldName, final int dataStartPosition) {
        if (fieldName == null || fieldName.isBlank()) {
            throw new IllegalArgumentException("fieldName must not be null or blank: " + fieldName);
        }
        if (root == null) {
            throw new IllegalArgumentException("JSON Any root hasn't been analysed/parsed yet");
        }
        this.queryFieldName = fieldName;
        // buffer.position(dataStartPosition); // N.B. not needed at this time
    }

    @Override
    public void updateDataEndMarker(final WireDataFieldDescription fieldHeader) {
        // not needed
    }

    private int getNumberElements(final int[] dims) {
        int n = 1;
        for (final int dim : dims) {
            n *= dim;
        }
        return n;
    }

    private void lineBreak() {
        if (hasFieldBefore) {
            builder.append(',');
            builder.append(LINE_BREAK);
            builder.append(indentation);
        }
    }

    private void parseIoStream(final WireDataFieldDescription fieldRoot, final Any any, final String fieldName) {
        if (!(any.object() instanceof Map) || any.size() == 0) {
            return;
        }

        final Map<String, Any> map = any.asMap();
        final WireDataFieldDescription putStartMarker = new WireDataFieldDescription(this, fieldRoot, fieldName, DataType.START_MARKER, 0, -1, -1);
        for (Map.Entry<String, Any> child : map.entrySet()) {
            final String childName = child.getKey();
            final Any childAny = map.get(childName);
            final Object data = childAny.object();
            if (data instanceof Map) {
                parseIoStream(putStartMarker, childAny, childName);
            } else if (data != null) {
                new WireDataFieldDescription(this, putStartMarker, childName, DataType.fromClassType(data.getClass()), 0, -1, -1); // NOPMD - necessary to allocate inside loop
            }
        }
        // add if necessary:
        // new WireDataFieldDescription(this, fieldRoot, fieldName.hashCode(), fieldName, DataType.END_MARKER, 0, -1, -1)
    }

    @Override
    public void setFieldSerialiserLookupFunction(final BiFunction<Type, Type[], FieldSerialiser<Object>> serialiserLookupFunction) {
        this.fieldSerialiserLookupFunction = serialiserLookupFunction;
    }

    @Override
    public BiFunction<Type, Type[], FieldSerialiser<Object>> getSerialiserLookupFunction() {
        return fieldSerialiserLookupFunction;
    }
}
