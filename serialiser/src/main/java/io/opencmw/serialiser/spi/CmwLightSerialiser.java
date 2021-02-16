package io.opencmw.serialiser.spi;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.function.BiFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opencmw.serialiser.DataType;
import io.opencmw.serialiser.FieldDescription;
import io.opencmw.serialiser.FieldSerialiser;
import io.opencmw.serialiser.IoBuffer;
import io.opencmw.serialiser.IoSerialiser;
import io.opencmw.serialiser.utils.ClassUtils;

/**
 * Light-weight open-source implementation of a (de-)serialiser that is binary-compatible to the serialiser used by CMW,
 * a proprietary closed-source middle-ware used in some accelerator laboratories.
 *
 * N.B. this implementation is intended only for performance/functionality comparison and to enable a backward compatible
 * transition to the {@link BinarySerialiser} implementation which is a bit more flexible,
 * has some additional (optional) features, and a better IO performance. See the corresponding benchmarks for details;
 *
 * @author rstein
 */
@SuppressWarnings({ "PMD.ExcessiveClassLength", "PMD.ExcessivePublicCount", "PMD.TooManyMethods" })
public class CmwLightSerialiser implements IoSerialiser {
    public static final String NOT_IMPLEMENTED = "not implemented";
    private static final Logger LOGGER = LoggerFactory.getLogger(CmwLightSerialiser.class);
    private static final int ADDITIONAL_HEADER_INFO_SIZE = 1000;
    private static final DataType[] BYTE_TO_DATA_TYPE = new DataType[256];
    private static final Byte[] DATA_TYPE_TO_BYTE = new Byte[256];

    static {
        // static mapping of protocol bytes -- needed to be compatible with other wire protocols
        // N.B. CmwLightSerialiser does not implement mappings for:
        // * discreteFunction
        // * discreteFunction_list
        // * array of Data objects (N.B. 'Data' and nested 'Data' is being explicitely supported)
        // 'Data' object is mapped to START_MARKER also used for nested data structures

        BYTE_TO_DATA_TYPE[0] = DataType.BOOL;
        BYTE_TO_DATA_TYPE[1] = DataType.BYTE;
        BYTE_TO_DATA_TYPE[2] = DataType.SHORT;
        BYTE_TO_DATA_TYPE[3] = DataType.INT;
        BYTE_TO_DATA_TYPE[4] = DataType.LONG;
        BYTE_TO_DATA_TYPE[5] = DataType.FLOAT;
        BYTE_TO_DATA_TYPE[6] = DataType.DOUBLE;
        BYTE_TO_DATA_TYPE[201] = DataType.CHAR; // not actually implemented by CMW
        BYTE_TO_DATA_TYPE[7] = DataType.STRING;
        BYTE_TO_DATA_TYPE[8] = DataType.START_MARKER; // mapped to CMW 'Data' type

        // needs to be defined last
        BYTE_TO_DATA_TYPE[9] = DataType.BOOL_ARRAY;
        BYTE_TO_DATA_TYPE[10] = DataType.BYTE_ARRAY;
        BYTE_TO_DATA_TYPE[11] = DataType.SHORT_ARRAY;
        BYTE_TO_DATA_TYPE[12] = DataType.INT_ARRAY;
        BYTE_TO_DATA_TYPE[13] = DataType.LONG_ARRAY;
        BYTE_TO_DATA_TYPE[14] = DataType.FLOAT_ARRAY;
        BYTE_TO_DATA_TYPE[15] = DataType.DOUBLE_ARRAY;
        BYTE_TO_DATA_TYPE[202] = DataType.CHAR_ARRAY; // not actually implemented by CMW
        BYTE_TO_DATA_TYPE[16] = DataType.STRING_ARRAY;

        // CMW 2D arrays -- also mapped internally to byte arrays
        BYTE_TO_DATA_TYPE[17] = DataType.BOOL_ARRAY;
        BYTE_TO_DATA_TYPE[18] = DataType.BYTE_ARRAY;
        BYTE_TO_DATA_TYPE[19] = DataType.SHORT_ARRAY;
        BYTE_TO_DATA_TYPE[20] = DataType.INT_ARRAY;
        BYTE_TO_DATA_TYPE[21] = DataType.LONG_ARRAY;
        BYTE_TO_DATA_TYPE[22] = DataType.FLOAT_ARRAY;
        BYTE_TO_DATA_TYPE[23] = DataType.DOUBLE_ARRAY;
        BYTE_TO_DATA_TYPE[203] = DataType.CHAR_ARRAY; // not actually implemented by CMW
        BYTE_TO_DATA_TYPE[24] = DataType.STRING_ARRAY;

        // CMW multi-dim arrays -- also mapped internally to byte arrays
        BYTE_TO_DATA_TYPE[25] = DataType.BOOL_ARRAY;
        BYTE_TO_DATA_TYPE[26] = DataType.BYTE_ARRAY;
        BYTE_TO_DATA_TYPE[27] = DataType.SHORT_ARRAY;
        BYTE_TO_DATA_TYPE[28] = DataType.INT_ARRAY;
        BYTE_TO_DATA_TYPE[29] = DataType.LONG_ARRAY;
        BYTE_TO_DATA_TYPE[30] = DataType.FLOAT_ARRAY;
        BYTE_TO_DATA_TYPE[31] = DataType.DOUBLE_ARRAY;
        BYTE_TO_DATA_TYPE[204] = DataType.CHAR_ARRAY; // not actually implemented by CMW
        BYTE_TO_DATA_TYPE[32] = DataType.STRING_ARRAY;

        for (int i = BYTE_TO_DATA_TYPE.length - 1; i >= 0; i--) {
            if (BYTE_TO_DATA_TYPE[i] == null) {
                continue;
            }
            final int id = BYTE_TO_DATA_TYPE[i].getID();
            DATA_TYPE_TO_BYTE[id] = (byte) i;
        }
    }

    private IoBuffer buffer;
    private WireDataFieldDescription parent;
    private WireDataFieldDescription lastFieldHeader;
    private BiFunction<Type, Type[], FieldSerialiser<Object>> fieldSerialiserLookupFunction;

    public CmwLightSerialiser(final IoBuffer buffer) {
        super();
        this.buffer = buffer;
    }

    @Override
    public ProtocolInfo checkHeaderInfo() {
        final String fieldName = "";
        final int dataSize = FastByteBuffer.SIZE_OF_INT;
        final WireDataFieldDescription headerStartField = new WireDataFieldDescription(this, parent, fieldName.hashCode(), fieldName, DataType.START_MARKER, buffer.position(), buffer.position(), dataSize); // NOPMD - needs to be read here
        final int nEntries = buffer.getInt();
        if (nEntries <= 0) {
            throw new IllegalStateException("nEntries = " + nEntries + " <= 0!");
        }
        parent = lastFieldHeader = headerStartField;
        return new ProtocolInfo(this, headerStartField, CmwLightSerialiser.class.getCanonicalName(), (byte) 1, (byte) 0, (byte) 1);
    }

    @Override
    public int[] getArraySizeDescriptor() {
        final int nDims = buffer.getInt(); // number of dimensions
        final int[] ret = new int[nDims];
        for (int i = 0; i < nDims; i++) {
            ret[i] = buffer.getInt(); // vector size for each dimension
        }
        return ret;
    }

    @Override
    public boolean getBoolean() {
        return buffer.getBoolean();
    }

    @Override
    public boolean[] getBooleanArray(final boolean[] dst, final int length) {
        getArraySizeDescriptor();
        return buffer.getBooleanArray(dst, length);
    }

    @Override
    public IoBuffer getBuffer() {
        return buffer;
    }

    @Override
    public void setBuffer(final IoBuffer buffer) {
        this.buffer = buffer;
    }

    public int getBufferIncrements() {
        return ADDITIONAL_HEADER_INFO_SIZE;
    }

    @Override
    public byte getByte() {
        return buffer.getByte();
    }

    @Override
    public byte[] getByteArray(final byte[] dst, final int length) {
        getArraySizeDescriptor();
        return buffer.getByteArray(dst, length);
    }

    @Override
    public char getChar() {
        return buffer.getChar();
    }

    @Override
    public char[] getCharArray(final char[] dst, final int length) {
        getArraySizeDescriptor();
        return buffer.getCharArray(dst, length);
    }

    @Override
    public <E> Collection<E> getCollection(final Collection<E> collection) {
        throw new UnsupportedOperationException(NOT_IMPLEMENTED);
    }

    @Override
    public <E> E getCustomData(final FieldSerialiser<E> serialiser) {
        throw new UnsupportedOperationException(NOT_IMPLEMENTED);
    }

    @Override
    public double getDouble() {
        return buffer.getDouble();
    }

    @Override
    public double[] getDoubleArray(final double[] dst, final int length) {
        getArraySizeDescriptor();
        return buffer.getDoubleArray(dst, length);
    }

    @Override
    public <E extends Enum<E>> Enum<E> getEnum(final Enum<E> enumeration) {
        final int ordinal = buffer.getInt();
        assert ordinal >= 0 : "enum ordinal should be positive";

        final String enumName = enumeration.getClass().getName();
        Class<?> enumClass = ClassUtils.getClassByName(enumName);
        if (enumClass == null) {
            final String enumSimpleName = enumeration.getClass().getSimpleName();
            enumClass = ClassUtils.getClassByName(enumSimpleName);
            if (enumClass == null) {
                throw new IllegalStateException("could not find enum class description '" + enumName + "' or '" + enumSimpleName + "'");
            }
        }

        try {
            final Method values = enumClass.getMethod("values");
            final Object[] possibleEnumValues = (Object[]) values.invoke(null);
            //noinspection unchecked
            return (Enum<E>) possibleEnumValues[ordinal]; // NOSONAR NOPMD
        } catch (final ReflectiveOperationException e) {
            LOGGER.atError().setCause(e).addArgument(enumClass).log("could not match 'valueOf(String)' function for class/(supposedly) enum of {}");
        }
        throw new UnsupportedOperationException(NOT_IMPLEMENTED);
    }

    @Override
    public String getEnumTypeList() {
        throw new UnsupportedOperationException(NOT_IMPLEMENTED);
    }

    @Override
    public WireDataFieldDescription getFieldHeader() {
        // process CMW-like wire-format
        final int headerStart = buffer.position(); // NOPMD - need to read the present buffer position

        final String fieldName = buffer.getStringISO8859(); // NOPMD - read advances position
        final byte dataTypeByte = buffer.getByte();
        final DataType dataType = getDataType(dataTypeByte);
        // process CMW-like wire-format - done

        final int dataStartOffset = buffer.position() - headerStart; // NOPMD - further reads advance the read position in the buffer
        final int dataSize;
        if (dataType == DataType.START_MARKER) {
            dataSize = FastByteBuffer.SIZE_OF_INT;
        } else if (dataType.isScalar()) {
            dataSize = dataType.getPrimitiveSize();
        } else if (dataType == DataType.STRING) {
            dataSize = FastByteBuffer.SIZE_OF_INT + buffer.getInt(); // <(>string size -1> + <string byte data>
        } else if (dataType.isArray() && dataType != DataType.STRING_ARRAY) {
            // read array descriptor
            final int[] dims = getArraySizeDescriptor();
            final int arraySize = buffer.getInt(); // strided array size
            dataSize = FastByteBuffer.SIZE_OF_INT * (dims.length + 2) + arraySize * dataType.getPrimitiveSize(); // <array description> + <nElments * primitive size>
        } else if (dataType == DataType.STRING_ARRAY) {
            // read array descriptor -- this case has a high-penalty since the size of all Strings needs to be read
            final int[] dims = getArraySizeDescriptor();
            final int arraySize = buffer.getInt(); // strided array size
            // String parsing, need to follow every single element
            int totalSize = FastByteBuffer.SIZE_OF_INT * arraySize;
            for (int i = 0; i < arraySize; i++) {
                final int stringSize = buffer.getInt(); // <(>string size -1> + <string byte data>
                totalSize += stringSize;
                buffer.position(buffer.position() + stringSize);
            }
            dataSize = FastByteBuffer.SIZE_OF_INT * (dims.length + 2) + totalSize;
        } else {
            throw new IllegalStateException("should not reach here -- format is incompatible with CMW");
        }

        final int fieldNameHashCode = fieldName.hashCode(); //TODO: verify same hashcode function

        lastFieldHeader = new WireDataFieldDescription(this, parent, fieldNameHashCode, fieldName, dataType, headerStart, dataStartOffset, dataSize);
        final int dataStartPosition = headerStart + dataStartOffset;
        buffer.position(dataStartPosition);

        if (dataType == DataType.START_MARKER) {
            parent = lastFieldHeader;
            buffer.position(dataStartPosition);
            buffer.position(dataStartPosition + dataSize);
        }

        if (dataSize < 0) {
            throw new IllegalStateException("should not reach here -- format is incompatible with CMW");
        }
        return lastFieldHeader;
    }

    @Override
    public float getFloat() {
        return buffer.getFloat();
    }

    @Override
    public float[] getFloatArray(final float[] dst, final int length) {
        getArraySizeDescriptor();
        return buffer.getFloatArray(dst, length);
    }

    @Override
    public int getInt() {
        return buffer.getInt();
    }

    @Override
    public int[] getIntArray(final int[] dst, final int length) {
        getArraySizeDescriptor();
        return buffer.getIntArray(dst, length);
    }

    @Override
    public <E> List<E> getList(final List<E> collection) {
        throw new UnsupportedOperationException(NOT_IMPLEMENTED);
    }

    @Override
    public long getLong() {
        return buffer.getLong();
    }

    @Override
    public long[] getLongArray(final long[] dst, final int length) {
        getArraySizeDescriptor();
        return buffer.getLongArray(dst, length);
    }

    @Override
    public <K, V> Map<K, V> getMap(final Map<K, V> map) {
        throw new UnsupportedOperationException(NOT_IMPLEMENTED);
    }

    public WireDataFieldDescription getParent() {
        return parent;
    }

    @Override
    public <E> Queue<E> getQueue(final Queue<E> collection) {
        throw new UnsupportedOperationException(NOT_IMPLEMENTED);
    }

    @Override
    public <E> Set<E> getSet(final Set<E> collection) {
        throw new UnsupportedOperationException(NOT_IMPLEMENTED);
    }

    @Override
    public short getShort() {
        return buffer.getShort();
    }

    @Override
    public short[] getShortArray(final short[] dst, final int length) {
        getArraySizeDescriptor();
        return buffer.getShortArray(dst, length);
    }

    @Override
    public String getString() {
        return buffer.getString();
    }

    @Override
    public String[] getStringArray(final String[] dst, final int length) {
        getArraySizeDescriptor();
        return buffer.getStringArray(dst, length);
    }

    @Override
    public String getStringISO8859() {
        return buffer.getStringISO8859();
    }

    /**
     * @return {@code true} the ISO-8859-1 character encoding is being enforced for data fields (better performance), otherwise UTF-8 is being used (more generic encoding)
     */
    public boolean isEnforceSimpleStringEncoding() {
        return buffer.isEnforceSimpleStringEncoding();
    }

    /**
     *
     * @param state {@code true} the ISO-8859-1 character encoding is being enforced for data fields (better performance), otherwise UTF-8 is being used (more generic encoding)
     */
    public void setEnforceSimpleStringEncoding(final boolean state) {
        buffer.setEnforceSimpleStringEncoding(state);
    }

    @Override
    public boolean isPutFieldMetaData() {
        return false;
    }

    @Override
    public void setPutFieldMetaData(final boolean putFieldMetaData) {
        // do nothing -- not implemented for this serialiser
    }

    @Override
    public WireDataFieldDescription parseIoStream(final boolean readHeader) {
        final WireDataFieldDescription fieldRoot = getRootElement();
        parent = fieldRoot;
        final WireDataFieldDescription headerRoot = readHeader ? checkHeaderInfo().getFieldHeader() : getFieldHeader();
        buffer.position(headerRoot.getDataStartPosition() + headerRoot.getDataSize());
        parseIoStream(headerRoot, 0);
        return fieldRoot;
    }

    public void parseIoStream(final WireDataFieldDescription fieldRoot, final int recursionDepth) {
        if (fieldRoot == null || fieldRoot.getDataType() != DataType.START_MARKER) {
            throw new IllegalStateException("fieldRoot not a START_MARKER but: " + fieldRoot);
        }
        buffer.position(fieldRoot.getDataStartPosition());
        final int nEntries = buffer.getInt();
        if (nEntries < 0) {
            throw new IllegalStateException("nEntries = " + nEntries + " < 0!");
        }
        parent = lastFieldHeader = fieldRoot;
        for (int i = 0; i < nEntries; i++) {
            final WireDataFieldDescription field = getFieldHeader(); // NOPMD - need to read the present buffer position
            final int dataSize = field.getDataSize();
            final int skipPosition = field.getDataStartPosition() + dataSize; // NOPMD - read at this location necessary, further reads advance position pointer

            if (field.getDataType() == DataType.START_MARKER) {
                // detected sub-class start marker
                parent = lastFieldHeader = field;
                parseIoStream(field, recursionDepth + 1);
                parent = lastFieldHeader = fieldRoot;
                continue;
            }

            if (dataSize < 0) {
                LOGGER.atWarn().addArgument(field.getFieldName()).addArgument(field.getDataType()).addArgument(dataSize).log("WireDataFieldDescription for '{}' type '{}' has bytesToSkip '{} <= 0'");
                // fall-back option in case of undefined dataSetSize -- usually indicated an internal serialiser error
                throw new IllegalStateException();
            }

            if (skipPosition < buffer.capacity()) {
                buffer.position(skipPosition);
            } else {
                // reached end of buffer
                if (skipPosition == buffer.capacity()) {
                    return;
                }
                throw new IllegalStateException("reached beyond end of buffer at " + skipPosition + " vs. capacity" + buffer.capacity() + " " + field);
            }
        }
    }

    @Override
    public <E> void put(final FieldDescription fieldDescription, final Collection<E> collection, final Type valueType) {
        throw new UnsupportedOperationException(NOT_IMPLEMENTED);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final Enum<?> enumeration) {
        this.putFieldHeader(fieldDescription, DataType.INT);
        buffer.putInt(enumeration.ordinal());
    }

    @Override
    public <K, V> void put(final FieldDescription fieldDescription, final Map<K, V> map, Type keyType, Type valueType) {
        throw new UnsupportedOperationException(NOT_IMPLEMENTED);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final boolean value) {
        this.putFieldHeader(fieldDescription, DataType.BOOL);
        buffer.putBoolean(value);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final boolean[] values, final int n) {
        final WireDataFieldDescription fieldHeader = putFieldHeader(fieldDescription, DataType.BOOL_ARRAY);
        final int valuesSize = values == null ? 0 : values.length;
        final int bytesToCopy = putArraySizeDescriptor(n >= 0 ? Math.min(n, valuesSize) : valuesSize);
        buffer.putBooleanArray(values, bytesToCopy);
        updateDataEndMarker(fieldHeader);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final boolean[] values, final int[] dims) {
        final WireDataFieldDescription fieldHeader = putFieldHeader(fieldDescription, DataType.BOOL_ARRAY);
        final int bytesToCopy = putArraySizeDescriptor(dims);
        buffer.putBooleanArray(values, bytesToCopy);
        updateDataEndMarker(fieldHeader);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final byte value) {
        this.putFieldHeader(fieldDescription, DataType.BYTE);
        buffer.putByte(value);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final byte[] values, final int n) {
        final WireDataFieldDescription fieldHeader = putFieldHeader(fieldDescription, DataType.BYTE_ARRAY);
        final int valuesSize = values == null ? 0 : values.length;
        final int bytesToCopy = putArraySizeDescriptor(n >= 0 ? Math.min(n, valuesSize) : valuesSize);
        buffer.putByteArray(values, bytesToCopy);
        updateDataEndMarker(fieldHeader);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final byte[] values, final int[] dims) {
        final WireDataFieldDescription fieldHeader = putFieldHeader(fieldDescription, DataType.BYTE_ARRAY);
        final int bytesToCopy = putArraySizeDescriptor(dims);
        buffer.putByteArray(values, bytesToCopy);
        updateDataEndMarker(fieldHeader);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final char value) {
        this.putFieldHeader(fieldDescription, DataType.CHAR);
        buffer.putChar(value);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final char[] values, final int n) {
        final WireDataFieldDescription fieldHeader = putFieldHeader(fieldDescription, DataType.CHAR_ARRAY);
        final int valuesSize = values == null ? 0 : values.length;
        final int bytesToCopy = putArraySizeDescriptor(n >= 0 ? Math.min(n, valuesSize) : valuesSize);
        buffer.putCharArray(values, bytesToCopy);
        updateDataEndMarker(fieldHeader);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final char[] values, final int[] dims) {
        final WireDataFieldDescription fieldHeader = putFieldHeader(fieldDescription, DataType.CHAR_ARRAY);
        final int bytesToCopy = putArraySizeDescriptor(dims);
        buffer.putCharArray(values, bytesToCopy);
        updateDataEndMarker(fieldHeader);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final double value) {
        this.putFieldHeader(fieldDescription, DataType.DOUBLE);
        buffer.putDouble(value);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final double[] values, final int n) {
        final WireDataFieldDescription fieldHeader = putFieldHeader(fieldDescription, DataType.DOUBLE_ARRAY);
        final int valuesSize = values == null ? 0 : values.length;
        final int bytesToCopy = putArraySizeDescriptor(n >= 0 ? Math.min(n, valuesSize) : valuesSize);
        buffer.putDoubleArray(values, bytesToCopy);
        updateDataEndMarker(fieldHeader);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final double[] values, final int[] dims) {
        final WireDataFieldDescription fieldHeader = putFieldHeader(fieldDescription, DataType.DOUBLE_ARRAY);
        final int bytesToCopy = putArraySizeDescriptor(dims);
        buffer.putDoubleArray(values, bytesToCopy);
        updateDataEndMarker(fieldHeader);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final float value) {
        this.putFieldHeader(fieldDescription, DataType.FLOAT);
        buffer.putFloat(value);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final float[] values, final int n) {
        final WireDataFieldDescription fieldHeader = putFieldHeader(fieldDescription, DataType.FLOAT_ARRAY);
        final int valuesSize = values == null ? 0 : values.length;
        final int bytesToCopy = putArraySizeDescriptor(n >= 0 ? Math.min(n, valuesSize) : valuesSize);
        buffer.putFloatArray(values, bytesToCopy);
        updateDataEndMarker(fieldHeader);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final float[] values, final int[] dims) {
        final WireDataFieldDescription fieldHeader = putFieldHeader(fieldDescription, DataType.FLOAT_ARRAY);
        final int bytesToCopy = putArraySizeDescriptor(dims);
        buffer.putFloatArray(values, bytesToCopy);
        updateDataEndMarker(fieldHeader);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final int value) {
        this.putFieldHeader(fieldDescription, DataType.INT);
        buffer.putInt(value);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final int[] values, final int n) {
        final WireDataFieldDescription fieldHeader = putFieldHeader(fieldDescription, DataType.INT_ARRAY);
        final int valuesSize = values == null ? 0 : values.length;
        final int bytesToCopy = putArraySizeDescriptor(n >= 0 ? Math.min(n, valuesSize) : valuesSize);
        buffer.putIntArray(values, bytesToCopy);
        updateDataEndMarker(fieldHeader);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final int[] values, final int[] dims) {
        final WireDataFieldDescription fieldHeader = putFieldHeader(fieldDescription, DataType.INT_ARRAY);
        final int bytesToCopy = putArraySizeDescriptor(dims);
        buffer.putIntArray(values, bytesToCopy);
        updateDataEndMarker(fieldHeader);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final long value) {
        this.putFieldHeader(fieldDescription, DataType.LONG);
        buffer.putLong(value);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final long[] values, final int n) {
        final WireDataFieldDescription fieldHeader = putFieldHeader(fieldDescription, DataType.LONG_ARRAY);
        final int valuesSize = values == null ? 0 : values.length;
        final int bytesToCopy = putArraySizeDescriptor(n >= 0 ? Math.min(n, valuesSize) : valuesSize);
        buffer.putLongArray(values, bytesToCopy);
        updateDataEndMarker(fieldHeader);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final long[] values, final int[] dims) {
        final WireDataFieldDescription fieldHeader = putFieldHeader(fieldDescription, DataType.LONG_ARRAY);
        final int bytesToCopy = putArraySizeDescriptor(dims);
        buffer.putLongArray(values, bytesToCopy);
        updateDataEndMarker(fieldHeader);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final short value) { // NOPMD by rstein
        this.putFieldHeader(fieldDescription, DataType.SHORT);
        buffer.putShort(value);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final short[] values, final int n) { // NOPMD by rstein
        final WireDataFieldDescription fieldHeader = putFieldHeader(fieldDescription, DataType.SHORT_ARRAY);
        final int valuesSize = values == null ? 0 : values.length;
        final int bytesToCopy = putArraySizeDescriptor(n >= 0 ? Math.min(n, valuesSize) : valuesSize);
        buffer.putShortArray(values, bytesToCopy);
        updateDataEndMarker(fieldHeader);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final short[] values, final int[] dims) { // NOPMD by rstein
        final WireDataFieldDescription fieldHeader = putFieldHeader(fieldDescription, DataType.SHORT_ARRAY);
        final int bytesToCopy = putArraySizeDescriptor(dims);
        buffer.putShortArray(values, bytesToCopy);
        updateDataEndMarker(fieldHeader);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final String string) {
        final WireDataFieldDescription fieldHeader = putFieldHeader(fieldDescription, DataType.STRING);
        buffer.putString(string);
        updateDataEndMarker(fieldHeader);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final String[] values, final int n) {
        final WireDataFieldDescription fieldHeader = putFieldHeader(fieldDescription, DataType.STRING_ARRAY);
        final int valuesSize = values == null ? 0 : values.length;
        final int nElements = n >= 0 ? Math.min(n, valuesSize) : valuesSize;
        putArraySizeDescriptor(nElements);
        buffer.putStringArray(values, nElements);
        updateDataEndMarker(fieldHeader);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final String[] values, final int[] dims) {
        final WireDataFieldDescription fieldHeader = putFieldHeader(fieldDescription, DataType.STRING_ARRAY);
        final int nElements = putArraySizeDescriptor(dims);
        putArraySizeDescriptor(nElements);
        buffer.putStringArray(values, nElements);
        updateDataEndMarker(fieldHeader);
    }

    @Override
    public void put(final String fieldName, final boolean value) {
        this.putFieldHeader(fieldName, DataType.BOOL);
        buffer.putBoolean(value);
    }

    @Override
    public void put(final String fieldName, final boolean[] values, final int n) {
        final WireDataFieldDescription fieldHeader = putFieldHeader(fieldName, DataType.BOOL_ARRAY);
        final int valuesSize = values == null ? 0 : values.length;
        final int bytesToCopy = putArraySizeDescriptor(n >= 0 ? Math.min(n, valuesSize) : valuesSize);
        buffer.putBooleanArray(values, bytesToCopy);
        updateDataEndMarker(fieldHeader);
    }

    @Override
    public void put(final String fieldName, final boolean[] values, final int[] dims) {
        final WireDataFieldDescription fieldHeader = putFieldHeader(fieldName, DataType.BOOL_ARRAY);
        if (dims.length == 2) {
            buffer.putByte(fieldHeader.getDataStartPosition() - 1, (byte) 17);
        } else if (dims.length > 2) {
            buffer.putByte(fieldHeader.getDataStartPosition() - 1, (byte) 25);
        }
        final int bytesToCopy = putArraySizeDescriptor(dims);
        buffer.putBooleanArray(values, bytesToCopy);
        updateDataEndMarker(fieldHeader);
    }

    @Override
    public void put(final String fieldName, final byte value) {
        this.putFieldHeader(fieldName, DataType.BYTE);
        buffer.putByte(value);
    }

    @Override
    public void put(final String fieldName, final byte[] values, final int n) {
        final WireDataFieldDescription fieldHeader = putFieldHeader(fieldName, DataType.BYTE_ARRAY);
        final int valuesSize = values == null ? 0 : values.length;
        final int bytesToCopy = putArraySizeDescriptor(n >= 0 ? Math.min(n, valuesSize) : valuesSize);
        buffer.putByteArray(values, bytesToCopy);
        updateDataEndMarker(fieldHeader);
    }

    @Override
    public void put(final String fieldName, final byte[] values, final int[] dims) {
        final WireDataFieldDescription fieldHeader = putFieldHeader(fieldName, DataType.BYTE_ARRAY);
        if (dims.length == 2) {
            buffer.putByte(fieldHeader.getDataStartPosition() - 1, (byte) 18);
        } else if (dims.length > 2) {
            buffer.putByte(fieldHeader.getDataStartPosition() - 1, (byte) 26);
        }
        final int bytesToCopy = putArraySizeDescriptor(dims);
        buffer.putByteArray(values, bytesToCopy);
        updateDataEndMarker(fieldHeader);
    }

    @Override
    public void put(final String fieldName, final char value) {
        this.putFieldHeader(fieldName, DataType.CHAR);
        buffer.putChar(value);
    }

    @Override
    public void put(final String fieldName, final char[] values, final int n) {
        final WireDataFieldDescription fieldHeader = putFieldHeader(fieldName, DataType.CHAR_ARRAY);
        final int valuesSize = values == null ? 0 : values.length;
        final int bytesToCopy = putArraySizeDescriptor(n >= 0 ? Math.min(n, valuesSize) : valuesSize);
        buffer.putCharArray(values, bytesToCopy);
        updateDataEndMarker(fieldHeader);
    }

    @Override
    public void put(final String fieldName, final char[] values, final int[] dims) {
        final WireDataFieldDescription fieldHeader = putFieldHeader(fieldName, DataType.CHAR_ARRAY);
        if (dims.length == 2) {
            buffer.putByte(fieldHeader.getDataStartPosition() - 1, (byte) 203);
        } else if (dims.length > 2) {
            buffer.putByte(fieldHeader.getDataStartPosition() - 1, (byte) 204);
        }
        final int bytesToCopy = putArraySizeDescriptor(dims);
        buffer.putCharArray(values, bytesToCopy);
        updateDataEndMarker(fieldHeader);
    }

    @Override
    public void put(final String fieldName, final double value) {
        this.putFieldHeader(fieldName, DataType.DOUBLE);
        buffer.putDouble(value);
    }

    @Override
    public void put(final String fieldName, final double[] values, final int n) {
        final WireDataFieldDescription fieldHeader = putFieldHeader(fieldName, DataType.DOUBLE_ARRAY);
        final int valuesSize = values == null ? 0 : values.length;
        final int bytesToCopy = putArraySizeDescriptor(n >= 0 ? Math.min(n, valuesSize) : valuesSize);
        buffer.putDoubleArray(values, bytesToCopy);
        updateDataEndMarker(fieldHeader);
    }

    @Override
    public void put(final String fieldName, final double[] values, final int[] dims) {
        final WireDataFieldDescription fieldHeader = putFieldHeader(fieldName, DataType.DOUBLE_ARRAY);
        if (dims.length == 2) {
            buffer.putByte(fieldHeader.getDataStartPosition() - 1, (byte) 23);
        } else if (dims.length > 2) {
            buffer.putByte(fieldHeader.getDataStartPosition() - 1, (byte) 31);
        }
        final int bytesToCopy = putArraySizeDescriptor(dims);
        buffer.putDoubleArray(values, bytesToCopy);
        updateDataEndMarker(fieldHeader);
    }

    @Override
    public void put(final String fieldName, final float value) {
        this.putFieldHeader(fieldName, DataType.FLOAT);
        buffer.putFloat(value);
    }

    @Override
    public void put(final String fieldName, final float[] values, final int n) {
        final WireDataFieldDescription fieldHeader = putFieldHeader(fieldName, DataType.FLOAT_ARRAY);
        final int valuesSize = values == null ? 0 : values.length;
        final int bytesToCopy = putArraySizeDescriptor(n >= 0 ? Math.min(n, valuesSize) : valuesSize);
        buffer.putFloatArray(values, bytesToCopy);
        updateDataEndMarker(fieldHeader);
    }

    @Override
    public void put(final String fieldName, final float[] values, final int[] dims) {
        final WireDataFieldDescription fieldHeader = putFieldHeader(fieldName, DataType.FLOAT_ARRAY);
        if (dims.length == 2) {
            buffer.putByte(fieldHeader.getDataStartPosition() - 1, (byte) 22);
        } else if (dims.length > 2) {
            buffer.putByte(fieldHeader.getDataStartPosition() - 1, (byte) 30);
        }
        final int bytesToCopy = putArraySizeDescriptor(dims);
        buffer.putFloatArray(values, bytesToCopy);
        updateDataEndMarker(fieldHeader);
    }

    @Override
    public void put(final String fieldName, final int value) {
        this.putFieldHeader(fieldName, DataType.INT);
        buffer.putInt(value);
    }

    @Override
    public void put(final String fieldName, final int[] values, final int n) {
        final WireDataFieldDescription fieldHeader = putFieldHeader(fieldName, DataType.INT_ARRAY);
        final int valuesSize = values == null ? 0 : values.length;
        final int bytesToCopy = putArraySizeDescriptor(n >= 0 ? Math.min(n, valuesSize) : valuesSize);
        buffer.putIntArray(values, bytesToCopy);
        updateDataEndMarker(fieldHeader);
    }

    @Override
    public void put(final String fieldName, final int[] values, final int[] dims) {
        final WireDataFieldDescription fieldHeader = putFieldHeader(fieldName, DataType.INT_ARRAY);
        if (dims.length == 2) {
            buffer.putByte(fieldHeader.getDataStartPosition() - 1, (byte) 20);
        } else if (dims.length > 2) {
            buffer.putByte(fieldHeader.getDataStartPosition() - 1, (byte) 28);
        }
        final int bytesToCopy = putArraySizeDescriptor(dims);
        buffer.putIntArray(values, bytesToCopy);
        updateDataEndMarker(fieldHeader);
    }

    @Override
    public void put(final String fieldName, final long value) {
        this.putFieldHeader(fieldName, DataType.LONG);
        buffer.putLong(value);
    }

    @Override
    public void put(final String fieldName, final long[] values, final int n) {
        final WireDataFieldDescription fieldHeader = putFieldHeader(fieldName, DataType.LONG_ARRAY);
        final int valuesSize = values == null ? 0 : values.length;
        final int bytesToCopy = putArraySizeDescriptor(n >= 0 ? Math.min(n, valuesSize) : valuesSize);
        buffer.putLongArray(values, bytesToCopy);
        updateDataEndMarker(fieldHeader);
    }

    @Override
    public void put(final String fieldName, final long[] values, final int[] dims) {
        final WireDataFieldDescription fieldHeader = putFieldHeader(fieldName, DataType.LONG_ARRAY);
        if (dims.length == 2) {
            buffer.putByte(fieldHeader.getDataStartPosition() - 1, (byte) 21);
        } else if (dims.length > 2) {
            buffer.putByte(fieldHeader.getDataStartPosition() - 1, (byte) 29);
        }
        final int bytesToCopy = putArraySizeDescriptor(dims);
        buffer.putLongArray(values, bytesToCopy);
        updateDataEndMarker(fieldHeader);
    }

    @Override
    public void put(final String fieldName, final short value) { // NOPMD by rstein
        this.putFieldHeader(fieldName, DataType.SHORT);
        buffer.putShort(value);
    }

    @Override
    public void put(final String fieldName, final short[] values, final int n) { // NOPMD by rstein
        final WireDataFieldDescription fieldHeader = putFieldHeader(fieldName, DataType.SHORT_ARRAY);
        final int valuesSize = values == null ? 0 : values.length;
        final int bytesToCopy = putArraySizeDescriptor(n >= 0 ? Math.min(n, valuesSize) : valuesSize);
        buffer.putShortArray(values, bytesToCopy);
        updateDataEndMarker(fieldHeader);
    }

    @Override
    public void put(final String fieldName, final short[] values, final int[] dims) { // NOPMD by rstein
        final WireDataFieldDescription fieldHeader = putFieldHeader(fieldName, DataType.SHORT_ARRAY);
        if (dims.length == 2) {
            buffer.putByte(fieldHeader.getDataStartPosition() - 1, (byte) 19);
        } else if (dims.length > 2) {
            buffer.putByte(fieldHeader.getDataStartPosition() - 1, (byte) 27);
        }
        final int bytesToCopy = putArraySizeDescriptor(dims);
        buffer.putShortArray(values, bytesToCopy);
        updateDataEndMarker(fieldHeader);
    }

    @Override
    public void put(final String fieldName, final String string) {
        final WireDataFieldDescription fieldHeader = putFieldHeader(fieldName, DataType.STRING);
        buffer.putString(string);
        updateDataEndMarker(fieldHeader);
    }

    @Override
    public void put(final String fieldName, final String[] values, final int n) {
        final WireDataFieldDescription fieldHeader = putFieldHeader(fieldName, DataType.STRING_ARRAY);
        final int valuesSize = values == null ? 0 : values.length;
        final int nElements = n >= 0 ? Math.min(n, valuesSize) : valuesSize;
        putArraySizeDescriptor(nElements);
        buffer.putStringArray(values, nElements);
        updateDataEndMarker(fieldHeader);
    }

    @Override
    public void put(final String fieldName, final String[] values, final int[] dims) {
        final WireDataFieldDescription fieldHeader = putFieldHeader(fieldName, DataType.STRING_ARRAY);
        if (dims.length == 2) {
            buffer.putByte(fieldHeader.getDataStartPosition() - 1, (byte) 24);
        } else if (dims.length > 2) {
            buffer.putByte(fieldHeader.getDataStartPosition() - 1, (byte) 32);
        }
        final int nElements = putArraySizeDescriptor(dims);
        buffer.putStringArray(values, nElements);
        updateDataEndMarker(fieldHeader);
    }

    @Override
    public <E> void put(final String fieldName, final Collection<E> collection, final Type valueType) {
        final DataType dataType;
        if (collection instanceof Queue) {
            dataType = DataType.QUEUE;
        } else if (collection instanceof Set) {
            dataType = DataType.SET;
        } else if (collection instanceof List) {
            dataType = DataType.LIST;
        } else {
            dataType = DataType.COLLECTION;
        }

        final WireDataFieldDescription fieldHeader = putFieldHeader(fieldName, dataType);
        this.put((FieldDescription) null, collection, valueType);
        this.updateDataEndMarker(fieldHeader);
    }

    @Override
    public void put(final String fieldName, final Enum<?> enumeration) {
        final WireDataFieldDescription fieldHeader = putFieldHeader(fieldName, DataType.ENUM);
        this.put((FieldDescription) null, enumeration);
        this.updateDataEndMarker(fieldHeader);
    }

    @Override
    public <K, V> void put(final String fieldName, final Map<K, V> map, final Type keyType, final Type valueType) {
        final WireDataFieldDescription fieldHeader = putFieldHeader(fieldName, DataType.MAP);
        this.put((FieldDescription) null, map, keyType, valueType);
        this.updateDataEndMarker(fieldHeader);
    }

    @Override
    public int putArraySizeDescriptor(final int n) {
        buffer.putInt(1); // number of dimensions
        buffer.putInt(n); // vector size for each dimension
        return n;
    }

    @Override
    public int putArraySizeDescriptor(final int[] dims) {
        buffer.putInt(dims.length); // number of dimensions
        int nElements = 1;
        for (final int dim : dims) {
            nElements *= dim;
            buffer.putInt(dim); // vector size for each dimension
        }
        return nElements;
    }

    @Override
    public <E> WireDataFieldDescription putCustomData(final FieldDescription fieldDescription, final E rootObject, Class<? extends E> type, final FieldSerialiser<E> serialiser) {
        throw new UnsupportedOperationException(NOT_IMPLEMENTED);
    }

    @Override
    public void putEndMarker(final FieldDescription fieldDescription) {
        if (parent.getParent() != null) {
            parent = (WireDataFieldDescription) parent.getParent();
        }
    }

    @Override
    public WireDataFieldDescription putFieldHeader(final FieldDescription fieldDescription) {
        return putFieldHeader(fieldDescription, fieldDescription.getDataType());
    }

    public WireDataFieldDescription putFieldHeader(final FieldDescription fieldDescription, DataType customDataType) {
        final boolean isScalar = customDataType.isScalar();

        final int headerStart = buffer.position();
        final String fieldName = fieldDescription.getFieldName();
        buffer.putStringISO8859(fieldName); // full field name
        buffer.putByte(getDataType(customDataType)); // data type ID

        final int dataStart = buffer.position();
        final int dataStartOffset = dataStart - headerStart;
        final int dataSize;
        if (isScalar) {
            dataSize = customDataType.getPrimitiveSize();
        } else if (customDataType == DataType.START_MARKER) {
            dataSize = FastByteBuffer.SIZE_OF_INT;
            buffer.ensureAdditionalCapacity(dataSize);
        } else {
            dataSize = -1;
            // from hereon there are data specific structures
            buffer.ensureAdditionalCapacity(16); // allocate 16+ bytes to account for potential array header (safe-bet)
        }
        lastFieldHeader = new WireDataFieldDescription(this, parent, fieldDescription.getFieldNameHashCode(), fieldDescription.getFieldName(), customDataType, headerStart, dataStartOffset, dataSize);
        updateDataEntryCount();

        return lastFieldHeader;
    }

    @Override
    public WireDataFieldDescription putFieldHeader(final String fieldName, final DataType dataType) {
        final boolean isScalar = dataType.isScalar();

        final int headerStart = buffer.position();
        buffer.putStringISO8859(fieldName); // full field name
        buffer.putByte(getDataType(dataType)); // data type ID

        final int dataStart = buffer.position();
        final int dataStartOffset = dataStart - headerStart;
        final int dataSize;
        if (isScalar) {
            dataSize = dataType.getPrimitiveSize();
        } else if (dataType == DataType.START_MARKER) {
            dataSize = FastByteBuffer.SIZE_OF_INT;
            buffer.ensureAdditionalCapacity(dataSize);
        } else {
            dataSize = -1;
            // from hereon there are data specific structures
            buffer.ensureAdditionalCapacity(16); // allocate 16+ bytes to account for potential array header (safe-bet)
        }

        final int fieldNameHashCode = fieldName.hashCode(); // TODO: check hashCode function
        lastFieldHeader = new WireDataFieldDescription(this, parent, fieldNameHashCode, fieldName, dataType, headerStart, dataStartOffset, dataSize);
        updateDataEntryCount();

        return lastFieldHeader;
    }

    @Override
    public void putHeaderInfo(final FieldDescription... field) {
        parent = lastFieldHeader = getRootElement();
        final String fieldName = "";
        final int dataSize = FastByteBuffer.SIZE_OF_INT;
        lastFieldHeader = new WireDataFieldDescription(this, parent, fieldName.hashCode(), fieldName, DataType.START_MARKER, buffer.position(), buffer.position(), dataSize);
        buffer.putInt(0);
        updateDataEntryCount();
        parent = lastFieldHeader;
    }

    @Override
    public void putStartMarker(final FieldDescription fieldDescription) {
        putFieldHeader(fieldDescription, DataType.START_MARKER);
        buffer.putInt(0);
        updateDataEndMarker(lastFieldHeader);
        parent = lastFieldHeader;
    }

    @Override
    public void setQueryFieldName(final String fieldName, final int dataStartPosition) {
        if (fieldName == null || fieldName.isBlank()) {
            throw new IllegalArgumentException("fieldName must not be null or blank: " + fieldName);
        }
        buffer.position(dataStartPosition);
    }

    @Override
    public void updateDataEndMarker(final WireDataFieldDescription fieldHeader) {
        final int dataSize = buffer.position() - fieldHeader.getDataStartPosition();
        if (fieldHeader.getDataSize() != dataSize) {
            fieldHeader.setDataSize(dataSize);
        }
    }

    private WireDataFieldDescription getRootElement() {
        return new WireDataFieldDescription(this, null, "ROOT".hashCode(), "ROOT", DataType.OTHER, buffer.position(), -1, -1);
    }

    private void updateDataEntryCount() {
        // increment parent child count
        if (parent == null) {
            throw new IllegalStateException("no parent");
        }

        final int parentDataStart = parent.getDataStartPosition();
        if (parentDataStart >= 0) { // N.B. needs to be '>=' since CMW header is an incomplete field header containing only an 'nEntries<int>' data field
            buffer.position(parentDataStart);
            final int nEntries = buffer.getInt();
            buffer.position(parentDataStart);
            buffer.putInt(nEntries + 1);
            buffer.position(lastFieldHeader.getDataStartPosition());
        }
    }

    public static byte getDataType(final DataType dataType) {
        final int id = dataType.getID();
        if (DATA_TYPE_TO_BYTE[id] != null) {
            return DATA_TYPE_TO_BYTE[id];
        }

        throw new IllegalArgumentException("DataType " + dataType + " not mapped to specific byte");
    }

    public static DataType getDataType(final byte byteValue) {
        final int id = byteValue & 0xFF;
        if (BYTE_TO_DATA_TYPE[id] != null) {
            return BYTE_TO_DATA_TYPE[id];
        }

        throw new IllegalArgumentException("DataType byteValue=" + byteValue + " rawByteValue=" + (byteValue & 0xFF) + " not mapped");
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
