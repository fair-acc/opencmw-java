package io.opencmw.serialiser.utils;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opencmw.serialiser.DataType;
import io.opencmw.serialiser.IoBuffer;
import io.opencmw.serialiser.IoClassSerialiser;
import io.opencmw.serialiser.IoSerialiser;
import io.opencmw.serialiser.benchmark.SerialiserQuickBenchmark;
import io.opencmw.serialiser.spi.BinarySerialiser;
import io.opencmw.serialiser.spi.FastByteBuffer;
import io.opencmw.serialiser.spi.WireDataFieldDescription;

@SuppressWarnings("PMD") // complexity is part of the very large use-case surface that is being tested
public final class SerialiserHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(SerialiserQuickBenchmark.class); // N.B. SerialiserQuickBenchmark reference on purpose
    private static final IoBuffer byteBuffer = new FastByteBuffer(100000);

    // private static final IoBuffer byteBuffer = new ByteBuffer(20000);
    private static final BinarySerialiser binarySerialiser = new BinarySerialiser(byteBuffer);
    private static final IoClassSerialiser ioSerialiser = new IoClassSerialiser(byteBuffer, BinarySerialiser.class);

    public static int checkCustomSerialiserIdentity(final TestDataClass inputObject, TestDataClass outputObject) {
        outputObject.clear();
        byteBuffer.reset();
        SerialiserHelper.serialiseCustom(binarySerialiser, inputObject);

        byteBuffer.flip();

        // keep: checks serialised data structure
        // byteBuffer.reset();
        // final WireDataFieldDescription fieldRoot = SerialiserHelper.deserialiseMap(byteBuffer);
        // fieldRoot.printFieldStructure();

        SerialiserHelper.deserialiseCustom(binarySerialiser, outputObject);

        // second test - both vectors should have the same initial values after serialise/deserialise
        assertArrayEquals(inputObject.stringArray, outputObject.stringArray);

        assertEquals(inputObject, outputObject, "TestDataClass input-output equality");
        return byteBuffer.limit();
    }

    public static int checkSerialiserIdentity(final TestDataClass inputObject, TestDataClass outputObject) {
        outputObject.clear();
        byteBuffer.reset();

        ioSerialiser.serialiseObject(inputObject);

        // SerialiserHelper.serialiseCustom(byteBuffer, inputObject);

        byteBuffer.flip();
        // keep: checks serialised data structure
        // byteBuffer.reset();
        // final WireDataFieldDescription fieldRoot = SerialiserHelper.deserialiseMap(byteBuffer);
        // fieldRoot.printFieldStructure();

        outputObject = (TestDataClass) ioSerialiser.deserialiseObject(outputObject);

        // second test - both vectors should have the same initial values after serialise/deserialise
        assertArrayEquals(inputObject.stringArray, outputObject.stringArray);

        assertEquals(inputObject, outputObject, "TestDataClass input-output equality");

        return byteBuffer.limit();
    }

    public static void deserialiseCustom(IoSerialiser ioSerialiser, final TestDataClass pojo) {
        deserialiseCustom(ioSerialiser, pojo, true);
    }

    public static void deserialiseCustom(IoSerialiser ioSerialiser, final TestDataClass pojo, boolean header) {
        if (header) {
            ioSerialiser.checkHeaderInfo();
        }

        getFieldHeader(ioSerialiser);
        pojo.bool1 = ioSerialiser.getBoolean();
        getFieldHeader(ioSerialiser);
        pojo.bool2 = ioSerialiser.getBoolean();

        getFieldHeader(ioSerialiser);
        pojo.byte1 = ioSerialiser.getByte();
        getFieldHeader(ioSerialiser);
        pojo.byte2 = ioSerialiser.getByte();

        getFieldHeader(ioSerialiser);
        pojo.char1 = ioSerialiser.getChar();
        getFieldHeader(ioSerialiser);
        pojo.char2 = ioSerialiser.getChar();

        getFieldHeader(ioSerialiser);
        pojo.short1 = ioSerialiser.getShort();
        getFieldHeader(ioSerialiser);
        pojo.short2 = ioSerialiser.getShort();

        getFieldHeader(ioSerialiser);
        pojo.int1 = ioSerialiser.getInt();
        getFieldHeader(ioSerialiser);
        pojo.int2 = ioSerialiser.getInt();

        getFieldHeader(ioSerialiser);
        pojo.long1 = ioSerialiser.getLong();
        getFieldHeader(ioSerialiser);
        pojo.long2 = ioSerialiser.getLong();

        getFieldHeader(ioSerialiser);
        pojo.float1 = ioSerialiser.getFloat();
        getFieldHeader(ioSerialiser);
        pojo.float2 = ioSerialiser.getFloat();

        getFieldHeader(ioSerialiser);
        pojo.double1 = ioSerialiser.getDouble();
        getFieldHeader(ioSerialiser);
        pojo.double2 = ioSerialiser.getDouble();

        getFieldHeader(ioSerialiser);
        pojo.string1 = ioSerialiser.getString();
        getFieldHeader(ioSerialiser);
        pojo.string2 = ioSerialiser.getString();

        // 1-dim arrays
        getFieldHeader(ioSerialiser);
        pojo.boolArray = ioSerialiser.getBooleanArray();
        getFieldHeader(ioSerialiser);
        pojo.byteArray = ioSerialiser.getByteArray();
        //getFieldHeader(ioSerialiser);
        //pojo.charArray = ioSerialiser.getCharArray(ioSerialiser);
        getFieldHeader(ioSerialiser);
        pojo.shortArray = ioSerialiser.getShortArray();
        getFieldHeader(ioSerialiser);
        pojo.intArray = ioSerialiser.getIntArray();
        getFieldHeader(ioSerialiser);
        pojo.longArray = ioSerialiser.getLongArray();
        getFieldHeader(ioSerialiser);
        pojo.floatArray = ioSerialiser.getFloatArray();
        getFieldHeader(ioSerialiser);
        pojo.doubleArray = ioSerialiser.getDoubleArray();
        getFieldHeader(ioSerialiser);
        pojo.stringArray = ioSerialiser.getStringArray();

        // multidim case
        getFieldHeader(ioSerialiser);
        pojo.nDimensions = ioSerialiser.getIntArray();
        getFieldHeader(ioSerialiser);
        pojo.boolNdimArray = ioSerialiser.getBooleanArray();
        getFieldHeader(ioSerialiser);
        pojo.byteNdimArray = ioSerialiser.getByteArray();
        getFieldHeader(ioSerialiser);
        pojo.shortNdimArray = ioSerialiser.getShortArray();
        getFieldHeader(ioSerialiser);
        pojo.intNdimArray = ioSerialiser.getIntArray();
        getFieldHeader(ioSerialiser);
        pojo.longNdimArray = ioSerialiser.getLongArray();
        getFieldHeader(ioSerialiser);
        pojo.floatNdimArray = ioSerialiser.getFloatArray();
        getFieldHeader(ioSerialiser);
        pojo.doubleNdimArray = ioSerialiser.getDoubleArray();

        final WireDataFieldDescription field = getFieldHeader(ioSerialiser);
        if (field.getDataType().equals(DataType.START_MARKER)) {
            if (pojo.nestedData == null) {
                pojo.nestedData = new TestDataClass();
            }
            deserialiseCustom(ioSerialiser, pojo.nestedData, false);

        } else if (!field.getDataType().equals(DataType.END_MARKER)) {
            throw new IllegalStateException("format error/unexpected tag with data type = " + field.getDataType() + " and field name = " + field.getFieldName());
        }
    }

    public static WireDataFieldDescription deserialiseMap(IoSerialiser ioSerialiser) {
        return ioSerialiser.parseIoStream(true);
    }

    public static BinarySerialiser getBinarySerialiser() {
        return binarySerialiser;
    }

    public static IoBuffer getByteBuffer() {
        return byteBuffer;
    }

    public static void serialiseCustom(IoSerialiser ioSerialiser, final TestDataClass pojo) {
        serialiseCustom(ioSerialiser, pojo, true);
    }

    public static void serialiseCustom(final IoSerialiser ioSerialiser, final TestDataClass pojo, final boolean header) {
        if (header) {
            ioSerialiser.putHeaderInfo();
        }

        ioSerialiser.put("bool1", pojo.bool1);
        ioSerialiser.put("bool2", pojo.bool2);
        ioSerialiser.put("byte1", pojo.byte1);
        ioSerialiser.put("byte2", pojo.byte2);
        ioSerialiser.put("char1", pojo.char1);
        ioSerialiser.put("char2", pojo.char2);
        ioSerialiser.put("short1", pojo.short1);
        ioSerialiser.put("short2", pojo.short2);
        ioSerialiser.put("int1", pojo.int1);
        ioSerialiser.put("int2", pojo.int2);
        ioSerialiser.put("long1", pojo.long1);
        ioSerialiser.put("long2", pojo.long2);
        ioSerialiser.put("float1", pojo.float1);
        ioSerialiser.put("float2", pojo.float2);
        ioSerialiser.put("double1", pojo.double1);
        ioSerialiser.put("double2", pojo.double2);
        ioSerialiser.put("string1", pojo.string1);
        ioSerialiser.put("string2", pojo.string2);

        // 1D-arrays
        ioSerialiser.put("boolArray", pojo.boolArray, pojo.boolArray.length);
        ioSerialiser.put("byteArray", pojo.byteArray, pojo.byteArray.length);
        //ioSerialiser.put("charArray", pojo.charArray,  pojo.charArray.lenght);
        ioSerialiser.put("shortArray", pojo.shortArray, pojo.shortArray.length);
        ioSerialiser.put("intArray", pojo.intArray, pojo.intArray.length);
        ioSerialiser.put("longArray", pojo.longArray, pojo.longArray.length);
        ioSerialiser.put("floatArray", pojo.floatArray, pojo.floatArray.length);
        ioSerialiser.put("doubleArray", pojo.doubleArray, pojo.doubleArray.length);
        ioSerialiser.put("stringArray", pojo.stringArray, pojo.stringArray.length);

        // multi-dim case
        ioSerialiser.put("nDimensions", pojo.nDimensions, pojo.nDimensions.length);
        ioSerialiser.put("boolNdimArray", pojo.boolNdimArray, pojo.nDimensions);
        ioSerialiser.put("byteNdimArray", pojo.byteNdimArray, pojo.nDimensions);
        //ioSerialiser.put("charNdimArray", pojo.nDimensions);
        ioSerialiser.put("shortNdimArray", pojo.shortNdimArray, pojo.nDimensions);
        ioSerialiser.put("intNdimArray", pojo.intNdimArray, pojo.nDimensions);
        ioSerialiser.put("longNdimArray", pojo.longNdimArray, pojo.nDimensions);
        ioSerialiser.put("floatNdimArray", pojo.floatNdimArray, pojo.nDimensions);
        ioSerialiser.put("doubleNdimArray", pojo.doubleNdimArray, pojo.nDimensions);

        if (pojo.nestedData != null) {
            final String dataStartMarkerName = "nestedData";
            final WireDataFieldDescription nestedDataMarker = new WireDataFieldDescription(ioSerialiser, null, dataStartMarkerName.hashCode(), dataStartMarkerName, DataType.START_MARKER, -1, -1, -1);
            ioSerialiser.putStartMarker(nestedDataMarker);
            serialiseCustom(ioSerialiser, pojo.nestedData, false);
            ioSerialiser.putEndMarker(nestedDataMarker);
        }

        if (header) {
            final String dataEndMarkerName = "OBJ_ROOT_END";
            final WireDataFieldDescription dataEndMarker = new WireDataFieldDescription(ioSerialiser, null, dataEndMarkerName.hashCode(), dataEndMarkerName, DataType.START_MARKER, -1, -1, -1);
            ioSerialiser.putEndMarker(dataEndMarker);
        }
    }

    public static void testCustomSerialiserPerformance(final int iterations, final TestDataClass inputObject, final TestDataClass outputObject) {
        final long startTime = System.nanoTime();

        for (int i = 0; i < iterations; i++) {
            byteBuffer.reset();
            SerialiserHelper.serialiseCustom(binarySerialiser, inputObject);

            byteBuffer.reset();
            SerialiserHelper.deserialiseCustom(binarySerialiser, outputObject);

            if (!inputObject.string1.contentEquals(outputObject.string1)) {
                // quick check necessary so that the above is not optimised by the Java JIT compiler to NOP
                throw new IllegalStateException("data mismatch");
            }
        }
        if (iterations <= 1) {
            // JMH use-case
            return;
        }

        final long stopTime = System.nanoTime();

        final double diffMillis = TimeUnit.NANOSECONDS.toMillis(stopTime - startTime);
        final double byteCount = iterations * ((byteBuffer.position() / diffMillis) * 1e3);
        LOGGER.atInfo().addArgument(SerialiserQuickBenchmark.humanReadableByteCount((long) byteCount, true)) //
                .addArgument(SerialiserQuickBenchmark.humanReadableByteCount(byteBuffer.position(), true)) //
                .addArgument(diffMillis) //
                .log("IO Serializer (custom) throughput = {}/s for {} per test run (took {} ms)");
    }

    public static void testPerformancePojo(final int iterations, final TestDataClass inputObject, TestDataClass outputObject) {
        binarySerialiser.setPutFieldMetaData(true);
        final long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            if (i == 1) {
                // only stream meta-data the first iteration
                binarySerialiser.setPutFieldMetaData(false);
            }
            byteBuffer.reset();
            ioSerialiser.serialiseObject(inputObject);

            byteBuffer.reset();

            outputObject = (TestDataClass) ioSerialiser.deserialiseObject(outputObject);

            if (!inputObject.string1.contentEquals(outputObject.string1)) {
                // quick check necessary so that the above is not optimised by the Java JIT compiler to NOP
                throw new IllegalStateException("data mismatch");
            }
        }
        if (iterations <= 1) {
            // JMH use-case
            return;
        }
        final long stopTime = System.nanoTime();

        final double diffMillis = TimeUnit.NANOSECONDS.toMillis(stopTime - startTime);
        final double byteCount = iterations * ((byteBuffer.position() / diffMillis) * 1e3);
        LOGGER.atInfo().addArgument(SerialiserQuickBenchmark.humanReadableByteCount((long) byteCount, true)) //
                .addArgument(SerialiserQuickBenchmark.humanReadableByteCount(byteBuffer.position(), true)) //
                .addArgument(diffMillis) //
                .log("IO Serializer (POJO) throughput = {}/s for {} per test run (took {} ms)");
    }

    public static WireDataFieldDescription testSerialiserPerformanceMap(final int iterations, final TestDataClass inputObject) {
        final long startTime = System.nanoTime();

        WireDataFieldDescription ret = null;
        for (int i = 0; i < iterations; i++) {
            byteBuffer.reset();
            SerialiserHelper.serialiseCustom(binarySerialiser, inputObject);
            byteBuffer.reset();
            ret = SerialiserHelper.deserialiseMap(binarySerialiser);

            if (ret.getDataSize() == 0) {
                // quick check necessary so that the above is not optimised by the Java JIT compiler to NOP
                throw new IllegalStateException("data mismatch");
            }
        }
        if (iterations <= 1) {
            // JMH use-case
            return ret;
        }

        final long stopTime = System.nanoTime();

        final double diffMillis = TimeUnit.NANOSECONDS.toMillis(stopTime - startTime);
        final double byteCount = iterations * ((byteBuffer.position() / diffMillis) * 1e3);
        LOGGER.atInfo().addArgument(SerialiserQuickBenchmark.humanReadableByteCount((long) byteCount, true)) //
                .addArgument(SerialiserQuickBenchmark.humanReadableByteCount(byteBuffer.position(), true)) //
                .addArgument(diffMillis) //
                .log("IO Serializer (Map only)  throughput = {}/s for {} per test run (took {} ms)");
        return ret;
    }

    private static WireDataFieldDescription getFieldHeader(IoSerialiser ioSerialiser) {
        WireDataFieldDescription field = ioSerialiser.getFieldHeader();
        ioSerialiser.getBuffer().position(field.getDataStartPosition());
        return field;
    }
}
