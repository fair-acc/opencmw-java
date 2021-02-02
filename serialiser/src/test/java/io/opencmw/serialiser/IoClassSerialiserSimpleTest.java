package io.opencmw.serialiser;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import io.opencmw.serialiser.spi.BinarySerialiser;
import io.opencmw.serialiser.spi.FastByteBuffer;
import io.opencmw.serialiser.utils.TestDataClass;

/**
 * This is a simple example illustrating the use of the IoClassSerialiser.
 *
 * @author rstein
 */
class IoClassSerialiserSimpleTest {
    @Test
    void simpleTest() {
        final IoBuffer byteBuffer = new FastByteBuffer(10_000); // alt: new ByteBuffer(10_000);
        final IoClassSerialiser ioClassSerialiser = new IoClassSerialiser(byteBuffer, BinarySerialiser.class);
        // alt:
        // ioClassSerialiser.setMatchedIoSerialiser(BinarySerialiser.class);
        // ioClassSerialiser.setMatchedIoSerialiser(CmwLightSerialiser.class);
        // ioClassSerialiser.setMatchedIoSerialiser(JsonSerialiser.class);
        // ioClassSerialiser.setAutoMatchSerialiser(true); // to auto-detect the suitable serialiser based on serialised data header

        TestDataClass data = new TestDataClass(); // object to be serialised

        byteBuffer.reset();
        ioClassSerialiser.serialiseObject(data); // pojo -> serialised data
        // [..] stream/write serialised byteBuffer content [..]
        // final byte[] serialisedData = byteBuffer.elements();
        // final int serialisedDataSize = byteBuffer.limit();

        // [..] stream/read serialised byteBuffer content
        byteBuffer.flip(); // mark byte-buffer for reading
        TestDataClass received = ioClassSerialiser.deserialiseObject(TestDataClass.class);

        // check data equality, etc...
        assertEquals(data, received);
    }
}
