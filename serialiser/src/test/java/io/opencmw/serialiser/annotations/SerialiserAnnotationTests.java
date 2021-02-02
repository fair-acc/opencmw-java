package io.opencmw.serialiser.annotations;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.InvocationTargetException;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.opencmw.serialiser.FieldDescription;
import io.opencmw.serialiser.IoBuffer;
import io.opencmw.serialiser.IoClassSerialiser;
import io.opencmw.serialiser.spi.BinarySerialiser;
import io.opencmw.serialiser.spi.ByteBuffer;
import io.opencmw.serialiser.spi.ClassFieldDescription;
import io.opencmw.serialiser.spi.FastByteBuffer;
import io.opencmw.serialiser.spi.WireDataFieldDescription;
import io.opencmw.serialiser.utils.ClassUtils;

class SerialiserAnnotationTests {
    private static final int BUFFER_SIZE = 40000;

    @Test
    void testAnnotationGeneration() {
        // test annotation parsing on the generation side
        final AnnotatedDataClass dataClass = new AnnotatedDataClass();
        final ClassFieldDescription classFieldDescription = ClassUtils.getFieldDescription(dataClass.getClass());
        // classFieldDescription.printFieldStructure();

        final FieldDescription energyField = classFieldDescription.findChildField("energy".hashCode(), "energy");
        assertNotNull(energyField);
        assertEquals("GeV/u", energyField.getFieldUnit());
        assertEquals("energy description", energyField.getFieldDescription());
        assertEquals("OUT", energyField.getFieldDirection());
        assertFalse(energyField.getFieldGroups().isEmpty());
        assertEquals("A", energyField.getFieldGroups().get(0));

        final FieldDescription temperatureField = classFieldDescription.findChildField("temperature".hashCode(), "temperature");
        assertNotNull(temperatureField);
        assertEquals("°C", temperatureField.getFieldUnit());
        assertEquals("important temperature reading", temperatureField.getFieldDescription());
        assertEquals("OUT", temperatureField.getFieldDirection());
        assertFalse(temperatureField.getFieldGroups().isEmpty());
        assertEquals(2, temperatureField.getFieldGroups().size());
        assertEquals("A", temperatureField.getFieldGroups().get(0));
        assertEquals("B", temperatureField.getFieldGroups().get(1));
    }

    @DisplayName("basic custom serialisation/deserialisation identity")
    @ParameterizedTest(name = "IoBuffer class - {0} recursion level {1}")
    @ValueSource(classes = { ByteBuffer.class, FastByteBuffer.class })
    void testCustomSerialiserIdentity(final Class<? extends IoBuffer> bufferClass) throws NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        assertNotNull(bufferClass, "bufferClass being not null");
        assertNotNull(bufferClass.getConstructor(int.class), "Constructor(Integer) present");
        final IoBuffer buffer = bufferClass.getConstructor(int.class).newInstance(BUFFER_SIZE);
        final BinarySerialiser ioSerialiser = new BinarySerialiser(buffer);
        final IoClassSerialiser serialiser = new IoClassSerialiser(buffer, ioSerialiser.getClass());

        final AnnotatedDataClass inputObject = new AnnotatedDataClass();

        buffer.reset();
        serialiser.serialiseObject(inputObject);

        buffer.reset();
        final WireDataFieldDescription root = ioSerialiser.parseIoStream(true);
        final FieldDescription serialiserFieldDescriptions = root.getChildren().get(0);

        final FieldDescription energyField = serialiserFieldDescriptions.findChildField("energy".hashCode(), "energy");
        assertNotNull(energyField);
        assertEquals("GeV/u", energyField.getFieldUnit());
        assertEquals("energy description", energyField.getFieldDescription());
        assertEquals("OUT", energyField.getFieldDirection());
        assertFalse(energyField.getFieldGroups().isEmpty());
        assertEquals("A", energyField.getFieldGroups().get(0));

        final FieldDescription temperatureField = serialiserFieldDescriptions.findChildField("temperature".hashCode(), "temperature");
        assertNotNull(temperatureField);
        assertEquals("°C", temperatureField.getFieldUnit());
        assertEquals("important temperature reading", temperatureField.getFieldDescription());
        assertEquals("OUT", temperatureField.getFieldDirection());
        assertFalse(temperatureField.getFieldGroups().isEmpty());
        assertEquals(2, temperatureField.getFieldGroups().size());
        assertEquals("A", temperatureField.getFieldGroups().get(0));
        assertEquals("B", temperatureField.getFieldGroups().get(1));
    }

    @Description("this class is used to test field annotation")
    public class AnnotatedDataClass {
        @MetaInfo(unit = "GeV/u", description = "energy description", direction = "OUT", groups = "A")
        public double energy;

        @Unit("°C")
        @Description("important temperature reading")
        @Direction("OUT")
        @Groups({ "A", "B" })
        public double temperature;

        @Unit("V")
        @Description("control variable")
        @Direction("IN/OUT")
        public double controlVariable;
    }
}
