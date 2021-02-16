package io.opencmw.serialiser.spi.iobuffer;

import io.opencmw.serialiser.FieldSerialiser;
import io.opencmw.serialiser.IoClassSerialiser;

/**
 * helper class to register default serialiser for boxed primitive types (ie. Boolean, Byte, Short, ..., double) w/o
 * String (already part of the {@link FieldPrimitiveValueHelper}
 * 
 * @author rstein
 */
public final class FieldBoxedValueHelper {
    public static final String NOT_SUPPORTED_FOR_PRIMITIVES = "return function not supported for primitive types";

    private FieldBoxedValueHelper() {
        // utility class
    }

    /**
     * registers default serialiser for primitive array types (ie. boolean[], byte[], short[], ..., double[]) and
     * String[]
     * 
     * @param serialiser for which the field serialisers should be registered
     */
    public static void register(final IoClassSerialiser serialiser) {
        serialiser.addClassDefinition(new FieldSerialiser<>( //
                (io, obj, field) -> field.getField().set(obj, io.getBoolean()), // reader
                (io, obj, field) -> { throw new UnsupportedOperationException(NOT_SUPPORTED_FOR_PRIMITIVES); }, // return
                (io, obj, field) -> io.put(field, (Boolean) field.getField().get(obj)), // writer
                Boolean.class));

        serialiser.addClassDefinition(new FieldSerialiser<>( //
                (io, obj, field) -> field.getField().set(obj, io.getByte()), // reader
                (io, obj, field) -> { throw new UnsupportedOperationException(NOT_SUPPORTED_FOR_PRIMITIVES); }, // return
                (io, obj, field) -> io.put(field, (Byte) field.getField().get(obj)), // writer
                Byte.class));

        serialiser.addClassDefinition(new FieldSerialiser<>( //
                (io, obj, field) -> field.getField().set(obj, io.getShort()), // reader
                (io, obj, field) -> { throw new UnsupportedOperationException(NOT_SUPPORTED_FOR_PRIMITIVES); }, // return
                (io, obj, field) -> io.put(field, (Short) field.getField().get(obj)), // writer
                Short.class));

        serialiser.addClassDefinition(new FieldSerialiser<>( //
                (io, obj, field) -> field.getField().set(obj, io.getInt()), // reader
                (io, obj, field) -> { throw new UnsupportedOperationException(NOT_SUPPORTED_FOR_PRIMITIVES); }, // return
                (io, obj, field) -> io.put(field, (Integer) field.getField().get(obj)), // writer
                Integer.class));

        serialiser.addClassDefinition(new FieldSerialiser<>( //
                (io, obj, field) -> field.getField().set(obj, io.getLong()), // reader
                (io, obj, field) -> { throw new UnsupportedOperationException(NOT_SUPPORTED_FOR_PRIMITIVES); }, // return
                (io, obj, field) -> io.put(field, (Long) field.getField().get(obj)), // writer
                Long.class));

        serialiser.addClassDefinition(new FieldSerialiser<>( //
                (io, obj, field) -> field.getField().set(obj, io.getFloat()), // reader
                (io, obj, field) -> { throw new UnsupportedOperationException(NOT_SUPPORTED_FOR_PRIMITIVES); }, // return
                (io, obj, field) -> io.put(field, (Float) field.getField().get(obj)), // writer
                Float.class));

        serialiser.addClassDefinition(new FieldSerialiser<>( //
                (io, obj, field) -> field.getField().set(obj, io.getDouble()), // reader
                (io, obj, field) -> { throw new UnsupportedOperationException(NOT_SUPPORTED_FOR_PRIMITIVES); }, // return
                (io, obj, field) -> io.put(field, (Double) field.getField().get(obj)), // writer
                Double.class));
    }
}
