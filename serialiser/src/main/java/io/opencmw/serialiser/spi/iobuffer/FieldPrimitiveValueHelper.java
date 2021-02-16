package io.opencmw.serialiser.spi.iobuffer;

import io.opencmw.serialiser.FieldSerialiser;
import io.opencmw.serialiser.IoClassSerialiser;

/**
 * helper class to register default serialiser for primitive types (ie. boolean, byte, short, ..., double) and String
 * 
 * @author rstein
 */
public final class FieldPrimitiveValueHelper {
    public static final String UNSUPPORTED = "return function not supported for primitive types";

    private FieldPrimitiveValueHelper() {
        // utility class
    }

    /**
     * registers default serialiser for primitive types (ie. boolean, byte, short, ..., double) and String
     * 
     * @param serialiser for which the field serialisers should be registered
     */
    public static void register(final IoClassSerialiser serialiser) {
        serialiser.addClassDefinition(new FieldSerialiser<>( //
                (io, obj, field) -> field.getField().setBoolean(obj, io.getBoolean()), // reader
                (io, obj, field) -> { throw new UnsupportedOperationException(UNSUPPORTED); }, // return
                (io, obj, field) -> io.put(field, field.getField().getBoolean(obj)), // writer
                boolean.class));

        serialiser.addClassDefinition(new FieldSerialiser<>( //
                (io, obj, field) -> field.getField().setByte(obj, io.getByte()), // reader
                (io, obj, field) -> { throw new UnsupportedOperationException(UNSUPPORTED); }, // return
                (io, obj, field) -> io.put(field, field.getField().getByte(obj)), // writer
                byte.class));

        serialiser.addClassDefinition(new FieldSerialiser<>( //
                (io, obj, field) -> field.getField().setChar(obj, io.getChar()), // reader
                (io, obj, field) -> { throw new UnsupportedOperationException(UNSUPPORTED); }, // return
                (io, obj, field) -> io.put(field, field.getField().getChar(obj)), // writer
                char.class));

        serialiser.addClassDefinition(new FieldSerialiser<>( //
                (io, obj, field) -> field.getField().setShort(obj, io.getShort()), // reader
                (io, obj, field) -> { throw new UnsupportedOperationException(UNSUPPORTED); }, // return
                (io, obj, field) -> io.put(field, field.getField().getShort(obj)), // writer
                short.class));

        serialiser.addClassDefinition(new FieldSerialiser<>( //
                (io, obj, field) -> field.getField().setInt(obj, io.getInt()), // reader
                (io, obj, field) -> { throw new UnsupportedOperationException(UNSUPPORTED); }, // return
                (io, obj, field) -> io.put(field, field.getField().getInt(obj)), // writer
                int.class));

        serialiser.addClassDefinition(new FieldSerialiser<>( //
                (io, obj, field) -> field.getField().setLong(obj, io.getLong()), // reader
                (io, obj, field) -> { throw new UnsupportedOperationException(UNSUPPORTED); }, // return
                (io, obj, field) -> io.put(field, field.getField().getLong(obj)), // writer
                long.class));

        serialiser.addClassDefinition(new FieldSerialiser<>( //
                (io, obj, field) -> field.getField().setFloat(obj, io.getFloat()), // reader
                (io, obj, field) -> { throw new UnsupportedOperationException(UNSUPPORTED); }, // return
                (io, obj, field) -> io.put(field, field.getField().getFloat(obj)), // writer
                float.class));

        serialiser.addClassDefinition(new FieldSerialiser<>( //
                (io, obj, field) -> field.getField().setDouble(obj, io.getDouble()), // reader
                (io, obj, field) -> { throw new UnsupportedOperationException(UNSUPPORTED); }, // return
                (io, obj, field) -> io.put(field, field.getField().getDouble(obj)), // writer
                double.class));

        serialiser.addClassDefinition(new FieldSerialiser<>( //
                (io, obj, field) -> field.getField().set(obj, io.getString()), // reader
                (io, obj, field) -> { throw new UnsupportedOperationException(UNSUPPORTED); }, // return
                (io, obj, field) -> io.put(field, (String) field.getField().get(obj)), // writer
                String.class));
    }
}
