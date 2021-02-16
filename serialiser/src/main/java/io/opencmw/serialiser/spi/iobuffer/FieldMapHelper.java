package io.opencmw.serialiser.spi.iobuffer;

import java.util.Map;

import io.opencmw.serialiser.FieldSerialiser;
import io.opencmw.serialiser.IoClassSerialiser;

public final class FieldMapHelper {
    private FieldMapHelper() {
        // utility class
    }

    /**
     * registers default Map interface and related helper methods
     *
     * @param serialiser for which the field serialisers should be registered
     */
    public static void register(final IoClassSerialiser serialiser) {
        // Map serialiser mapper to IoBuffer

        final FieldSerialiser.TriFunction<Map<?, ?>> returnMapFunction = (io, obj, field) -> {
            final Map<?, ?> origMap = field == null ? null : (Map<?, ?>) field.getField().get(obj);
            return io.getMap(origMap);
        };

        serialiser.addClassDefinition(new FieldSerialiser<>( //
                (io, obj, field) -> field.getField().set(obj, returnMapFunction.apply(io, obj, field)), // reader
                returnMapFunction, // return
                (io, obj, field) -> io.put(field, (Map<?, ?>) field.getField().get(obj), field.getActualTypeArguments().get(0), field.getActualTypeArguments().get(1)), // writer
                Map.class));
    }
}
