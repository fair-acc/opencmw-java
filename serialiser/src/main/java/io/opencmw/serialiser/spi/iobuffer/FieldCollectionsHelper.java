package io.opencmw.serialiser.spi.iobuffer;

import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import io.opencmw.serialiser.FieldSerialiser;
import io.opencmw.serialiser.IoClassSerialiser;
import io.opencmw.serialiser.utils.ClassUtils;

public final class FieldCollectionsHelper {
    private FieldCollectionsHelper() {
        // utility class
    }

    /**
     * registers default Collection, List, Set, and Queue interface and related helper methods
     *
     * @param serialiser for which the field serialisers should be registered
     */
    @SuppressWarnings("PMD.NPathComplexity")
    public static void register(final IoClassSerialiser serialiser) {
        // Collection serialiser mapper to IoBuffer
        final FieldSerialiser.TriFunction<Collection<?>> returnCollection = (io, obj, field) -> //
                io.getCollection(field == null ? null : (Collection<?>) field.getField().get(obj)); // return function
        final FieldSerialiser.TriFunction<Collection<?>> returnList = (io, obj, field) -> //
                io.getList(field == null ? null : (List<?>) field.getField().get(obj)); // return function
        final FieldSerialiser.TriFunction<Collection<?>> returnQueue = (io, obj, field) -> //
                io.getQueue(field == null ? null : (Queue<?>) field.getField().get(obj)); // return function
        final FieldSerialiser.TriFunction<Collection<?>> returnSet = (io, obj, field) -> //
                io.getSet(field == null ? null : (Set<?>) field.getField().get(obj)); // return function

        final FieldSerialiser.TriConsumer collectionWriter = (io, obj, field) -> {
            if (field != null && !field.getActualTypeArguments().isEmpty() && ClassUtils.isPrimitiveWrapperOrString(ClassUtils.getRawType(field.getActualTypeArguments().get(0)))) {
                io.put(field, (Collection<?>) field.getField().get(obj), field.getActualTypeArguments().get(0));
                return;
            }
            if (field != null) {
                // Collection<custom class> serialiser
                io.put(field, (Collection<?>) field.getField().get(obj), field.getActualTypeArguments().get(0));
                return;
            }
            throw new IllegalArgumentException("serialiser for obj = '" + obj + "' and type = '" + (obj == null ? "null" : obj.getClass()) + "'  not yet implemented, field = null");
        }; // writer

        serialiser.addClassDefinition(new FieldSerialiser<>((io, obj, field) -> //
                field.getField().set(obj, returnCollection.apply(io, obj, field)), // reader
                returnCollection, collectionWriter, Collection.class));

        serialiser.addClassDefinition(new FieldSerialiser<>((io, obj, field) -> //
                field.getField().set(obj, returnList.apply(io, obj, field)), // reader
                returnList, collectionWriter, List.class));

        serialiser.addClassDefinition(new FieldSerialiser<>((io, obj, field) -> //
                field.getField().set(obj, returnQueue.apply(io, obj, field)), // reader
                returnQueue, collectionWriter, Queue.class));

        serialiser.addClassDefinition(new FieldSerialiser<>((io, obj, field) -> //
                field.getField().set(obj, returnSet.apply(io, obj, field)), // reader
                returnSet, collectionWriter, Set.class));
    }
}
