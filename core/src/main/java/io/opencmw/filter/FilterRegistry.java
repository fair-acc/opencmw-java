package io.opencmw.filter;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import io.opencmw.Filter;
import io.opencmw.serialiser.FieldDescription;
import io.opencmw.serialiser.spi.ClassFieldDescription;
import io.opencmw.serialiser.utils.ClassUtils;

/**
 * Global registry of known Filter objects.
 * @author rstein
 */
@SuppressWarnings("PMD.UseConcurrentHashMap") // thread-safe use -- only reads after initialisation
public final class FilterRegistry { // NOPMD - nomen est omen
    private static final AtomicReference<Map<String, Filter>> MAP_KEY_FILTER = new AtomicReference<>(new HashMap<>()); //  <filter key, filter prototype>
    private static final AtomicReference<Map<Class<? extends Filter>, Filter>> MAP_CLASS_FILTER = new AtomicReference<>(new HashMap<>()); // <filter class, filter prototype>
    private static final AtomicReference<Map<Class<? extends Filter>, String>> MAP_CLASS_FILTER_KEY = new AtomicReference<>(new HashMap<>()); // <filter class, filter prototype>

    private FilterRegistry() {
        // global static helper class
    }

    public static Map<String, Filter> getKeyFilterMap() {
        return Collections.unmodifiableMap(MAP_KEY_FILTER.get());
    }

    public static Map<Class<? extends Filter>, Filter> getClassFilterMap() {
        return Collections.unmodifiableMap(MAP_CLASS_FILTER.get());
    }

    public static Map<Class<? extends Filter>, String> getClassFilterKeyMap() {
        return Collections.unmodifiableMap(MAP_CLASS_FILTER_KEY.get());
    }

    public static void clear() {
        MAP_KEY_FILTER.set(new HashMap<>());
        MAP_CLASS_FILTER.set(new HashMap<>());
        MAP_CLASS_FILTER_KEY.set(new HashMap<>());
    }

    @SuppressWarnings("unchecked")
    public static boolean checkClassForNewFilters(final Object object) {
        if (object == null) {
            return false;
        }
        boolean newFilter = false;
        final ClassFieldDescription fieldDescription = ClassUtils.getFieldDescription(Class.class.isAssignableFrom(object.getClass()) ? (Class<?>) object : object.getClass());
        for (FieldDescription child : fieldDescription.getChildren()) {
            ClassFieldDescription field = (ClassFieldDescription) child;
            if (Filter.class.isAssignableFrom((Class<?>) field.getType())) {
                newFilter |= FilterRegistry.registerNewFilter((Class<? extends Filter>) field.getType());
            }
        }
        return newFilter;
    }

    @SafeVarargs
    public static boolean registerNewFilter(final Class<? extends Filter>... filterConfig) {
        Map<Class<? extends Filter>, Filter> classFilterMap = MAP_CLASS_FILTER.get();
        if (Arrays.stream(filterConfig).filter(newFilter -> classFilterMap.get(newFilter) == null).findFirst().isEmpty()) {
            // all known
            return false;
        }
        Map<String, Filter> oldKeyFilterMap = getKeyFilterMap();
        Map<Class<? extends Filter>, Filter> oldClassFilterMap = getClassFilterMap();
        Map<Class<? extends Filter>, String> oldClassFilterKEyMap = getClassFilterKeyMap();
        final HashMap<String, Filter> newKeyHashMap = new HashMap<>(oldKeyFilterMap);
        final HashMap<Class<? extends Filter>, Filter> newClassHashMap = new HashMap<>(oldClassFilterMap);
        final HashMap<Class<? extends Filter>, String> newClassKeyHashMap = new HashMap<>(oldClassFilterKEyMap);
        try {
            for (final Class<? extends Filter> aClass : filterConfig) {
                final Constructor<? extends Filter> constructor = aClass.getDeclaredConstructor();
                final Filter prototype = constructor.newInstance(); // needed to have access to non-static 'get("..")' initialsers as Interfaces cannot have statics
                final String key = prototype.getKey(); // instantiated for getting key and to check that the default constructor for the filter is declared
                newKeyHashMap.put(key, prototype);
                newClassHashMap.put(aClass, prototype);
                newClassKeyHashMap.put(aClass, key);
            }
        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new IllegalArgumentException("filter must be public and have a public default constructor", e);
        }
        // small trick to make HashMap (which is more performant) thread-safe with AtomicReference
        while (!MAP_KEY_FILTER.compareAndSet(oldKeyFilterMap, newKeyHashMap)) {
            // rare case for if the previous map has been modified meanwhile
            // retrieve and update with new filters - spin until this commit has been incorporated
            // advantage: penalty is on rare writes/registering new filters rather than on frequent reads
            oldKeyFilterMap = MAP_KEY_FILTER.get();
            newKeyHashMap.putAll(oldKeyFilterMap);
        }
        while (!MAP_CLASS_FILTER.compareAndSet(oldClassFilterMap, newClassHashMap)) {
            oldClassFilterMap = MAP_CLASS_FILTER.get();
            newClassHashMap.putAll(oldClassFilterMap);
        }
        while (!MAP_CLASS_FILTER_KEY.compareAndSet(oldClassFilterKEyMap, newClassKeyHashMap)) {
            oldClassFilterKEyMap = MAP_CLASS_FILTER_KEY.get();
            newClassKeyHashMap.putAll(oldClassFilterKEyMap);
        }
        return true;
    }
}
