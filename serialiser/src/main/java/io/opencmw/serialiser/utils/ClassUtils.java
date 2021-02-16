package io.opencmw.serialiser.utils;

import java.lang.reflect.Array;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.nio.CharBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opencmw.serialiser.spi.ClassFieldDescription;

@SuppressWarnings("PMD.UseConcurrentHashMap")
public final class ClassUtils { //NOPMD nomen est omen
    private static final Logger LOGGER = LoggerFactory.getLogger(ClassUtils.class);
    // some helper declarations
    private static final Map<Class<?>, Class<?>> PRIMITIVE_WRAPPER_MAP = new HashMap<>();
    private static final Map<Class<?>, Class<?>> WRAPPER_PRIMITIVE_MAP = new HashMap<>();
    private static final Map<Class<?>, Class<?>> PRIMITIVE_ARRAY_BOXED_MAP = new HashMap<>();
    private static final Map<Class<?>, Class<?>> BOXED_ARRAY_PRIMITIVE_MAP = new HashMap<>();
    public static final Map<Class<?>, String> DO_NOT_PARSE_MAP = new HashMap<>(); // NOPMD should not be a threading issue - static one-time/init write, multiple reads afterwards are safe
    private static final Map<Integer, ClassFieldDescription> CLASS_FIELD_DESCRIPTION_MAP = new ConcurrentHashMap<>();
    private static final Map<String, Class<?>> CLASS_STRING_MAP = new ConcurrentHashMap<>();
    private static final Map<Class<?>, Map<String, Method>> CLASS_METHOD_MAP = new ConcurrentHashMap<>();
    private static int indentationNumberOfSpace = 4;
    private static int maxRecursionDepth = 10;

    static {
        // primitive types
        add(WRAPPER_PRIMITIVE_MAP, PRIMITIVE_WRAPPER_MAP, Boolean.class, boolean.class);
        add(WRAPPER_PRIMITIVE_MAP, PRIMITIVE_WRAPPER_MAP, Byte.class, byte.class);
        add(WRAPPER_PRIMITIVE_MAP, PRIMITIVE_WRAPPER_MAP, Character.class, char.class);
        add(WRAPPER_PRIMITIVE_MAP, PRIMITIVE_WRAPPER_MAP, Short.class, short.class);
        add(WRAPPER_PRIMITIVE_MAP, PRIMITIVE_WRAPPER_MAP, Integer.class, int.class);
        add(WRAPPER_PRIMITIVE_MAP, PRIMITIVE_WRAPPER_MAP, Long.class, long.class);
        add(WRAPPER_PRIMITIVE_MAP, PRIMITIVE_WRAPPER_MAP, Float.class, float.class);
        add(WRAPPER_PRIMITIVE_MAP, PRIMITIVE_WRAPPER_MAP, Double.class, double.class);
        add(WRAPPER_PRIMITIVE_MAP, PRIMITIVE_WRAPPER_MAP, Void.class, void.class);
        add(WRAPPER_PRIMITIVE_MAP, PRIMITIVE_WRAPPER_MAP, String.class, String.class);

        // primitive arrays
        add(PRIMITIVE_ARRAY_BOXED_MAP, BOXED_ARRAY_PRIMITIVE_MAP, boolean[].class, Boolean[].class);
        add(PRIMITIVE_ARRAY_BOXED_MAP, BOXED_ARRAY_PRIMITIVE_MAP, byte[].class, Byte[].class);
        add(PRIMITIVE_ARRAY_BOXED_MAP, BOXED_ARRAY_PRIMITIVE_MAP, char[].class, Character[].class);
        add(PRIMITIVE_ARRAY_BOXED_MAP, BOXED_ARRAY_PRIMITIVE_MAP, short[].class, Short[].class);
        add(PRIMITIVE_ARRAY_BOXED_MAP, BOXED_ARRAY_PRIMITIVE_MAP, int[].class, Integer[].class);
        add(PRIMITIVE_ARRAY_BOXED_MAP, BOXED_ARRAY_PRIMITIVE_MAP, long[].class, Long[].class);
        add(PRIMITIVE_ARRAY_BOXED_MAP, BOXED_ARRAY_PRIMITIVE_MAP, float[].class, Float[].class);
        add(PRIMITIVE_ARRAY_BOXED_MAP, BOXED_ARRAY_PRIMITIVE_MAP, double[].class, Double[].class);
        add(PRIMITIVE_ARRAY_BOXED_MAP, BOXED_ARRAY_PRIMITIVE_MAP, String[].class, String[].class);

        // boxed arrays

        // do not parse following classes
        DO_NOT_PARSE_MAP.put(Class.class, "private java implementation");
        DO_NOT_PARSE_MAP.put(Thread.class, "recursive definitions"); // NOPMD - not an issue/not a use within a J2EE context
        DO_NOT_PARSE_MAP.put(AtomicBoolean.class, "does not like to be parsed");
    }
    private ClassUtils() {
        // utility class
    }

    public static void checkArgument(boolean condition) {
        if (!condition) {
            throw new IllegalArgumentException();
        }
    }

    public static Class<?> getClassByName(final String name) {
        return CLASS_STRING_MAP.computeIfAbsent(name, key -> {
            try {
                return Class.forName(key);
            } catch (ClassNotFoundException | SecurityException e) {
                LOGGER.atError().setCause(e).addArgument(name).log("exception while getting class {}");
                return null;
            }
        });
    }

    public static Class<?> getClassByNameNonVerboseError(final String name) {
        return CLASS_STRING_MAP.computeIfAbsent(name, key -> {
            try {
                return Class.forName(key);
            } catch (ClassNotFoundException | SecurityException e) {
                return Object.class;
            }
        });
    }

    public static Map<Integer, ClassFieldDescription> getClassDescriptions() {
        return CLASS_FIELD_DESCRIPTION_MAP;
    }

    public static ClassFieldDescription getFieldDescription(final Class<?> clazz, final Class<?>... classArguments) {
        if (clazz == null) {
            throw new IllegalArgumentException("object must not be null");
        }
        return CLASS_FIELD_DESCRIPTION_MAP.computeIfAbsent(computeHashCode(clazz, classArguments),
                key -> new ClassFieldDescription(clazz, false));
    }

    public static int getIndentationNumberOfSpace() {
        return indentationNumberOfSpace;
    }

    public static Collection<ClassFieldDescription> getKnownClasses() {
        return CLASS_FIELD_DESCRIPTION_MAP.values();
    }

    public static Map<Class<?>, Map<String, Method>> getKnownMethods() {
        return CLASS_METHOD_MAP;
    }

    public static int getMaxRecursionDepth() {
        return maxRecursionDepth;
    }

    public static Method getMethod(final Class<?> clazz, final String methodName) {
        return CLASS_METHOD_MAP.computeIfAbsent(clazz, c -> new ConcurrentHashMap<>()).computeIfAbsent(methodName, name -> {
            try {
                return clazz.getMethod(methodName);
            } catch (NoSuchMethodException | SecurityException e) {
                return null;
            }
        });
    }

    public static Class<?> getRawType(Type type) {
        if (type instanceof Class<?>) {
            // type is a normal class.
            return (Class<?>) type;

        } else if (type instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) type;

            // unsure why getRawType() returns Type instead of Class.
            // possibly related to pathological case involving nested classes....
            Type rawType = parameterizedType.getRawType();
            checkArgument(rawType instanceof Class);
            return (Class<?>) rawType;

        } else if (type instanceof GenericArrayType) {
            Type componentType = ((GenericArrayType) type).getGenericComponentType();
            return Array.newInstance(getRawType(componentType), 0).getClass();

        } else if (type instanceof TypeVariable) {
            // we could use the variable's bounds, but that won't work if there are multiple.
            // having a raw type that's more general than necessary is okay
            return Object.class;

        } else if (type instanceof WildcardType) {
            return getRawType(((WildcardType) type).getUpperBounds()[0]);

        } else {
            String className = type == null ? "null" : type.getClass().getName();
            throw new IllegalArgumentException("Expected a Class, ParameterizedType, or "
                                               + "GenericArrayType, but <" + type + "> is of type " + className);
        }
    }

    public static Type[] getSecondaryType(final Type type) {
        if (type instanceof ParameterizedType) {
            return ((ParameterizedType) type).getActualTypeArguments();
        }
        return new Type[0];
    }

    public static boolean isBoxedArray(final Class<?> type) {
        return BOXED_ARRAY_PRIMITIVE_MAP.containsKey(type);
    }

    public static boolean isPrimitiveArray(final Class<?> type) {
        return PRIMITIVE_ARRAY_BOXED_MAP.containsKey(type);
    }

    public static boolean isPrimitiveOrString(final Class<?> type) {
        if (type == null) {
            return false;
        }
        return type.isPrimitive() || String.class.isAssignableFrom(type);
    }

    public static boolean isPrimitiveOrWrapper(final Class<?> type) {
        if (type == null) {
            return false;
        }
        return type.isPrimitive() || isPrimitiveWrapper(type);
    }

    public static boolean isPrimitiveWrapper(final Class<?> type) {
        return WRAPPER_PRIMITIVE_MAP.containsKey(type);
    }

    public static boolean isPrimitiveWrapperOrString(final Class<?> type) {
        if (type == null) {
            return false;
        }
        return WRAPPER_PRIMITIVE_MAP.containsKey(type) || String.class.isAssignableFrom(type);
    }

    public static Class<?> primitiveToWrapper(final Class<?> cls) {
        Class<?> convertedClass = cls;
        if (cls != null && cls.isPrimitive()) {
            convertedClass = PRIMITIVE_WRAPPER_MAP.get(cls);
        }
        return convertedClass;
    }

    public static void setIndentationNumberOfSpace(final int indentationNumberOfSpace) {
        ClassUtils.indentationNumberOfSpace = indentationNumberOfSpace;
    }

    public static void setMaxRecursionDepth(final int maxRecursionDepth) {
        ClassUtils.maxRecursionDepth = maxRecursionDepth;
    }

    public static String spaces(final int spaces) {
        return CharBuffer.allocate(spaces).toString().replace('\0', ' ');
    }

    public static String translateClassName(final String name) {
        if (name.startsWith("[Z")) {
            return boolean[].class.getName();
        } else if (name.startsWith("[B")) {
            return byte[].class.getName();
        } else if (name.startsWith("[S")) {
            return short[].class.getName();
        } else if (name.startsWith("[I")) {
            return int[].class.getName();
        } else if (name.startsWith("[J")) {
            return long[].class.getName();
        } else if (name.startsWith("[F")) {
            return float[].class.getName();
        } else if (name.startsWith("[D")) {
            return double[].class.getName();
        } else if (name.startsWith("[L")) {
            return name.substring(2, name.length() - 1) + "[]";
        }

        return name;
    }

    private static void add(Map<Class<?>, Class<?>> map1, Map<Class<?>, Class<?>> map2, Class<?> obj1, Class<?> obj2) {
        map1.put(obj1, obj2);
        map2.put(obj2, obj1);
    }

    private static int computeHashCode(final Class<?> classPrototype, final Class<?>... classArguments) {
        final int prime = 31;
        int result = 1;
        result = (prime * result) + ((classPrototype == null) ? 0 : classPrototype.getName().hashCode());
        if ((classArguments == null) || (classArguments.length <= 0)) {
            return result;
        }

        for (final Class<?> clazz : classArguments) {
            result = (prime * result) + clazz.hashCode();
        }

        return result;
    }
}
