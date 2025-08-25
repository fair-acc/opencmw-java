package io.opencmw.serialiser.spi;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;

import org.jetbrains.annotations.NotNull;

import sun.misc.Unsafe; // NOPMD NOSONAR - there is nothing better under the Sun

/**
 * Reflection-based direct access methods to Fields including those w/o necessary direct access from standard user-code.
 *
 * <p> N.B. While this can be (mis-)used in user-level code, this is primarily intended for library-use and performance related code
 * that needs to access protected fields e.g. in view of unit-tests, serialiser or cleaner/simpler static class initialisation.
 */
@SuppressWarnings("PMD") // purposeful reflection-based accesses, safe/tested use of direct memory access (via Unsafe class), 'short' primitive handling, etc.
public class Field implements AnnotatedElement {
    private static final Unsafe unsafe;
    static {
        // get an instance of the otherwise private 'Unsafe' class
        try {
            final java.lang.reflect.Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true); // NOSONAR
            unsafe = (Unsafe) field.get(null);
        } catch (NoSuchFieldException | SecurityException | IllegalAccessException e) {
            throw new SecurityException(e);
        }
    }
    private static final Field STRING_FIELD_VALUE = getField(String.class, "value");
    private static final Field STRING_FIELD_HASH = getField(String.class, "hash");
    private final java.lang.reflect.Field jdkField;
    private final Object staticBase;
    private final long fieldByteOffset;
    private final boolean invalidTypeCombination;
    private final String exceptionMessage;
    private final boolean primitive;
    private final Type genericType;
    private Annotation[] declaredAnnotations;

    Field(final java.lang.reflect.Field jdkField) {
        exceptionMessage = "cannot set static final primitive-typed variable " + jdkField.getType() + " " + jdkField.getName();
        this.jdkField = jdkField;
        this.genericType = jdkField.getGenericType();
        primitive = jdkField.getType().isPrimitive();
        invalidTypeCombination = isStatic() && isFinal() && primitive;

        if (isStatic()) {
            this.staticBase = unsafe.staticFieldBase(jdkField);
            this.fieldByteOffset = unsafe.staticFieldOffset(jdkField);
        } else {
            this.staticBase = null;
            this.fieldByteOffset = unsafe.objectFieldOffset(jdkField);
        }
    }

    /**
     * Generic access to field values for a given class instance reference.
     *
     * @param classReference class instance the data should be retrieved for.
     * @return the value object contained by this field. Primitives are automatically boxed.
     */
    public Object get(final Object classReference) {
        if (isPrimitive()) {
            if (getType() == boolean.class) {
                return getBoolean(classReference);
            } else if (getType() == byte.class) {
                return getByte(classReference);
            } else if (getType() == char.class) {
                return getChar(classReference);
            } else if (getType() == short.class) {
                return getShort(classReference);
            } else if (getType() == int.class) {
                return getInt(classReference);
            } else if (getType() == long.class) {
                return getLong(classReference);
            } else if (getType() == float.class) {
                return getFloat(classReference);
            } else if (getType() == double.class) {
                return getDouble(classReference);
            }
        }
        return unsafe.getObject(staticBase == null ? classReference : staticBase, fieldByteOffset);
    }

    /**
     * @param classReference class instance the data should be retrieved for.
     * @return the primitive value the field is pointing to for a given {@code classReference} instance.
     */
    public boolean getBoolean(final Object classReference) {
        if (isPrimitive()) {
            return unsafe.getBoolean(staticBase == null ? classReference : staticBase, fieldByteOffset);
        } else {
            return (boolean) unsafe.getObject(staticBase == null ? classReference : staticBase, fieldByteOffset);
        }
    }

    /**
     * @param classReference class instance the data should be retrieved for.
     * @return the primitive value the field is pointing to for a given {@code classReference} instance.
     */
    public byte getByte(final Object classReference) {
        if (isPrimitive()) {
            return unsafe.getByte(staticBase == null ? classReference : staticBase, fieldByteOffset);
        } else {
            return (byte) unsafe.getObject(staticBase == null ? classReference : staticBase, fieldByteOffset);
        }
    }

    /**
     * @param classReference class instance the data should be retrieved for.
     * @return the primitive value the field is pointing to for a given {@code classReference} instance.
     */
    public char getChar(final Object classReference) {
        if (isPrimitive()) {
            return unsafe.getChar(staticBase == null ? classReference : staticBase, fieldByteOffset);
        } else {
            return (char) unsafe.getObject(staticBase == null ? classReference : staticBase, fieldByteOffset);
        }
    }

    /**
     * @return the {@code Class} object representing the class or interface that declares this {@code Field} object.
     */
    public final Class<?> getDeclaringClass() {
        return jdkField.getDeclaringClass();
    }

    /**
     * @param classReference class instance the data should be retrieved for.
     * @return the primitive value the field is pointing to for a given {@code classReference} instance.
     */
    public double getDouble(final Object classReference) {
        if (isPrimitive()) {
            return unsafe.getDouble(staticBase == null ? classReference : staticBase, fieldByteOffset);
        } else {
            return (double) unsafe.getObject(staticBase == null ? classReference : staticBase, fieldByteOffset);
        }
    }

    /**
     *
     * @return the JDK-based field description
     * @deprecated shouldn't ideally be used in end-user-code and restraint to well-tested/encapsulated library code
     */
    @Deprecated(since = "do no use direct access to reflection but the other equivalent direct methods")
    public final java.lang.reflect.Field getJdkField() {
        return jdkField;
    }

    /**
     * @param classReference class instance the data should be retrieved for.
     * @return the primitive value the field is pointing to for a given {@code classReference} instance.
     */
    public float getFloat(final Object classReference) {
        if (isPrimitive()) {
            return unsafe.getFloat(staticBase == null ? classReference : staticBase, fieldByteOffset);
        } else {
            return (float) unsafe.getObject(staticBase == null ? classReference : staticBase, fieldByteOffset);
        }
    }

    /**
     * @return {@code Type} object that represents the declared type for this {@code Field} object.
     *
     * <p>If the {@code Type} is a parameterized type, the {@code Type} object returned must accurately reflect the
     * actual type parameters used in the source code.
     *
     * <p>If the type of the underlying field is a type variable or a parameterized type, it is created. Otherwise, it is resolved.
     */
    public final Type getGenericType() {
        return genericType;
    }

    /**
     * @param classReference class instance the data should be retrieved for.
     * @return the primitive value the field is pointing to for a given {@code classReference} instance.
     */
    public int getInt(final Object classReference) {
        if (isPrimitive()) {
            return unsafe.getInt(staticBase == null ? classReference : staticBase, fieldByteOffset);
        } else {
            return (int) unsafe.getObject(staticBase == null ? classReference : staticBase, fieldByteOffset);
        }
    }

    /**
     * @param classReference class instance the data should be retrieved for.
     * @return the primitive value the field is pointing to for a given {@code classReference} instance.
     */
    public long getLong(final Object classReference) {
        if (isPrimitive()) {
            return unsafe.getLong(staticBase == null ? classReference : staticBase, fieldByteOffset);
        } else {
            return (long) unsafe.getObject(staticBase == null ? classReference : staticBase, fieldByteOffset);
        }
    }

    /**
     * @return the Java language modifiers for the field represented by this {@code Field} object, as an integer.
     * The {@code Modifier} class should be used to decode the modifiers.
     */
    public final int getModifiers() {
        return jdkField.getModifiers();
    }

    /**
     * @return the name of the field represented by this {@code Field} object.
     */
    public final String getName() {
        return jdkField.getName();
    }

    /**
     * @param classReference class instance the data should be retrieved for.
     * @return the primitive value the field is pointing to for a given {@code classReference} instance.
     */
    public short getShort(final Object classReference) {
        if (isPrimitive()) {
            return unsafe.getShort(staticBase == null ? classReference : staticBase, fieldByteOffset);
        } else {
            return (short) unsafe.getObject(staticBase == null ? classReference : staticBase, fieldByteOffset);
        }
    }

    /**
     * @return {@code Class} object that identifies the declared type for this {@code Field} object.
     */
    public final Class<?> getType() {
        return jdkField.getType();
    }

    /**
     * @return {@code true} if the field is defined with the {@code abstract} modifier, {@code false} otherwise.
     */
    public final boolean isAbstract() {
        return Modifier.isAbstract(jdkField.getModifiers());
    }

    /**
     * @return {@code true} if the field is defined with the {@code final} modifier, {@code false} otherwise.
     */
    public final boolean isFinal() {
        return Modifier.isFinal(jdkField.getModifiers());
    }

    /**
     * @return @return {@code true} if the field is defined with the {@code native} modifier, {@code false} otherwise.
     */
    public final boolean isNative() {
        return Modifier.isNative(jdkField.getModifiers());
    }

    /**
     * @return {@code true} if the field is defined with the {@code private} modifier, {@code false} otherwise.
     */
    public final boolean isPackagePrivate() {
        return !isPrivate() && !isProtected() && !isPublic();
    }

    /**
     * @return {@code true} if the field is a primitive (e.g. boolean, int, .., float, double value, {@code false} otherwise.
     */
    public final boolean isPrimitive() {
        return primitive;
    }

    /**
     * @return {@code true} if the field is defined with the {@code private} modifier, {@code false} otherwise.
     */
    public final boolean isPrivate() {
        return Modifier.isPrivate(jdkField.getModifiers());
    }

    /**
     * @return {@code true} if the field is defined with the {@code protected} modifier, {@code false} otherwise.
     */
    public final boolean isProtected() {
        return Modifier.isProtected(jdkField.getModifiers());
    }

    /**
     * @return {@code true} if the field is defined with the {@code public} modifier, {@code false} otherwise.
     */
    public final boolean isPublic() {
        return Modifier.isPublic(jdkField.getModifiers());
    }

    /**
     * @return {@code true} if the field is defined with the {@code static} modifier, {@code false} otherwise.
     */
    public final boolean isStatic() {
        return Modifier.isStatic(jdkField.getModifiers());
    }

    /**
     * @return {@code true} if the field is defined with the {@code strictfp} modifier, {@code false} otherwise.
     */
    public final boolean isStrict() {
        return Modifier.isStrict(jdkField.getModifiers());
    }

    /**
     * @return {@code true} if the field is defined with the {@code synchronised} modifier, {@code false} otherwise.
     */
    public final boolean isSynchronized() {
        return Modifier.isSynchronized(jdkField.getModifiers());
    }

    /**
     * @return {@code true} if the field is defined with the {@code transient} modifier, {@code false} otherwise.
     */
    public final boolean isTransient() {
        return Modifier.isTransient(jdkField.getModifiers());
    }

    /**
     * @return {@code true} if the field is defined with the {@code volatile} modifier, {@code false} otherwise.
     */
    public final boolean isVolatile() {
        return Modifier.isVolatile(jdkField.getModifiers());
    }

    /**
     * Stores a reference Object value into a given Java variable.
     *
     * @param classReference classReference class instance the data should be retrieved for.
     * @param obj the Object value the field is pointing to for a given {@code classReference} instance.
     */
    public void set(final Object classReference, final Object obj) {
        unsafe.putObject(staticBase == null ? classReference : staticBase, fieldByteOffset, obj);
    }

    /**
     * Convert a String value and stores the result into a given Java variable.
     * Supported are Strings, primitive and boxed Numbers
     *
     * @param classReference classReference class instance the data should be retrieved for.
     * @param srcString the string to be converted to the field value.
     * @throws NumberFormatException if the string does not contain a parsable number.
     * @throws IllegalArgumentException if string cannot be converted to the field type.
     */
    public void set(final Object classReference, @NotNull final String srcString) { // NOSONAR - complexity is inherent to lack of generics for primitive types
        final Class<?> type = getType();
        if (type == String.class) {
            unsafe.putObject(staticBase == null ? classReference : staticBase, fieldByteOffset, srcString);
            return;
        }
        try {
            if (type == boolean.class || type == Boolean.class) {
                setBoolean(classReference, Boolean.parseBoolean(srcString));
                return;
            } else if (type == byte.class || type == Byte.class) {
                setByte(classReference, Byte.parseByte(srcString));
                return;
            } else if (type == char.class || type == Character.class) {
                setChar(classReference, srcString.isBlank() ? 0 : srcString.charAt(0));
                return;
            } else if (type == short.class || type == Short.class) {
                setShort(classReference, Short.parseShort(srcString));
                return;
            } else if (type == int.class || type == Integer.class) {
                setInt(classReference, Integer.parseInt(srcString));
                return;
            } else if (type == long.class || type == Long.class) {
                setLong(classReference, Long.parseLong(srcString));
                return;
            } else if (type == float.class || type == Float.class) {
                setFloat(classReference, Float.parseFloat(srcString));
                return;
            } else if (type == double.class || type == Double.class) {
                setDouble(classReference, Double.parseDouble(srcString));
                return;
            }
        } catch (NumberFormatException e) {
            throw new NumberFormatException("cannot cast String '" + srcString + "' to field type '" + type + "' of " + this.getName());
        }
        throw new IllegalArgumentException("cannot cast String '" + srcString + "' to field type '" + type + "' of " + this.getName());
    }

    /**
     * Sets primitive or boxed value to field for a given {@code classReference}
     *
     * @param classReference class instance the data should be retrieved for.
     * @param value the primitive value to be set.
     */
    public void setBoolean(final Object classReference, final boolean value) {
        if (isPrimitive()) {
            guardAgainstIllegalStaticFinalPrimitiveAccess();
            unsafe.putBoolean(staticBase == null ? classReference : staticBase, fieldByteOffset, value);
        } else {
            unsafe.putObject(staticBase == null ? classReference : staticBase, fieldByteOffset, value);
        }
    }

    /**
     * Sets primitive or boxed value to field for a given {@code classReference}
     *
     * @param classReference class instance the data should be retrieved for.
     * @param value the primitive value to be set.
     */
    public void setByte(final Object classReference, final byte value) {
        if (isPrimitive()) {
            guardAgainstIllegalStaticFinalPrimitiveAccess();
            unsafe.putByte(staticBase == null ? classReference : staticBase, fieldByteOffset, value);
        } else {
            unsafe.putObject(staticBase == null ? classReference : staticBase, fieldByteOffset, value);
        }
    }

    /**
     * Sets primitive or boxed value to field for a given {@code classReference}
     *
     * @param classReference class instance the data should be retrieved for.
     * @param value the primitive value to be set.
     */
    public void setChar(final Object classReference, final char value) {
        if (isPrimitive()) {
            guardAgainstIllegalStaticFinalPrimitiveAccess();
            unsafe.putChar(staticBase == null ? classReference : staticBase, fieldByteOffset, value);
        } else {
            unsafe.putObject(staticBase == null ? classReference : staticBase, fieldByteOffset, value);
        }
    }

    /**
     * Sets primitive or boxed value to field for a given {@code classReference}
     *
     * @param classReference class instance the data should be retrieved for.
     * @param value the primitive value to be set.
     */
    public void setDouble(final Object classReference, final double value) {
        if (isPrimitive()) {
            guardAgainstIllegalStaticFinalPrimitiveAccess();
            unsafe.putDouble(staticBase == null ? classReference : staticBase, fieldByteOffset, value);
        } else {
            unsafe.putObject(staticBase == null ? classReference : staticBase, fieldByteOffset, value);
        }
    }

    /**
     * Sets primitive or boxed value to field for a given {@code classReference}
     *
     * @param classReference class instance the data should be retrieved for.
     * @param value the primitive value to be set.
     */
    public void setFloat(final Object classReference, final float value) {
        if (isPrimitive()) {
            guardAgainstIllegalStaticFinalPrimitiveAccess();
            unsafe.putFloat(staticBase == null ? classReference : staticBase, fieldByteOffset, value);
        } else {
            unsafe.putObject(staticBase == null ? classReference : staticBase, fieldByteOffset, value);
        }
    }

    /**
     * Sets primitive or boxed value to field for a given {@code classReference}
     *
     * @param classReference class instance the data should be retrieved for.
     * @param value the primitive value to be set.
     */
    public void setInt(final Object classReference, final int value) {
        if (isPrimitive()) {
            guardAgainstIllegalStaticFinalPrimitiveAccess();
            unsafe.putInt(staticBase == null ? classReference : staticBase, fieldByteOffset, value);
        } else {
            unsafe.putObject(staticBase == null ? classReference : staticBase, fieldByteOffset, value);
        }
    }

    /**
     * Sets primitive or boxed value to field for a given {@code classReference}
     *
     * @param classReference class instance the data should be retrieved for.
     * @param value the primitive value to be set.
     */
    public void setLong(final Object classReference, final long value) {
        if (isPrimitive()) {
            guardAgainstIllegalStaticFinalPrimitiveAccess();
            unsafe.putLong(staticBase == null ? classReference : staticBase, fieldByteOffset, value);
        } else {
            unsafe.putObject(staticBase == null ? classReference : staticBase, fieldByteOffset, value);
        }
    }

    /**
     * Sets primitive or boxed value to field for a given {@code classReference}
     *
     * @param classReference class instance the data should be retrieved for.
     * @param value the primitive value to be set.
     */
    public void setShort(final Object classReference, final short value) {
        if (isPrimitive()) {
            guardAgainstIllegalStaticFinalPrimitiveAccess();
            unsafe.putShort(staticBase == null ? classReference : staticBase, fieldByteOffset, value);
        } else {
            unsafe.putObject(staticBase == null ? classReference : staticBase, fieldByteOffset, value);
        }
    }

    @Override
    public <T extends Annotation> T getAnnotation(@NotNull final Class<T> annotationClass) {
        return jdkField.getAnnotation(annotationClass);
    }

    @Override
    public final Annotation @NotNull[] getAnnotations() {
        return getDeclaredAnnotations();
    }

    @Override
    public Annotation @NotNull[] getDeclaredAnnotations() {
        if (declaredAnnotations == null) {
            declaredAnnotations = jdkField.getDeclaredAnnotations();
        }
        return declaredAnnotations;
    }

    /**
     * @throws IllegalArgumentException in case the field is of a static final primitive-type (N.B. this are inlined/truly hardcoded by the java compiler
     */
    private void guardAgainstIllegalStaticFinalPrimitiveAccess() {
        if (invalidTypeCombination) {
            throw new IllegalArgumentException(exceptionMessage);
        }
    }

    public static Field getField(@NotNull final Class<?> clazz, @NotNull final String fieldName) {
        try {
            return new Field(clazz.getDeclaredField(fieldName));
        } catch (NoSuchFieldException e) {
            throw new IllegalArgumentException("class " + clazz + " does not contain field named '" + fieldName + "'", e);
        }
    }

    /**
     * Resets the internal value and hash of String. This is useful for globally unique String parameter or to
     * reset Strings that may contain, for example, plain-text passwords that otherwise could be recuperated via the gc.
     *
     * @param oldString the old String handle (should be globally unique)
     * @param newValue the new String value
     */
    public static void resetString(@NotNull final String oldString, @NotNull final String newValue) {
        final byte[] newValueBytes = newValue.getBytes(StandardCharsets.UTF_8);
        STRING_FIELD_VALUE.set(oldString, newValueBytes);
        STRING_FIELD_HASH.setInt(oldString, 0); // forces to recompute hash once needed
    }
}
