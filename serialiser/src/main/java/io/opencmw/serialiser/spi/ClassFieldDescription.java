package io.opencmw.serialiser.spi;

import java.lang.reflect.*;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opencmw.serialiser.DataType;
import io.opencmw.serialiser.FieldDescription;
import io.opencmw.serialiser.FieldSerialiser;
import io.opencmw.serialiser.annotations.*;
import io.opencmw.serialiser.annotations.Modifier;
import io.opencmw.serialiser.utils.ClassUtils;

/**
 * @author rstein
 */
@SuppressWarnings({ "PMD.ExcessivePublicCount", "PMD.TooManyFields" }) // utility class for safe reflection handling
public class ClassFieldDescription implements FieldDescription {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClassFieldDescription.class);
    private final int hierarchyDepth;
    private final Field field;
    private final String fieldName;
    private final String fieldNameRelative;
    private final String fieldUnit;
    private final String fieldQuantity;
    private final String fieldDescription;
    private final byte fieldModifier;
    private final boolean annotationPresent;
    private final ClassFieldDescription parent;
    private final List<FieldDescription> children = new ArrayList<>();
    private final Class<?> classType;
    private final DataType dataType;
    private final String typeName;
    private final String typeNameSimple;
    private final int modifierID;
    private final boolean modPublic;
    private final boolean modProtected;
    private final boolean modPrivate;
    // field modifier in canonical order
    private final boolean modAbstract;
    private final boolean modStatic;
    private final boolean modFinal;
    private final boolean modTransient;
    private final boolean modVolatile;
    private final boolean modSynchronized;
    private final boolean modNative;
    private final boolean modStrict;
    private final boolean modInterface;
    // additional qualities
    private final boolean isPrimitiveType;
    private final boolean isClassType;
    private final boolean isEnumType;
    private final List<?> enumDefinitions;
    private final boolean serializable;
    private String toStringName; // computed on demand and cached
    private Type genericType; // computed on demand and cached
    private List<Type> genericTypeList; // computed on demand and cached
    private List<String> genericTypeNameList; // computed on demand and cached
    private String genericTypeNames; // computed on demand and cached
    private String genericTypeNamesSimple; // computed on demand and cached
    private String modifierStr; // computed on demand and cached
    // serialiser info
    private FieldSerialiser<?> fieldSerialiser;

    /**
     * This should be called only once with the root class as an argument
     *
     * @param referenceClass the root node containing further Field children
     * @param fullScan {@code true} if the class field should be serialised according to {@link java.io.Serializable}
     *        (ie. object's non-static and non-transient fields); {@code false} otherwise.
     */
    public ClassFieldDescription(final Class<?> referenceClass, final boolean fullScan) {
        this(referenceClass, null, null, 0);
        if (referenceClass == null) {
            throw new IllegalArgumentException("object must not be null");
        }

        genericType = classType.getGenericSuperclass();

        // parse object
        exploreClass(classType, this, 0, fullScan);
    }

    protected ClassFieldDescription(final Class<?> referenceClass, final Field field, final ClassFieldDescription parent, final int recursionLevel) {
        super();
        hierarchyDepth = recursionLevel;
        this.parent = parent;

        if (referenceClass == null) {
            this.field = Objects.requireNonNull(field, "field must not be null");
            classType = field.getType();
            fieldName = field.getName().intern();
            fieldNameRelative = this.parent == null ? fieldName : (this.parent.getFieldNameRelative() + "." + fieldName).intern();

            modifierID = field.getModifiers();
            dataType = DataType.fromClassType(classType);
        } else {
            this.field = null; // NOPMD it's a root, no field definition available
            classType = referenceClass;
            fieldName = classType.getName().intern();
            fieldNameRelative = fieldName;

            modifierID = classType.getModifiers();
            dataType = DataType.START_MARKER;
        }

        // read annotation values
        AnnotatedElement annotatedElement = field == null ? referenceClass : field;
        fieldUnit = getFieldUnit(annotatedElement);
        fieldQuantity = getFieldQuantity(annotatedElement);
        fieldDescription = getFieldDescription(annotatedElement);
        fieldModifier = getFieldModifier(annotatedElement);

        annotationPresent = fieldUnit != null || fieldQuantity != null || fieldDescription != null;

        typeName = ClassUtils.translateClassName(classType.getTypeName()).intern();
        final int lastDot = typeName.lastIndexOf('.');
        typeNameSimple = typeName.substring(lastDot < 0 ? 0 : lastDot + 1);

        modPublic = java.lang.reflect.Modifier.isPublic(modifierID);
        modProtected = java.lang.reflect.Modifier.isProtected(modifierID);
        modPrivate = java.lang.reflect.Modifier.isPrivate(modifierID);

        modAbstract = java.lang.reflect.Modifier.isAbstract(modifierID);
        modStatic = java.lang.reflect.Modifier.isStatic(modifierID);
        modFinal = java.lang.reflect.Modifier.isFinal(modifierID);
        modTransient = java.lang.reflect.Modifier.isTransient(modifierID);
        modVolatile = java.lang.reflect.Modifier.isVolatile(modifierID);
        modSynchronized = java.lang.reflect.Modifier.isSynchronized(modifierID);
        modNative = java.lang.reflect.Modifier.isNative(modifierID);
        modStrict = java.lang.reflect.Modifier.isStrict(modifierID);
        modInterface = classType.isInterface();

        // additional fields
        isPrimitiveType = classType.isPrimitive();
        isClassType = !isPrimitiveType && !modInterface;
        isEnumType = Enum.class.isAssignableFrom(classType);
        if (isEnumType) {
            enumDefinitions = List.of(classType.getEnumConstants());
        } else {
            enumDefinitions = Collections.emptyList();
        }
        serializable = !modTransient && !modStatic;
    }

    /**
     * This should be called for individual class field members
     *
     * @param field Field reference for the given class member
     * @param parent pointer to the root/parent reference class field description
     * @param recursionLevel hierarchy level (i.e. '0' being the root class, '1' the sub-class etc.
     * @param fullScan {@code true} if the class field should be serialised according to {@link java.io.Serializable}
     *        (ie. object's non-static and non-transient fields); {@code false} otherwise.
     */
    public ClassFieldDescription(@NotNull final Field field, final ClassFieldDescription parent, final int recursionLevel, final boolean fullScan) {
        this(null, field, parent, recursionLevel);

        // add child to parent if it serializable or if a full scan is requested
        if (this.parent != null && (serializable || fullScan)) {
            this.parent.getChildren().add(this);
        }
    }

    public Object allocateMemberClassField(final Object fieldParent) {
        try {
            // need to allocate new class object
            final Object newFieldObj;
            if (this.classType.getDeclaringClass() == null || java.lang.reflect.Modifier.isStatic(this.classType.getModifiers())) {
                final Constructor<?> constr = this.classType.getDeclaredConstructor();
                newFieldObj = constr.newInstance();
            } else {
                final Constructor<?> constr = this.classType.getDeclaredConstructor(fieldParent.getClass());
                newFieldObj = constr.newInstance(fieldParent);
            }
            this.getField().set(fieldParent, newFieldObj);

            return newFieldObj;
        } catch (InstantiationException | InvocationTargetException | SecurityException | NoSuchMethodException | IllegalAccessException e) {
            LOGGER.atError().setCause(e).log("error initialising inner class object");
        }
        return null;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof FieldDescription other)) {
            return false;
        }
        if (this.getDataType() != other.getDataType()) {
            return false;
        }

        return this.getFieldName().equals(other.getFieldName());
    }

    @Override
    public FieldDescription findChildField(final String fieldName) {
        for (final FieldDescription child : children) {
            final String name = child.getFieldName();
            if (name.equals(fieldName)) { // NOSONAR NOPMD early return if the same String object reference
                return child;
            }
        }
        return null;
    }

    /**
     * @return generic type argument name of the class (e.g. for List&lt;String&gt; this would return
     *         'java.lang.String')
     */
    public List<String> getActualTypeArgumentNames() {
        if (genericTypeNameList == null) {
            genericTypeNameList = getActualTypeArguments().stream().map(Type::getTypeName).collect(Collectors.toList());
        }

        return genericTypeNameList;
    }

    /**
     * @return generic type argument objects of the class (e.g. for List&lt;String&gt; this would return 'String.class')
     */
    public List<Type> getActualTypeArguments() {
        if (genericTypeList == null) {
            genericTypeList = new ArrayList<>();
            if ((field == null) || (getGenericType() == null) || !(getGenericType() instanceof ParameterizedType)) {
                return genericTypeList;
            }
            genericTypeList.addAll(Arrays.asList(((ParameterizedType) getGenericType()).getActualTypeArguments()));
        }

        return genericTypeList;
    }

    /**
     * @return the children (if any) from the super classes
     */
    @Override
    public List<FieldDescription> getChildren() {
        return children;
    }

    @Override
    public int getDataSize() {
        return 0;
    }

    @Override
    public int getDataStartOffset() {
        return 0;
    }

    @Override
    public int getDataStartPosition() {
        return 0;
    }

    /**
     * @return the DataType (if known) for the detected Field, {@link DataType#OTHER} in all other cases
     */
    @Override
    public DataType getDataType() {
        return dataType;
    }

    /**
     * @return possible Enum definitions, see also 'isEnum()'
     */
    public List<?> getEnumConstants() {
        return enumDefinitions;
    }

    /**
     * @return the underlying Field type or {@code null} if it's a root node
     */
    public Field getField() {
        return field;
    }

    @Override
    public String getFieldDescription() {
        return fieldDescription;
    }

    @Override
    public byte getFieldModifier() {
        return fieldModifier;
    }

    /**
     * @return the underlying field name
     */
    @Override
    public String getFieldName() {
        return fieldName;
    }

    /**
     * @return relative field name within class hierarchy (ie. field_level0.field_level1.variable_0)
     */
    public String getFieldNameRelative() {
        return fieldNameRelative;
    }

    public FieldSerialiser<?> getFieldSerialiser() {
        return fieldSerialiser;
    }

    @Override
    public int getFieldStart() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public String getFieldUnit() {
        return fieldUnit;
    }

    @Override
    public String getFieldQuantity() {
        return fieldQuantity;
    }

    /**
     * @return field type strings (e.g. for the class Map&lt;Integer,String&gt; this returns
     *         '&lt;java.lang.Integer,java.lang.String&gt;'
     */
    public String getGenericFieldTypeString() {
        if (genericTypeNames == null) {
            if (getActualTypeArgumentNames().isEmpty()) {
                genericTypeNames = "";
            } else {
                genericTypeNames = getActualTypeArgumentNames().stream().collect(Collectors.joining(", ", "<", ">")).intern();
            }
        }
        return genericTypeNames;
    }

    /**
     * @return field type strings (e.g. for the class Map&lt;Integer,String&gt; this returns
     *         '&lt;java.lang.Integer,java.lang.String&gt;'
     */
    public String getGenericFieldTypeStringSimple() {
        if (genericTypeNamesSimple == null) {
            if (getActualTypeArgumentNames().isEmpty()) {
                genericTypeNamesSimple = "";
            } else {
                genericTypeNamesSimple = getActualTypeArguments().stream() //
                                                 .map(t -> ClassUtils.translateClassName(t.getTypeName()))
                                                 .collect(Collectors.joining(", ", "<", ">"))
                                                 .intern();
            }
        }
        return genericTypeNamesSimple;
    }

    public Type getGenericType() {
        if (genericType == null) {
            genericType = field == null ? new Type() {
                @Override
                public String getTypeName() {
                    return "unknown type";
                }
            } : field.getGenericType();
        }
        return genericType;
    }

    /**
     * @return hierarchy level depth w.r.t. root object (ie. '0' being a variable in the root object)
     */
    public int getHierarchyDepth() {
        return hierarchyDepth;
    }

    /**
     * @return the modifierID
     */
    public int getModifierID() {
        return modifierID;
    }

    /**
     * @return the full modifier string (cached)
     */
    public String getModifierString() {
        if (modifierStr == null) {
            // initialise only on a need to basis
            // for performance reasons
            modifierStr = java.lang.reflect.Modifier.toString(modifierID).intern();
        }
        return modifierStr;
    }

    /**
     * @return the parent
     */
    @Override
    public FieldDescription getParent() {
        return parent;
    }

    /**
     * @param field class Field description for which
     * @param hierarchyLevel the recursion level of the parent (e.g. '1' yields the immediate parent, '2' the parent of
     *        the parent etc.)
     * @return the parent field reference description for the provided field
     */
    public FieldDescription getParent(final FieldDescription field, final int hierarchyLevel) {
        if (field == null) {
            throw new IllegalArgumentException("field is null at hierarchyLevel = " + hierarchyLevel);
        }
        if ((hierarchyLevel == 0) || field.getParent() == null) {
            return field;
        }
        return getParent(field.getParent(), hierarchyLevel - 1);
    }

    /**
     * @return field class type
     */
    @Override
    public Type getType() {
        return classType;
    }

    /**
     * @return field class type name
     */
    public String getTypeName() {
        return typeName;
    }

    /**
     * @return field class type name
     */
    public String getTypeNameSimple() {
        return typeNameSimple;
    }

    /**
     * @return the isAbstract
     */
    public boolean isAbstract() {
        return modAbstract;
    }

    @Override
    public boolean isAnnotationPresent() {
        return annotationPresent;
    }

    /**
     * @return the isClass
     */
    public boolean isClass() {
        return isClassType;
    }

    /**
     * @return whether class is an Enum type
     */
    public boolean isEnum() {
        return isEnumType;
    }

    /**
     * @return {@code true} if the class field includes the {@code final} modifier; {@code false} otherwise.
     */
    public boolean isFinal() {
        return modFinal;
    }

    /**
     * @return {@code true} if the class field is an interface
     */
    public boolean isInterface() {
        return modInterface;
    }

    /**
     * @return the isNative
     */
    public boolean isNative() {
        return modNative;
    }

    /**
     * @return {@code true} if the class field is a primitive type (ie. boolean, byte, ..., int, float, double)
     */
    public boolean isPrimitive() {
        return isPrimitiveType;
    }

    /**
     * @return {@code true} if the class field includes the {@code private} modifier; {@code false} otherwise.
     */
    public boolean isPrivate() {
        return modPrivate;
    }

    /**
     * @return the isProtected
     */
    public boolean isProtected() {
        return modProtected;
    }

    /**
     * @return {@code true} if the class field includes the {@code public} modifier; {@code false} otherwise.
     */
    public boolean isPublic() {
        return modPublic;
    }

    /**
     * @return the isRoot
     */
    public boolean isRoot() {
        return hierarchyDepth == 0;
    }

    /**
     * @return {@code true} if the class field should be serialised according to {@link java.io.Serializable} (ie.
     *         object's non-static and non-transient fields); {@code false} otherwise.
     */
    public boolean isSerializable() {
        return serializable;
    }

    /**
     * @return {@code true} if the class field includes the {@code static} modifier; {@code false} otherwise.
     */
    public boolean isStatic() {
        return modStatic;
    }

    /**
     * @return {@code true} if the class field includes the {@code strictfp} modifier; {@code false} otherwise.
     */
    public boolean isStrict() {
        return modStrict;
    }

    /**
     * @return {@code true} if the class field includes the {@code synchronized} modifier; {@code false} otherwise.
     */
    public boolean isSynchronized() {
        return modSynchronized;
    }

    /**
     * @return {@code true} if the class field includes the {@code transient} modifier; {@code false} otherwise.
     */
    public boolean isTransient() {
        return modTransient;
    }

    /**
     * @return {@code true} if the class field includes the {@code volatile} modifier; {@code false} otherwise.
     */
    public boolean isVolatile() {
        return modVolatile;
    }

    @Override
    public void printFieldStructure() {
        printClassStructure(this, true, 0);
    }

    public void setFieldSerialiser(final FieldSerialiser<?> fieldSerialiser) {
        this.fieldSerialiser = fieldSerialiser;
    }

    @Override
    public String toString() {
        if (toStringName == null) {
            toStringName = (ClassFieldDescription.class.getSimpleName() + " for: " + getModifierString() + " "
                            + getTypeName() + getGenericFieldTypeStringSimple() + " " + getFieldName() + " (hierarchyDepth = " + getHierarchyDepth() + ")")
                                   .intern();
        }
        return toStringName;
    }

    protected static void exploreClass(final Class<?> classType, final ClassFieldDescription parent, final int recursionLevel, final boolean fullScan) { // NOSONAR NOPMD
        if (ClassUtils.DO_NOT_PARSE_MAP.get(classType) != null) {
            return;
        }
        if (recursionLevel > ClassUtils.getMaxRecursionDepth()) {
            throw new IllegalStateException("recursion error while scanning " + parent.getTypeName() + "::" + classType.getName() + " structure: recursionLevel = '"
                                            + recursionLevel + "' > " + ClassFieldDescription.class.getSimpleName() + ".maxRecursionLevel ='"
                                            + ClassUtils.getMaxRecursionDepth() + "'");
        }

        // call super types
        if ((classType.getSuperclass() != null) && !classType.getSuperclass().equals(Object.class) && !classType.getSuperclass().equals(Enum.class)) {
            // dive into parent hierarchy w/o parsing Object.class, -> meaningless and causes infinite recursion
            exploreClass(classType.getSuperclass(), parent, recursionLevel + 1, fullScan);
        }

        // loop over member fields and inner classes
        Arrays.stream(classType.getDeclaredFields()).map(Field::new).forEach(pfield -> {
            final FieldDescription localParent = parent.getParent();
            if ((localParent != null && pfield.getType().equals(localParent.getType()) && recursionLevel >= ClassUtils.getMaxRecursionDepth()) || pfield.getName().startsWith("this$")) {
                // inner classes contain parent as part of declared fields
                return;
            }
            final ClassFieldDescription field = new ClassFieldDescription(pfield, parent, recursionLevel + 1, fullScan); // NOPMD
            // N.B. unavoidable in-loop object generation

            // N.B. omitting field.isSerializable() (static or transient modifier) is essential
            // as they often indicate class dependencies that are prone to infinite dependency loops
            // (e.g. for classes with static references to themselves or maps-of-maps-of-maps-....)
            final boolean isClassAndNotObjectOrEnmum = field.isClass() && (!field.getType().equals(Object.class) || !field.getType().equals(Enum.class));
            if (field.isSerializable() && (isClassAndNotObjectOrEnmum || field.isInterface()) && field.getDataType().equals(DataType.OTHER)) {
                // object is a (technically) Serializable, unknown (ie 'OTHER) compound object or interface than can be further parsed
                exploreClass(ClassUtils.getRawType(field.getType()), field, recursionLevel + 1, fullScan);
            }
        });
    }

    protected static void printClassStructure(final ClassFieldDescription field, final boolean fullView, final int recursionLevel) {
        final String enumOrClass = field.isEnum() ? "Enum " : "class ";
        final String typeCategory = (field.isInterface() ? "interface " : (field.isPrimitive() ? "" : enumOrClass)); // NOSONAR //NOPMD
        final String typeName = field.getTypeName() + field.getGenericFieldTypeString();
        final String mspace = spaces(recursionLevel * ClassUtils.getIndentationNumberOfSpace());
        final boolean isSerialisable = field.isSerializable();

        if (isSerialisable || fullView) {
            LOGGER.atInfo().addArgument(mspace).addArgument(isSerialisable ? "  " : "//") //
                    .addArgument(field.getModifierString())
                    .addArgument(typeCategory)
                    .addArgument(typeName)
                    .addArgument(field.getFieldName())
                    .log("{} {} {} {}{} {}");
            if (field.isAnnotationPresent()) {
                LOGGER.atInfo().addArgument(mspace).addArgument(isSerialisable ? "  " : "//") //
                        .addArgument(field.getFieldUnit())
                        .addArgument(field.getFieldQuantity())
                        .addArgument(field.getFieldDescription())
                        .addArgument(field.getFieldModifier())
                        .log("{} {}         <meta-info: unit:'{}' quantity:'{}' description:'{}' modifier:'{}'>");
            }

            field.getChildren().forEach(f -> printClassStructure((ClassFieldDescription) f, fullView, recursionLevel + 1));
        }
    }

    private static String getFieldDescription(final AnnotatedElement annotatedElement) {
        final MetaInfo[] annotationMeta = annotatedElement.getAnnotationsByType(MetaInfo.class);
        if (annotationMeta.length > 0) {
            return annotationMeta[0].description().intern();
        }
        final Description[] annotationDescription = annotatedElement.getAnnotationsByType(Description.class);
        if (annotationDescription.length > 0) {
            return annotationDescription[0].value().intern();
        }
        return null;
    }

    private static byte getFieldModifier(final AnnotatedElement annotatedElement) {
        final MetaInfo[] annotationMeta = annotatedElement.getAnnotationsByType(MetaInfo.class);
        if (annotationMeta.length > 0) {
            return annotationMeta[0].modifier();
        }
        final Modifier[] annotationModifier = annotatedElement.getAnnotationsByType(Modifier.class);
        if (annotationModifier.length > 0) {
            return annotationModifier[0].value();
        }
        return 0;
    }

    private static String getFieldUnit(final AnnotatedElement annotatedElement) {
        final MetaInfo[] annotationMeta = annotatedElement.getAnnotationsByType(MetaInfo.class);
        if (annotationMeta.length > 0) {
            return annotationMeta[0].unit().intern();
        }
        final Unit[] annotationUnit = annotatedElement.getAnnotationsByType(Unit.class);
        if (annotationUnit.length > 0) {
            return annotationUnit[0].value().intern();
        }
        return null;
    }

    private static String getFieldQuantity(final AnnotatedElement annotatedElement) {
        final MetaInfo[] annotationMeta = annotatedElement.getAnnotationsByType(MetaInfo.class);
        if (annotationMeta.length > 0) {
            return annotationMeta[0].quantity().intern();
        }
        final Quantity[] annotationQuantity = annotatedElement.getAnnotationsByType(Quantity.class);
        if (annotationQuantity.length > 0) {
            return annotationQuantity[0].value().intern();
        }
        return null;
    }

    private static String spaces(final int spaces) {
        return CharBuffer.allocate(spaces).toString().replace('\0', ' ');
    }
}
