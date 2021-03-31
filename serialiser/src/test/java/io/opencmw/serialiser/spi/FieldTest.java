package io.opencmw.serialiser.spi;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import io.opencmw.serialiser.annotations.MetaInfo;

/**
 * Tests reflection related access to Fields w/o necessary direct access. N.B. this is intended for library-use only.
 */
@SuppressWarnings("PMD") // many assertions, 'short' primitive accesses, etc.
class FieldTest {
    @Test
    void testFieldAccess() {
        assertThrows(IllegalArgumentException.class, () -> Field.getField(Field.class, "333"));
        assertNotNull(Field.getField(Field.class, "unsafe"));
        final String testFieldName = "PUBLIC_VARIABLE";
        final Field testField = Field.getField(TestClassModifiers.class, testFieldName);
        assertEquals(testFieldName, testField.getName());
        assertEquals(int.class, testField.getGenericType());
        assertEquals(int.class, testField.getType());
        assertEquals(TestClassModifiers.class, testField.getDeclaringClass());
        //noinspection
        assertNotNull(testField.getJdkField()); // NOPMD NOSONAR - just for testing can be removed once this become unnecessary
        assertNotNull(testField.getAnnotations());
        assertNotNull(testField.getDeclaredAnnotations());
        assertNull(testField.getAnnotation(MetaInfo.class));
    }

    @Test
    void testModifiers() {
        final Field testField = Field.getField(TestClassModifiers.class, "PRIVATE_TEST_VARIABLE");
        assertDoesNotThrow(testField::getModifiers);
        assertTrue(testField.isPrivate());
        assertFalse(testField.isProtected());
        assertFalse(testField.isPublic());
        assertFalse(testField.isPackagePrivate());
        assertTrue(testField.isPrimitive());
        assertTrue(testField.isStatic());
        assertTrue(testField.isFinal());
        assertTrue(testField.isTransient());
        assertFalse(testField.isVolatile());
        assertFalse(testField.isNative());
        assertFalse(testField.isStrict());
        assertFalse(testField.isProtected());
        assertFalse(testField.isAbstract());
        assertFalse(testField.isSynchronized());

        assertFalse(Field.getField(TestClassModifiers.class, "PROTECTED_TEST_VARIABLE").isPrivate());
        assertTrue(Field.getField(TestClassModifiers.class, "PROTECTED_TEST_VARIABLE").isProtected());
        assertFalse(Field.getField(TestClassModifiers.class, "PROTECTED_TEST_VARIABLE").isPublic());
        assertFalse(Field.getField(TestClassModifiers.class, "PROTECTED_TEST_VARIABLE").isPackagePrivate());
        assertTrue(Field.getField(TestClassModifiers.class, "PROTECTED_TEST_VARIABLE").isPrimitive());

        assertFalse(Field.getField(TestClassModifiers.class, "PUBLIC_VARIABLE").isPrivate());
        assertFalse(Field.getField(TestClassModifiers.class, "PUBLIC_VARIABLE").isProtected());
        assertTrue(Field.getField(TestClassModifiers.class, "PUBLIC_VARIABLE").isPublic());
        assertFalse(Field.getField(TestClassModifiers.class, "PUBLIC_VARIABLE").isPackagePrivate());
        assertTrue(Field.getField(TestClassModifiers.class, "PUBLIC_VARIABLE").isPrimitive());

        assertFalse(Field.getField(TestClassModifiers.class, "PACKAGE_PRIVATE_TEST_VARIABLE").isPrivate());
        assertFalse(Field.getField(TestClassModifiers.class, "PACKAGE_PRIVATE_TEST_VARIABLE").isProtected());
        assertFalse(Field.getField(TestClassModifiers.class, "PACKAGE_PRIVATE_TEST_VARIABLE").isPublic());
        assertTrue(Field.getField(TestClassModifiers.class, "PACKAGE_PRIVATE_TEST_VARIABLE").isPackagePrivate());
        assertTrue(Field.getField(TestClassModifiers.class, "PACKAGE_PRIVATE_TEST_VARIABLE").isPrimitive());

        assertFalse(Field.getField(TestClassModifiers.class, "NON_STATIC_PUBLIC_TEST_VARIABLE").isStatic());
        assertTrue(Field.getField(TestClassModifiers.class, "NON_STATIC_PUBLIC_TEST_VARIABLE").isVolatile());
    }

    @Test
    void testPrimitiveAccess() {
        final TestClassPrimitiveAccess testClass = new TestClassPrimitiveAccess();
        final Class<? extends TestClassPrimitiveAccess> clazz = testClass.getClass();

        Field.getField(clazz, "dummyBoolean").setBoolean(testClass, true);
        assertTrue(testClass.dummyBoolean);
        assertTrue(Field.getField(clazz, "dummyBoolean").getBoolean(testClass));

        Field.getField(clazz, "dummyByte").setByte(testClass, (byte) 3);
        assertEquals(3, testClass.dummyByte);
        assertEquals(testClass.dummyByte, Field.getField(clazz, "dummyByte").getByte(testClass));

        Field.getField(clazz, "dummyChar").setChar(testClass, (char) 4);
        assertEquals(4, testClass.dummyChar);
        assertEquals(testClass.dummyChar, Field.getField(clazz, "dummyChar").getChar(testClass));

        Field.getField(clazz, "dummyShort").setShort(testClass, (short) 12);
        assertEquals(12, testClass.dummyShort);
        assertEquals(testClass.dummyShort, Field.getField(clazz, "dummyShort").getShort(testClass));

        Field.getField(clazz, "dummyInt").setInt(testClass, 42);
        assertEquals(42, testClass.dummyInt);
        assertEquals(testClass.dummyInt, Field.getField(clazz, "dummyInt").getInt(testClass));

        Field.getField(clazz, "dummyLong").setLong(testClass, 42L);
        assertEquals(42L, testClass.dummyLong);
        assertEquals(testClass.dummyLong, Field.getField(clazz, "dummyLong").getLong(testClass));

        Field.getField(clazz, "dummyFloat").setFloat(testClass, 1.0f);
        assertEquals(1.0f, testClass.dummyFloat);
        assertEquals(testClass.dummyFloat, Field.getField(clazz, "dummyFloat").getFloat(testClass));

        Field.getField(clazz, "dummyDouble").setDouble(testClass, 2.0);
        assertEquals(2.0, testClass.dummyDouble);
        assertEquals(testClass.dummyDouble, Field.getField(clazz, "dummyDouble").getDouble(testClass));

        Field.getField(clazz, "dummyString").set(testClass, "test");
        assertEquals("test", testClass.dummyString);
        assertEquals(testClass.dummyString, Field.getField(clazz, "dummyString").get(testClass));
    }

    @Test
    void testPrimitiveStaticAccess() {
        final Class<? extends TestClassPrimitiveStaticAccess> clazz = TestClassPrimitiveStaticAccess.class;

        Field.getField(clazz, "dummyBoolean").setBoolean(null, true);
        assertTrue(TestClassPrimitiveStaticAccess.dummyBoolean);
        assertTrue(Field.getField(clazz, "dummyBoolean").getBoolean(null));

        Field.getField(clazz, "dummyByte").setByte(null, (byte) 3);
        assertEquals(3, TestClassPrimitiveStaticAccess.dummyByte);
        assertEquals(TestClassPrimitiveStaticAccess.dummyByte, Field.getField(clazz, "dummyByte").getByte(null));

        Field.getField(clazz, "dummyChar").setChar(null, (char) 4);
        assertEquals(4, TestClassPrimitiveStaticAccess.dummyChar);
        assertEquals(TestClassPrimitiveStaticAccess.dummyChar, Field.getField(clazz, "dummyChar").getChar(null));

        Field.getField(clazz, "dummyShort").setShort(null, (short) 12);
        assertEquals(12, TestClassPrimitiveStaticAccess.dummyShort);
        assertEquals(TestClassPrimitiveStaticAccess.dummyShort, Field.getField(clazz, "dummyShort").getShort(null));

        Field.getField(clazz, "dummyInt").setInt(null, 42);
        assertEquals(42, TestClassPrimitiveStaticAccess.dummyInt);
        assertEquals(TestClassPrimitiveStaticAccess.dummyInt, Field.getField(clazz, "dummyInt").getInt(null));

        Field.getField(clazz, "dummyLong").setLong(null, 42L);
        assertEquals(42L, TestClassPrimitiveStaticAccess.dummyLong);
        assertEquals(TestClassPrimitiveStaticAccess.dummyLong, Field.getField(clazz, "dummyLong").getLong(null));

        Field.getField(clazz, "dummyFloat").setFloat(null, 1.0f);
        assertEquals(1.0f, TestClassPrimitiveStaticAccess.dummyFloat);
        assertEquals(TestClassPrimitiveStaticAccess.dummyFloat, Field.getField(clazz, "dummyFloat").getFloat(null));

        Field.getField(clazz, "dummyDouble").setDouble(null, 2.0);
        assertEquals(2.0, TestClassPrimitiveStaticAccess.dummyDouble);
        assertEquals(TestClassPrimitiveStaticAccess.dummyDouble, Field.getField(clazz, "dummyDouble").getDouble(null));

        Field.getField(clazz, "dummyString").set(null, "test");
        assertEquals("test", TestClassPrimitiveStaticAccess.dummyString);
        assertEquals(TestClassPrimitiveStaticAccess.dummyString, Field.getField(clazz, "dummyString").get(null));
    }

    @Test
    void testGenericStringSetterGetter() {
        final Class<? extends TestClassPrimitiveStaticAccess> clazz = TestClassPrimitiveStaticAccess.class;

        Field.getField(clazz, "dummyInt").set(null, "42");
        assertEquals(42, TestClassPrimitiveStaticAccess.dummyInt);
        assertEquals(TestClassPrimitiveStaticAccess.dummyInt, Field.getField(clazz, "dummyInt").get(null));

        Field.getField(clazz, "dummyBoolean").set(null, "true");
        assertTrue(TestClassPrimitiveStaticAccess.dummyBoolean);
        assertTrue(Field.getField(clazz, "dummyBoolean").getBoolean(null));
        assertTrue((boolean) Field.getField(clazz, "dummyBoolean").get(null));

        Field.getField(clazz, "dummyByte").set(null, "3");
        assertEquals(3, TestClassPrimitiveStaticAccess.dummyByte);
        assertEquals(TestClassPrimitiveStaticAccess.dummyByte, Field.getField(clazz, "dummyByte").get(null));

        Field.getField(clazz, "dummyChar").set(null, "4");
        assertEquals('4', TestClassPrimitiveStaticAccess.dummyChar);
        assertEquals(TestClassPrimitiveStaticAccess.dummyChar, Field.getField(clazz, "dummyChar").get(null));

        Field.getField(clazz, "dummyShort").set(null, "12");
        assertEquals(12, TestClassPrimitiveStaticAccess.dummyShort);
        assertEquals(TestClassPrimitiveStaticAccess.dummyShort, Field.getField(clazz, "dummyShort").get(null));

        Field.getField(clazz, "dummyInt").set(null, "42");
        assertEquals(42, TestClassPrimitiveStaticAccess.dummyInt);
        assertEquals(TestClassPrimitiveStaticAccess.dummyInt, Field.getField(clazz, "dummyInt").get(null));

        Field.getField(clazz, "dummyLong").set(null, "42");
        assertEquals(42L, TestClassPrimitiveStaticAccess.dummyLong);
        assertEquals(TestClassPrimitiveStaticAccess.dummyLong, Field.getField(clazz, "dummyLong").get(null));

        Field.getField(clazz, "dummyFloat").set(null, "1.0");
        assertEquals(1.0f, TestClassPrimitiveStaticAccess.dummyFloat);
        assertEquals(TestClassPrimitiveStaticAccess.dummyFloat, Field.getField(clazz, "dummyFloat").get(null));

        Field.getField(clazz, "dummyDouble").set(null, "2.0");
        assertEquals(2.0, TestClassPrimitiveStaticAccess.dummyDouble);
        assertEquals(TestClassPrimitiveStaticAccess.dummyDouble, Field.getField(clazz, "dummyDouble").get(null));

        Field.getField(clazz, "dummyString").set(null, "test");
        assertEquals("test", TestClassPrimitiveStaticAccess.dummyString);
        assertEquals(TestClassPrimitiveStaticAccess.dummyString, Field.getField(clazz, "dummyString").get(null));

        assertThrows(NumberFormatException.class, () -> Field.getField(clazz, "dummyLong").set(null, "42L"));
        assertThrows(IllegalArgumentException.class, () -> Field.getField(clazz, "unknownObject").set(null, "??"));
    }

    @Test
    @SuppressWarnings({ "StringOperationCanBeSimplified" })
    void testStringReset() {
        final String testString1 = "Hello World! - immutable".intern(); //N.B. String content should be unique

        assertEquals("Hello World! - immutable", testString1);
        Field.resetString(testString1, "Something else.");
        assertEquals("Something else.", testString1);
    }

    @Test
    void testImpossiblePrimitiveAccess() {
        final Class<? extends TestClassStaticPrimitiveImpossibleWriteAccess> clazz = TestClassStaticPrimitiveImpossibleWriteAccess.class;

        assertThrows(IllegalArgumentException.class, () -> Field.getField(clazz, "dummyBoolean").setBoolean(null, true));
        assertThrows(IllegalArgumentException.class, () -> Field.getField(clazz, "dummyByte").setByte(null, (byte) 3));
        assertThrows(IllegalArgumentException.class, () -> Field.getField(clazz, "dummyChar").setChar(null, (char) 4));
        assertThrows(IllegalArgumentException.class, () -> Field.getField(clazz, "dummyShort").setShort(null, (short) 12));
        assertThrows(IllegalArgumentException.class, () -> Field.getField(clazz, "dummyInt").setInt(null, 42));
        assertThrows(IllegalArgumentException.class, () -> Field.getField(clazz, "dummyLong").setLong(null, 42L));
        assertThrows(IllegalArgumentException.class, () -> Field.getField(clazz, "dummyFloat").setFloat(null, 1.0f));
        assertThrows(IllegalArgumentException.class, () -> Field.getField(clazz, "dummyDouble").setDouble(null, 2.0));
    }

    @Test
    void testBoxedAccess() {
        final TestClassBoxedAccess testClass = new TestClassBoxedAccess();
        final Class<? extends TestClassBoxedAccess> clazz = testClass.getClass();

        Field.getField(clazz, "dummyBoolean").setBoolean(testClass, true);
        assertTrue(testClass.dummyBoolean);
        assertTrue(Field.getField(clazz, "dummyBoolean").getBoolean(testClass));

        Field.getField(clazz, "dummyByte").setByte(testClass, (byte) 3);
        assertEquals((byte) 3, testClass.dummyByte);
        assertEquals(testClass.dummyByte, Field.getField(clazz, "dummyByte").getByte(testClass));

        Field.getField(clazz, "dummyChar").setChar(testClass, (char) 4);
        assertEquals((char) 4, testClass.dummyChar);
        assertEquals(testClass.dummyChar, Field.getField(clazz, "dummyChar").getChar(testClass));

        Field.getField(clazz, "dummyShort").setShort(testClass, (short) 12);
        assertEquals((short) 12, testClass.dummyShort);
        assertEquals(testClass.dummyShort, Field.getField(clazz, "dummyShort").getShort(testClass));

        Field.getField(clazz, "dummyInt").setInt(testClass, 42);
        assertEquals(42, testClass.dummyInt);
        assertEquals(testClass.dummyInt, Field.getField(clazz, "dummyInt").getInt(testClass));

        Field.getField(clazz, "dummyLong").setLong(testClass, 42L);
        assertEquals(42L, testClass.dummyLong);
        assertEquals(testClass.dummyLong, Field.getField(clazz, "dummyLong").getLong(testClass));

        Field.getField(clazz, "dummyFloat").setFloat(testClass, 1.0f);
        assertEquals(1.0f, testClass.dummyFloat);
        assertEquals(testClass.dummyFloat, Field.getField(clazz, "dummyFloat").getFloat(testClass));

        Field.getField(clazz, "dummyDouble").setDouble(testClass, 2.0);
        assertEquals(2.0, testClass.dummyDouble);
        assertEquals(testClass.dummyDouble, Field.getField(clazz, "dummyDouble").getDouble(testClass));

        Field.getField(clazz, "dummyString").set(testClass, "test");
        assertEquals("test", testClass.dummyString);
        assertEquals(testClass.dummyString, Field.getField(clazz, "dummyString").get(testClass));
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    void TestClassStaticBoxedAccess() {
        final Class<?> clazz = TestClassStaticBoxedAccess.class;

        assertFalse(TestClassStaticBoxedAccess.dummyBoolean);
        Field.getField(clazz, "dummyBoolean").setBoolean(null, true);
        assertTrue(TestClassStaticBoxedAccess.dummyBoolean); // NOPMD NOSONAR the magic of reflection makes this possible
        assertTrue(Field.getField(clazz, "dummyBoolean").getBoolean(null));

        Field.getField(clazz, "dummyByte").setByte(null, (byte) 3);
        assertEquals(3, (int) TestClassStaticBoxedAccess.dummyByte);
        assertEquals(TestClassStaticBoxedAccess.dummyByte, Field.getField(clazz, "dummyByte").getByte(null));

        Field.getField(clazz, "dummyChar").setChar(null, (char) 4);
        assertEquals(4, (int) TestClassStaticBoxedAccess.dummyChar);
        assertEquals(TestClassStaticBoxedAccess.dummyChar, Field.getField(clazz, "dummyChar").getChar(null));

        Field.getField(clazz, "dummyShort").setShort(null, (short) 12);
        assertEquals(12, (int) TestClassStaticBoxedAccess.dummyShort);
        assertEquals(TestClassStaticBoxedAccess.dummyShort, Field.getField(clazz, "dummyShort").getShort(null));

        Field.getField(clazz, "dummyInt").setInt(null, 42);
        assertEquals(42, TestClassStaticBoxedAccess.dummyInt);
        assertEquals(TestClassStaticBoxedAccess.dummyInt, Field.getField(clazz, "dummyInt").getInt(null));

        Field.getField(clazz, "dummyLong").setLong(null, 42L);
        assertEquals(42L, TestClassStaticBoxedAccess.dummyLong);
        assertEquals(TestClassStaticBoxedAccess.dummyLong, Field.getField(clazz, "dummyLong").getLong(null));

        Field.getField(clazz, "dummyFloat").setFloat(null, 1.0f);
        assertEquals(1.0f, TestClassStaticBoxedAccess.dummyFloat);
        assertEquals(TestClassStaticBoxedAccess.dummyFloat, Field.getField(clazz, "dummyFloat").getFloat(null));

        Field.getField(clazz, "dummyDouble").setDouble(null, 2.0);
        assertEquals(2.0, TestClassStaticBoxedAccess.dummyDouble);
        assertEquals(TestClassStaticBoxedAccess.dummyDouble, Field.getField(clazz, "dummyDouble").getDouble(null));

        Field.getField(clazz, "dummyString").set(null, "test");
        // this doesn't seem to be working -- possible early JIT optimisation for inlining 'static final' Strings
        // assertEquals("test", TestClassStaticBoxedAccess.dummyString);
        assertEquals("test", Field.getField(clazz, "dummyString").get(null));
    }

    /**
     * small test class - do not remove test fields these are all access via reflection
     */
    private static class TestClassModifiers {
        public static final transient int PUBLIC_VARIABLE = 3;
        protected static final transient int PROTECTED_TEST_VARIABLE = 2;
        static final transient int PACKAGE_PRIVATE_TEST_VARIABLE = 4;
        private static final transient int PRIVATE_TEST_VARIABLE = 1;
        public transient volatile double NON_STATIC_PUBLIC_TEST_VARIABLE = 5.0;
    }

    private static class TestClassPrimitiveAccess {
        protected boolean dummyBoolean;
        protected byte dummyByte;
        protected char dummyChar;
        protected short dummyShort;
        protected int dummyInt;
        protected long dummyLong;
        protected float dummyFloat;
        protected double dummyDouble;
        protected String dummyString = "Test";
    }

    /**
     * small test class - do not remove test fields these are all access via reflection
     */
    private static class TestClassPrimitiveStaticAccess {
        protected static boolean dummyBoolean;
        protected static byte dummyByte;
        protected static char dummyChar;
        protected static short dummyShort;
        protected static int dummyInt;
        protected static long dummyLong;
        protected static float dummyFloat;
        protected static double dummyDouble;
        protected static String dummyString = "Test";
        protected static Object unknownObject;
    }

    /**
     * small test class - do not remove test fields these are all access via reflection
     */
    private static class TestClassStaticPrimitiveImpossibleWriteAccess {
        public static final boolean dummyBoolean = false;
        public static final byte dummyByte = (byte) 0x04;
        public static final char dummyChar = (char) 0x02;
        public static final short dummyShort = (short) 0x05;
        public static final int dummyInt = 0x05;
        public static final long dummyLong = 0x07;
        public static final float dummyFloat = 41.0f;
        public static final double dummyDouble = 42.0;
    }

    private static class TestClassBoxedAccess {
        protected Boolean dummyBoolean;
        protected Byte dummyByte;
        protected Character dummyChar;
        protected Short dummyShort;
        protected Integer dummyInt;
        protected Long dummyLong;
        protected Float dummyFloat;
        protected Double dummyDouble;
        protected String dummyString = "Test";
    }

    public static class TestClassStaticBoxedAccess {
        public static final Boolean dummyBoolean = false;
        public static final Byte dummyByte = (byte) 0;
        public static final Character dummyChar = (char) 0;
        public static final Short dummyShort = (short) 0;
        public static final Integer dummyInt = 0;
        public static final Long dummyLong = 0L;
        public static final Float dummyFloat = .0f;
        public static final Double dummyDouble = .0;
        public static final String dummyString = "";
    }
}