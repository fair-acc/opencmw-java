package io.opencmw.utils;

import static org.junit.jupiter.api.Assertions.*;

import static io.opencmw.utils.SystemProperties.parseOptions;

import java.util.Map;

import org.docopt.DocoptExitException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.opencmw.serialiser.annotations.MetaInfo;
import io.opencmw.serialiser.spi.Field;

class SystemPropertiesTest {
    @BeforeEach
    void init() {
        // re-initialise properties before each test
        SystemProperties.put("testString", "test");
        SystemProperties.put("testInt", "42");
        SystemProperties.put("testLong", "43");
        SystemProperties.put("testDouble", "44.0");
    }

    @SuppressWarnings("InstantiationOfUtilityClass")
    @Test
    void testCommandLineOptions() {
        Field.getField(SystemProperties.class, "WITH_EXIT").setBoolean(null, false); // needed only for testing purposes

        final Map<String, Object> ret = parseOptions(new String[0]);
        assertNotNull(ret);
        assertFalse(ret.isEmpty());

        try {
            parseOptions(new String[] { "--version" });
            fail();
        } catch (DocoptExitException e) {
            assertEquals(SystemProperties.version, e.getMessage());
        }

        assertThrows(DocoptExitException.class, () -> parseOptions(new String[] { "-h" }));
        assertThrows(DocoptExitException.class, () -> parseOptions(new String[] { "-help" }));

        final String testCommand = "  <program> myCommand [options] command documentation\n";
        SystemProperties.addCommandArgument(testCommand);

        SystemProperties.addCommandOptions(new TestOptionClass());
        // uncomment for testing:
        // System.err.println(SystemProperties.getCommandLineDoc())
        try {
            parseOptions(new String[] { "-help" });
            fail();
        } catch (DocoptExitException e) {
            assertTrue(e.getMessage().contains(testCommand.split("<program>")[1]));
            assertTrue(e.getMessage().contains(TestOptionClass.TEST_OPTION_DOC));
        }

        final String option = "SystemPropertiesTest$TestOptionClass.TEST_OPTION";
        assertEquals(42, Integer.parseInt(parseOptions(new String[] { "--" + option + "=42" }).get(option).toString()));
        assertEquals(42, SystemProperties.getIntValueIgnoreCase(option));

        assertEquals(43, Integer.parseInt(parseOptions(new String[] { "--" + option + "=43" }).get(option).toString()));
        assertEquals(43, SystemProperties.getIntValueIgnoreCase(option));

        SystemProperties.COMMAND_ARGUMENTS.clear();
        SystemProperties.COMMAND_OPTION_CLASSES.clear();
        // uncomment for testing:
        try {
            parseOptions(new String[] { "-help" });
            fail();
        } catch (DocoptExitException e) {
            assertFalse(e.getMessage().contains(testCommand));
            assertFalse(e.getMessage().contains(TestOptionClass.TEST_OPTION_DOC));
        }

        Field.getField(SystemProperties.class, "WITH_EXIT").setBoolean(null, true); // needed only for testing purposes
    }

    private static class TestOptionClass {
        public static final String TEST_OPTION_DOC = "TestOptionClass.TEST_OPTION=<int>";
        @MetaInfo(unit = "int", description = "test option documentation")
        public static final Integer TEST_OPTION = 0;
    }

    @Test
    void getProperty() {
        assertNotNull(SystemProperties.getProperty("testString"));
        assertEquals(SystemProperties.getPropertyIgnoreCase("testString"), SystemProperties.getPropertyIgnoreCase("TESTSTRING"));
        assertEquals(SystemProperties.getPropertyIgnoreCase("testInt"), SystemProperties.getPropertyIgnoreCase("TESTINT"));
        assertEquals(SystemProperties.getPropertyIgnoreCase("testLong"), SystemProperties.getPropertyIgnoreCase("TESTLONG"));
        assertEquals(SystemProperties.getPropertyIgnoreCase("testDouble"), SystemProperties.getPropertyIgnoreCase("TESTDOUBLE"));
        assertNull(SystemProperties.getPropertyIgnoreCase("unknownProperty"));
    }

    @Test
    void getValue() {
        assertEquals(SystemProperties.getValue("testInt", 1), SystemProperties.getValueIgnoreCase("TESTINT", 3));
        assertEquals(SystemProperties.getValue("testLong", 1), SystemProperties.getValueIgnoreCase("TESTLONG", 3L));
        assertEquals(SystemProperties.getValue("testDouble", 1.0), SystemProperties.getValueIgnoreCase("TESTDOUBLE", 3.0));
        assertEquals(SystemProperties.getValue("testInt", 1), SystemProperties.getIntValueIgnoreCase("TESTINT"));
        assertEquals(SystemProperties.getValue("testLong", 1), SystemProperties.getLongValueIgnoreCase("TESTLONG"));
        assertEquals(SystemProperties.getValue("testDouble", 1.0), SystemProperties.getDoubleValueIgnoreCase("TESTDOUBLE"));
    }

    @Test
    void put() {
        assertDoesNotThrow(() -> SystemProperties.put("testKey", "testValue"));
        assertEquals("testValue", SystemProperties.getProperty("testKey"));
    }
}