package io.opencmw.utils;

import static org.junit.jupiter.api.Assertions.*;

import static io.opencmw.utils.SystemProperties.parseOptions;

import java.net.URL;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.opencmw.serialiser.annotations.MetaInfo;
import io.opencmw.serialiser.spi.Field;

import com.google.auto.service.AutoService;

class SystemPropertiesTest {
    private static Properties originalStore;

    @BeforeAll
    static void register() {
        SystemProperties.addCommandOptions(TestOptionClass.class);
        originalStore = System.getProperties();
    }

    @BeforeEach
    void init() {
        // re-initialise properties before each test
        System.getProperties().clear();
        System.getProperties().putAll(originalStore);
        SystemProperties.put("testString", "test");
        SystemProperties.put("testInt", "42");
        SystemProperties.put("testLong", "43");
        SystemProperties.put("testDouble", "44.0");
    }

    @Test
    void testCommandLineOptions() {
        Field.getField(SystemProperties.class, "WITH_EXIT").setBoolean(null, false); // needed only for testing purposes

        final Map<String, Object> ret = parseOptions(new String[0]);
        assertNotNull(ret);
        assertTrue(ret.isEmpty());

        try {
            // malformed argument
            parseOptions(new String[] { "--a==0" });
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("could not parse command line options: [--a==0]", e.getMessage());
        }

        try {
            parseOptions(new String[] { "--version" });
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals(SystemProperties.version, e.getMessage());
        }

        assertThrows(IllegalArgumentException.class, () -> parseOptions(new String[] { "-h" }));
        assertThrows(IllegalArgumentException.class, () -> parseOptions(new String[] { "-help" }));

        final String testCommand = "  <program> myCommand [options] command documentation\n";
        SystemProperties.addCommandArgument(testCommand);

        // uncomment for testing:
        // System.err.println(SystemProperties.getCommandLineDoc())
        try {
            parseOptions(new String[] { "-h" });
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains(testCommand));
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
            parseOptions(new String[] { "--help" });
            fail();
        } catch (IllegalArgumentException e) {
            assertFalse(e.getMessage().contains(testCommand));
            assertFalse(e.getMessage().contains(TestOptionClass.TEST_OPTION_DOC));
        }

        Field.getField(SystemProperties.class, "WITH_EXIT").setBoolean(null, true); // needed only for testing purposes
    }

    @Test
    void testHierarchy() {
        assertTrue(parseOptions(new String[] {}).isEmpty(), "empty map");
        final String option = "SystemPropertiesTest$TestOptionClass.TEST_OPTION";
        final URL configFileUrl = SystemProperties.class.getResource("SimplePropertiesTest.properties");
        final String configFile = Objects.requireNonNull(configFileUrl, "could not find test config file").getPath();
        final String configFileError = configFile + "Error";

        // setting via config file
        assertEquals(45, Integer.parseInt(parseOptions(new String[] { "--config=" + configFile }).get(option).toString()));
        assertThrows(IllegalArgumentException.class, () -> parseOptions(new String[] { "--config=" + configFileError }));

        // setting via vm property
        SystemProperties.put(option, "43");
        assertEquals(43, Integer.parseInt(parseOptions(new String[] {}).get(option).toString()));
        assertEquals(43, Integer.parseInt(parseOptions(new String[] { "--config=" + configFile }).get(option).toString()));

        // setting via command line
        assertEquals(42, Integer.parseInt(parseOptions(new String[] { "--" + option + "=42" }).get(option).toString()));
        assertEquals(42, Integer.parseInt(parseOptions(new String[] { "--config=" + configFile }).get(option).toString()));
    }

    @AutoService(Settings.class)
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