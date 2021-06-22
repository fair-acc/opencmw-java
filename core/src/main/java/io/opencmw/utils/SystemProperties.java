package io.opencmw.utils;

import static java.util.Map.Entry;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.docopt.Docopt;
import org.docopt.DocoptExitException;
import org.jetbrains.annotations.NotNull;

import io.opencmw.serialiser.spi.ClassFieldDescription;

@SuppressWarnings({ "PMD.GodClass" })
public final class SystemProperties { //NOPMD -- nomen est omen
    protected static final List<String> COMMAND_ARGUMENTS = new NoDuplicatesList<>(); // NOPMD 'protected' needed only for testing purposes
    protected static final Map<String, Object> COMMAND_OPTION_CLASSES = new ConcurrentHashMap<>(); // NOPMD 'protected' needed only for testing purposes
    public static final String DEFAULT_CFG = "default.cfg";
    private static final String PROGRAM_NAME_TAG = "<program>";
    private static final Properties SYSTEM_PROPERTIES = System.getProperties();
    private static final String DEFAULT_ARGUMENTS = "  <program> [options]\n";
    public static String version = "<program version>";
    private static final Boolean WITH_EXIT = true; // needed only for testing purposes

    private SystemProperties() {
        // utility class
    }

    public static void addCommandArgument(final @NotNull String arguments) {
        COMMAND_ARGUMENTS.add(arguments);
    }

    public static void addCommandOptions(final @NotNull Object definition) {
        if (null == COMMAND_OPTION_CLASSES.put(StringUtils.removeStart(definition.toString(), "class "), definition)) {
            SystemProperties.parseOptions(new String[0]);
        }
    }

    @SuppressWarnings("PMD.NPathComplexity")
    public static Map<String, Object> parseOptions(@NotNull final String[] args) {
        final String commandLineDoc = getCommandLineDoc();
        Map<String, Object> tempOpts;
        try {
            tempOpts = new Docopt(commandLineDoc).withExit(false).withVersion(version).parse(List.of(args)); // NOPMD - no concurrent access
        } catch (DocoptExitException e) {
            if (version != null && version.equals(e.getMessage())) {
                if (WITH_EXIT) {
                    System.out.println("program version: " + version);
                    System.exit(0); // NOPMD -- needed once/in case DocOpt is being replaced
                }
                throw new IllegalArgumentException(version, e); // parsed '--version' option
            }

            if (Boolean.TRUE.equals(StringUtils.startsWith(e.getMessage(), "Usage:"))) {
                if (WITH_EXIT) {
                    System.out.println(e.getMessage());
                    System.exit(0); // NOPMD -- needed once/in case DocOpt is being replaced
                }
                throw new IllegalArgumentException(e.getMessage(), e); // requested '--help' option (TEST-only)
            }

            if (WITH_EXIT) {
                System.out.println(commandLineDoc);
            }
            throw new IllegalArgumentException("could not parse command line options: " + Arrays.toString(args), e);
        }

        final Map<String, Object> opts = Objects.requireNonNull(tempOpts, "could not parse command line options: " + Arrays.toString(args));
        final String configFile = Objects.requireNonNull(opts.get("--config"), "corrupt config file name").toString();

        final Properties configFileProperties = new Properties();
        try (InputStream input = Files.newInputStream(Paths.get(configFile))) { // N
            // load a properties file
            configFileProperties.load(input);
        } catch (final IOException e) {
            if (!DEFAULT_CFG.equals(configFile)) {
                throw new IllegalArgumentException("could not find file: '" + Paths.get(configFile) + "'", e);
            }
            // to improve: add searching for file in default path, silently continue if this is the missing 'default.cfg' file and throwing an exception otherwise
        }

        final ConcurrentHashMap<String, Object> retMap = new ConcurrentHashMap<>();
        for (Object optionClass : COMMAND_OPTION_CLASSES.values()) {
            final ClassFieldDescription classDescription = new ClassFieldDescription((optionClass instanceof Class) ? (Class<?>) optionClass : optionClass.getClass(), true); // NOPMD
            final String configClassName = classDescription.getTypeNameSimple();
            classDescription.getChildren().stream().map(ClassFieldDescription.class ::cast).forEach(field -> {
                final String environmentOption = configClassName + '.' + field.getFieldName();
                String stringValue = null;
                // default values taken form class definition -- lowest priority

                // passing config file option
                if (configFileProperties.get(environmentOption) != null) {
                    stringValue = configFileProperties.get(environmentOption).toString();
                }

                // passing JVM environment Variables
                if (SYSTEM_PROPERTIES.get(environmentOption) != null) {
                    final String testStringValue = SYSTEM_PROPERTIES.get(environmentOption).toString();
                    if (!testStringValue.isBlank()) {
                        stringValue = SYSTEM_PROPERTIES.get(environmentOption).toString();
                    }
                }

                // passing command-line options -- highest priority
                final String commandLineOption = "--" + environmentOption;
                final boolean argsContainCommandLineOption = Stream.of(args).anyMatch(s -> StringUtils.startsWith(s, commandLineOption));
                if (opts.get(commandLineOption) != null && argsContainCommandLineOption) {
                    final Object value = opts.get(commandLineOption);
                    // found matching field
                    stringValue = (value instanceof List ? ((List<?>) value).get(0) : value).toString();
                }

                // perform actual setting of settings
                if (stringValue != null) {
                    field.getField().set(optionClass, stringValue);
                    SystemProperties.put(environmentOption, stringValue);
                    retMap.put(environmentOption, stringValue);
                }
            });
        }

        return retMap;
    }

    public static String getCommandLineDoc() {
        for (final Settings settingClass : ServiceLoader.load(Settings.class)) {
            // enable this for testing
            COMMAND_OPTION_CLASSES.put(settingClass.getClass().getName(), settingClass);
        }

        final StringBuilder builder = new StringBuilder(1000);
        builder.append("Usage:\n").append(String.join("", COMMAND_ARGUMENTS)).append(DEFAULT_ARGUMENTS).append("\nOptions:\n");

        final List<String[]> descriptionItems = new ArrayList<>();
        for (Object optionClass : COMMAND_OPTION_CLASSES.values()) {
            final ClassFieldDescription classDescription = new ClassFieldDescription((optionClass instanceof Class) ? (Class<?>) optionClass : optionClass.getClass(), true); // NOPMD
            final String configClassName = classDescription.getTypeNameSimple();
            classDescription.getChildren().stream().map(ClassFieldDescription.class ::cast).filter(f -> f.isPublic() && f.isAnnotationPresent()).forEach(field -> {
                final String commandLineOption = "--" + configClassName + '.' + field.getFieldName();
                final String[] propertyDescription = new String[3]; // NOPMD - dynamic in loop generation needed
                propertyDescription[0] = commandLineOption + "=<" + field.getFieldUnit() + '>';
                propertyDescription[1] = "[default: " + field.getField().get(optionClass) + "]";
                propertyDescription[2] = field.getFieldDescription();
                descriptionItems.add(propertyDescription);
            });
        }
        // add default optional parameters
        descriptionItems.add(new String[] { "", "", "" }); // empty line on purpose
        descriptionItems.add(new String[] { "-c FILE --config=FILE", "[default: " + DEFAULT_CFG + "]", "load properties from file." });
        descriptionItems.add(new String[] { "-p --print", "", "print actual parsed/provided parameters." });
        descriptionItems.add(new String[] { "-h --help", "", "show this screen." });
        descriptionItems.add(new String[] { "--version", "", "show version." });

        final int longestArg = descriptionItems.stream().mapToInt(s -> s[0].length()).max().orElse(0) + 2;
        final int longestUnit = descriptionItems.stream().mapToInt(s -> s[1].length()).max().orElse(0);
        for (String[] line : descriptionItems) {
            boolean first = true;
            for (String description : StringUtils.split(line[2], "\n")) {
                builder.append(String.format("  %-" + longestArg + "s %-" + longestUnit + "s %s\n", first ? line[0] : "", first ? line[1] : "", description));
                first = false;
            }
        }

        final String programName = StringUtils.split(Objects.requireNonNullElse(System.getProperty("sun.java.command"), PROGRAM_NAME_TAG), " ")[0];
        return StringUtils.replace(builder.toString(), PROGRAM_NAME_TAG, programName);
    }

    public static String getProperty(final String key) {
        return SYSTEM_PROPERTIES.getProperty(key);
    }

    public static String getPropertyIgnoreCase(String key, String defaultValue) {
        String value = SYSTEM_PROPERTIES.getProperty(key);
        if (null != value) {
            return value;
        }

        // Not matching with the actual key then
        Set<Entry<Object, Object>> systemProperties = SYSTEM_PROPERTIES.entrySet();
        for (final Entry<Object, Object> entry : systemProperties) {
            if (key.equalsIgnoreCase((String) entry.getKey())) {
                return (String) entry.getValue();
            }
        }
        return defaultValue;
    }

    public static String getPropertyIgnoreCase(String key) {
        return getPropertyIgnoreCase(key, null);
    }

    public static double getValue(String key, double defaultValue) {
        final String value = getProperty(key);
        return value == null ? defaultValue : Double.parseDouble(value);
    }

    public static int getValue(String key, int defaultValue) {
        final String value = getProperty(key);
        return value == null ? defaultValue : Integer.parseInt(value);
    }

    public static int getIntValueIgnoreCase(String key) {
        return Integer.parseInt(Objects.requireNonNull(getPropertyIgnoreCase(key), "value null for key: " + key));
    }

    public static long getLongValueIgnoreCase(String key) {
        return Long.parseLong(Objects.requireNonNull(getPropertyIgnoreCase(key), "value null for key: " + key));
    }

    public static double getDoubleValueIgnoreCase(String key) {
        return Double.parseDouble(Objects.requireNonNull(getPropertyIgnoreCase(key), "value null for key: " + key));
    }

    public static double getValueIgnoreCase(String key, double defaultValue) {
        final String value = getPropertyIgnoreCase(key);
        return value == null ? defaultValue : Double.parseDouble(value);
    }

    public static int getValueIgnoreCase(String key, int defaultValue) {
        final String value = getPropertyIgnoreCase(key);
        return value == null ? defaultValue : Integer.parseInt(value);
    }

    public static long getValueIgnoreCase(String key, long defaultValue) {
        final String value = getPropertyIgnoreCase(key);
        return value == null ? defaultValue : Long.parseLong(value);
    }

    public static Object put(final Object key, final Object value) {
        return SYSTEM_PROPERTIES.put(key, value);
    }
}