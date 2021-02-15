package io.opencmw.client;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Endpoint helper class to deserialise endpoint strings.
 * Uses lazy initialisation to prevent doing unnecessary work or doing the same thing twice.
 */
public class Endpoint { // NOPMD data class
    private static final String DEFAULT_SELECTOR = "";
    public static final String FILTER_TYPE_LONG = "long:";
    public static final String FILTER_TYPE_INT = "int:";
    public static final String FILTER_TYPE_BOOL = "bool:";
    public static final String FILTER_TYPE_DOUBLE = "double:";
    public static final String FILTER_TYPE_FLOAT = "float:";
    private final String value;
    private String protocol;
    private String address;
    private String device;
    private String property;
    private String selector;
    private Map<String, Object> filters;

    public Endpoint(final String endpoint) {
        this.value = endpoint;
    }

    public String getProtocol() {
        if (protocol == null) {
            parse();
        }
        return protocol;
    }

    @Override
    public String toString() {
        return value;
    }

    public String getAddress() {
        if (protocol == null) {
            parse();
        }
        return address;
    }

    public String getDevice() {
        if (protocol == null) {
            parse();
        }
        return device;
    }

    public String getSelector() {
        if (protocol == null) {
            parse();
        }
        return selector;
    }

    public String getProperty() {
        return property;
    }

    public Map<String, Object> getFilters() {
        return filters;
    }

    public String getEndpointForContext(final String context) {
        if (context == null || context.equals("")) {
            return value;
        }
        parse();
        final String filterString = filters.entrySet().stream() //
                                            .map(e -> {
                                                String val;
                                                if (e.getValue() instanceof String) {
                                                    val = (String) e.getValue();
                                                } else if (e.getValue() instanceof Integer) {
                                                    val = FILTER_TYPE_INT + e.getValue();
                                                } else if (e.getValue() instanceof Long) {
                                                    val = FILTER_TYPE_LONG + e.getValue();
                                                } else if (e.getValue() instanceof Boolean) {
                                                    val = FILTER_TYPE_BOOL + e.getValue();
                                                } else if (e.getValue() instanceof Double) {
                                                    val = FILTER_TYPE_DOUBLE + e.getValue();
                                                } else if (e.getValue() instanceof Float) {
                                                    val = FILTER_TYPE_FLOAT + e.getValue();
                                                } else {
                                                    throw new UnsupportedOperationException("Data type not supported in endpoint filters");
                                                }
                                                return e.getKey() + '=' + val;
                                            }) //
                                            .collect(Collectors.joining("&"));
        return address + '/' + device + '/' + property + "?ctx=" + context + '&' + filterString;
    }

    private void parse() {
        final String[] tmp = value.split("\\?", 2); // split into address/dev/prop and sel+filters part
        final String[] adp = tmp[0].split("/"); // split access point into parts
        device = adp[adp.length - 2]; // get device name from access point
        property = adp[adp.length - 1]; // get property name from access point
        address = tmp[0].substring(0, tmp[0].length() - device.length() - property.length() - 2);
        protocol = address.substring(0, address.indexOf("://") + 3);
        filters = new HashMap<>();
        selector = DEFAULT_SELECTOR;
        filters = new HashMap<>();

        final String paramString = tmp[1];
        final String[] kvpairs = paramString.split("&"); // split into individual key/value pairs
        for (final String pair : kvpairs) {
            String[] splitpair = pair.split("=", 2); // split at first equal sign
            if (splitpair.length != 2) {
                continue;
            }
            if ("ctx".equals(splitpair[0])) {
                selector = splitpair[1];
            } else {
                if (splitpair[1].startsWith(FILTER_TYPE_INT)) {
                    filters.put(splitpair[0], Integer.valueOf(splitpair[1].substring(FILTER_TYPE_INT.length())));
                } else if (splitpair[1].startsWith(FILTER_TYPE_LONG)) {
                    filters.put(splitpair[0], Long.valueOf(splitpair[1].substring(FILTER_TYPE_LONG.length())));
                } else if (splitpair[1].startsWith(FILTER_TYPE_BOOL)) {
                    filters.put(splitpair[0], Boolean.valueOf(splitpair[1].substring(FILTER_TYPE_BOOL.length())));
                } else if (splitpair[1].startsWith(FILTER_TYPE_DOUBLE)) {
                    filters.put(splitpair[0], Double.valueOf(splitpair[1].substring(FILTER_TYPE_DOUBLE.length())));
                } else if (splitpair[1].startsWith(FILTER_TYPE_FLOAT)) {
                    filters.put(splitpair[0], Float.valueOf(splitpair[1].substring(FILTER_TYPE_FLOAT.length())));
                } else {
                    filters.put(splitpair[0], splitpair[1]);
                }
            }
        }
    }
}
