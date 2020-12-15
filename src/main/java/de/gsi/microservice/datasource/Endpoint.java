package de.gsi.microservice.datasource;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * Endpoint helper class to deserialise endpoint strings.
 * Uses lazy initialisation to prevent doing unnecessary work or doing the same thing twice.
 */
public class Endpoint {
    private static final String DEFAULT_SELECTOR = "";
    public static final String FILTER_TYPE_LONG = "long:";
    public static final String FILTER_TYPE_INT = "int:";
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
        if (protocol == null) parse();
        return protocol;
    }

    public String toString(){
        return value;
    }

    public String getAddress() {
        if (address == null) parse();
        return address;
    }

    public String getDevice() {
        if (device == null) parse();
        return device;
    }

    public String getSelector() {
        if (selector == null) parse();
        return selector;
    }

    public String getProperty() {
        return property;
    }

    public Map<String, Object> getFilters() {
        return filters;
    }

    public String getEndpointForContext(final String context) {
        if (context == null || context.equals("")) return value;
        parse();
        return address + '/' + device + '/' + property + "?ctx=" + context + '&' + filters.entrySet().stream().map(e ->{
            String val;
            if (e.getValue() instanceof String) {
                val = (String) e.getValue();
            } else if (e.getValue() instanceof Integer) {
                val = FILTER_TYPE_INT + e.getValue();
            } else if (e.getValue() instanceof Long) {
                val = FILTER_TYPE_LONG + e.getValue();
            } else {
                throw new UnsupportedOperationException("Data type not supported in endpoint filters");
            }
            return e.getKey() + '=' + val;
        }).collect(Collectors.joining("&"));
    }

    private void parse() {
        final String[] tmp = value.split("\\?", 2); // split into address/dev/prop and sel+filters part
        final String[] adp = tmp[0].split("/"); // split access point into parts
        device= adp[adp.length - 2]; // get device name from access point
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
            if (splitpair[0].equals("ctx")) {
                selector = splitpair[1];
            } else {
                if (splitpair[1].startsWith(FILTER_TYPE_INT)) {
                    filters.put(splitpair[0], Integer.valueOf(splitpair[1].substring(FILTER_TYPE_INT.length())));
                } else if (splitpair[1].startsWith(FILTER_TYPE_LONG)) {
                    filters.put(splitpair[0], Long.valueOf(splitpair[1].substring(FILTER_TYPE_LONG.length())));
                } else {
                    filters.put(splitpair[0], splitpair[1]);
                }
            }
        }
    }
}
