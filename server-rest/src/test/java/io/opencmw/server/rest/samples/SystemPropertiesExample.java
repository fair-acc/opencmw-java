package io.opencmw.server.rest.samples;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opencmw.utils.SystemProperties;

/**
 * Small usage example for SystemProperties command line interface
 * call with e.g. '--OpenCmwConstants.DNS_TIMEOUT=2' or '-h' or '--help' for help
 */
public class SystemPropertiesExample {
    private static final Logger LOGGER = LoggerFactory.getLogger(SystemPropertiesExample.class);

    public static void main(String[] argv) {
        if (argv.length == 0) {
            SystemProperties.parseOptions(new String[] { "-h" });
        }
        final Map<String, Object> cmdLineValues = SystemProperties.parseOptions(argv);
        if (!cmdLineValues.isEmpty()) {
            LOGGER.atInfo().addArgument(cmdLineValues).log("cmd line values set: {}");
        }

        LOGGER.atInfo().log("main finished");
    }
}
