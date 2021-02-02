package io.opencmw.serialiser.benchmark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opencmw.serialiser.utils.CmwLightHelper;
import io.opencmw.serialiser.utils.FlatBuffersHelper;
import io.opencmw.serialiser.utils.JsonHelper;
import io.opencmw.serialiser.utils.SerialiserHelper;
import io.opencmw.serialiser.utils.TestDataClass;

/**
 * Simple (rough) benchmark of various internal and external serialiser protocols.
 * Test consists of a simple repeated POJO->serialised->byte[] buffer -> de-serialisation -> POJO + comparison checks.
 * N.B. this isn't as precise as the JMH tests but gives a rough idea whether the protocol degraded or needs to be improved.
 *
 * Example output - numbers should be compared relatively (nIterations = 100000):
 * (openjdk 11.0.7 2020-04-14, ASCII-only, nSizePrimitiveArrays = 10, nSizeString = 100, nestedClassRecursion = 1)
 * [..] more string-heavy TestDataClass
 * - run 1
 * - JSON Serializer (Map only)  throughput = 371.4 MB/s for 5.2 kB per test run (took 1413.0 ms)
 * - CMW Serializer (Map only) throughput = 220.2 MB/s for 6.3 kB per test run (took 2871.0 ms)
 * - CmwLight Serializer (Map only)  throughput = 683.1 MB/s for 6.4 kB per test run (took 935.0 ms)
 * - IO Serializer (Map only)  throughput = 810.0 MB/s for 7.4 kB per test run (took 908.0 ms)
 *
 * - FlatBuffers (custom FlexBuffers) throughput = 173.7 MB/s for 6.1 kB per test run (took 3536.0 ms)
 * - CmwLight Serializer (custom) throughput = 460.5 MB/s for 6.4 kB per test run (took 1387.0 ms)
 * - IO Serializer (custom) throughput = 545.0 MB/s for 7.3 kB per test run (took 1344.0 ms)
 *
 * - JSON Serializer (POJO) throughput = 53.8 MB/s for 5.2 kB per test run (took 9747.0 ms)
 * - CMW Serializer (POJO) throughput = 182.8 MB/s for 6.3 kB per test run (took 3458.0 ms)
 * - CmwLight Serializer (POJO) throughput = 329.2 MB/s for 6.3 kB per test run (took 1906.0 ms)
 * - IO Serializer (POJO) throughput = 374.9 MB/s for 7.2 kB per test run (took 1925.0 ms)
 *
 * [..] more primitive-array-heavy TestDataClass
 * (openjdk 11.0.7 2020-04-14, UTF8, nSizePrimitiveArrays = 1000, nSizeString = 0, nestedClassRecursion = 0)
 * - run 1
 * - JSON Serializer (Map only)  throughput = 350.7 MB/s for 34.3 kB per test run (took 9793.0 ms)
 * - CMW Serializer (Map only) throughput = 1.7 GB/s for 29.2 kB per test run (took 1755.0 ms)
 * - CmwLight Serializer (Map only)  throughput = 6.7 GB/s for 29.2 kB per test run (took 437.0 ms)
 * - IO Serializer (Map only)  throughput = 6.1 GB/s for 29.7 kB per test run (took 485.0 ms)
 *
 * - FlatBuffers (custom FlexBuffers) throughput = 123.1 MB/s for 30.1 kB per test run (took 24467.0 ms)
 * - CmwLight Serializer (custom) throughput = 3.9 GB/s for 29.2 kB per test run (took 751.0 ms)
 * - IO Serializer (custom) throughput = 3.8 GB/s for 29.7 kB per test run (took 782.0 ms)
 *
 * - JSON Serializer (POJO) throughput = 31.7 MB/s for 34.3 kB per test run (took 108415.0 ms)
 * - CMW Serializer (POJO) throughput = 1.5 GB/s for 29.2 kB per test run (took 1924.0 ms)
 * - CmwLight Serializer (POJO) throughput = 3.5 GB/s for 29.1 kB per test run (took 824.0 ms)
 * - IO Serializer (POJO) throughput = 3.4 GB/s for 29.7 kB per test run (took 870.0 ms)
 *
 * @author rstein
 */
public class SerialiserQuickBenchmark { // NOPMD - nomen est omen
    private static final Logger LOGGER = LoggerFactory.getLogger(SerialiserQuickBenchmark.class);
    private static final TestDataClass inputObject = new TestDataClass(10, 100, 1);
    private static final TestDataClass outputObject = new TestDataClass(-1, -1, 0);

    public static String humanReadableByteCount(final long bytes, final boolean si) {
        final int unit = si ? 1000 : 1024;
        if (bytes < unit) {
            return bytes + " B";
        }

        final int exp = (int) (Math.log(bytes) / Math.log(unit));
        final String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp - 1) + (si ? "" : "i");
        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }

    public static void main(final String... argv) {
        // CmwHelper.checkSerialiserIdentity(inputObject, outputObject);
        LOGGER.atInfo().addArgument(CmwLightHelper.checkSerialiserIdentity(inputObject, outputObject)).log("CmwLight serialiser nBytes = {}");
        LOGGER.atInfo().addArgument(CmwLightHelper.checkCustomSerialiserIdentity(inputObject, outputObject)).log("CmwLight (custom) serialiser nBytes = {}");

        LOGGER.atInfo().addArgument(JsonHelper.checkSerialiserIdentity(inputObject, outputObject)).log("JSON Serialiser serialiser nBytes = {}");
        LOGGER.atInfo().addArgument(JsonHelper.checkCustomSerialiserIdentity(inputObject, outputObject)).log("JSON Serialiser (custom) serialiser nBytes = {}");
        LOGGER.atInfo().addArgument(SerialiserHelper.checkSerialiserIdentity(inputObject, outputObject)).log("generic serialiser nBytes = {}");
        LOGGER.atInfo().addArgument(SerialiserHelper.checkCustomSerialiserIdentity(inputObject, outputObject)).log("generic serialiser (custom) nBytes = {}");
        LOGGER.atInfo().addArgument(FlatBuffersHelper.checkCustomSerialiserIdentity(inputObject, outputObject)).log("flatBuffers serialiser nBytes = {}");

        // Cmw vs. CmwLight compatibility - requires CMW binary libs
        // CmwLightHelper.checkCmwLightVsCmwIdentityForward(inputObject, outputObject);
        // CmwLightHelper.checkCmwLightVsCmwIdentityBackward(inputObject, outputObject);

        // optimisation to be enabled if e.g. to protocols that do not support UTF-8 string encoding
        // CmwLightHelper.getCmwLightSerialiser().setEnforceSimpleStringEncoding(true);
        // SerialiserHelper.getBinarySerialiser().setEnforceSimpleStringEncoding(true);
        // SerialiserHelper.getBinarySerialiser().setPutFieldMetaData(false);

        final int nIterations = 100000;
        for (int i = 0; i < 10; i++) {
            LOGGER.atInfo().addArgument(i).log("run {}");
            // map-only performance
            JsonHelper.testSerialiserPerformanceMap(nIterations, inputObject);
            // CmwHelper.testSerialiserPerformanceMap(nIterations, inputObject, outputObject);
            CmwLightHelper.testSerialiserPerformanceMap(nIterations, inputObject);
            SerialiserHelper.testSerialiserPerformanceMap(nIterations, inputObject);

            // custom serialiser performance
            JsonHelper.testCustomSerialiserPerformance(nIterations, inputObject, outputObject);
            FlatBuffersHelper.testCustomSerialiserPerformance(nIterations, inputObject, outputObject);
            CmwLightHelper.testCustomSerialiserPerformance(nIterations, inputObject, outputObject);
            SerialiserHelper.testCustomSerialiserPerformance(nIterations, inputObject, outputObject);

            // POJO performance
            JsonHelper.testPerformancePojo(nIterations, inputObject, outputObject);
            JsonHelper.testPerformancePojoCodeGen(nIterations, inputObject, outputObject);
            // CmwHelper.testPerformancePojo(nIterations, inputObject, outputObject);
            CmwLightHelper.testPerformancePojo(nIterations, inputObject, outputObject);
            SerialiserHelper.testPerformancePojo(nIterations, inputObject, outputObject);
        }
    }
}
