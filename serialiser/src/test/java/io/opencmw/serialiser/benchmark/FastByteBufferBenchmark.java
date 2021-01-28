package io.opencmw.serialiser.benchmark;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import io.opencmw.serialiser.spi.FastByteBuffer;

/**
 * Simple benchmark to evaluate the effect of range checks on FastByteBuffer performance
 *
 * Without range check:
 * FastByteBufferBenchmark.putInts  thrpt   10  40419,996 ± 3480,205  ops/s
 * With range check:
 * FastByteBufferBenchmark.putInts  thrpt   10  40108,335 ±  912,071  ops/s
 *
 * @author akrimm
 */
@State(Scope.Benchmark)
public class FastByteBufferBenchmark {
    private static final FastByteBuffer fastByteBuffer = new FastByteBuffer(200_000);

    @Benchmark
    @Warmup(iterations = 1)
    @Fork(value = 2, warmups = 2)
    public void putInts(Blackhole blackhole) {
        fastByteBuffer.reset();
        for (int i = 0; i < 200_000 / 16; ++i) {
            fastByteBuffer.putInt(blackhole.i1);
            fastByteBuffer.putInt(blackhole.i2);
        }
        blackhole.consume(fastByteBuffer.elements());
    }
}
