package io.opencmw.serialiser.benchmark;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import io.opencmw.serialiser.IoClassSerialiser;
import io.opencmw.serialiser.spi.BinarySerialiser;
import io.opencmw.serialiser.spi.ByteBuffer;
import io.opencmw.serialiser.spi.FastByteBuffer;

import de.gsi.dataset.DataSet;
import de.gsi.dataset.spi.DoubleDataSet;
import de.gsi.dataset.testdata.spi.GaussFunction;

/**
 * Simple benchmark to verify that the in-place DataSet (de-)serialiser is not significantly slower than creating a new DataSet
 *
 * Benchmark                                                                Mode  Cnt     Score     Error  Units
 * DataSetSerialiserBenchmark.serialiserRoundTripByteBufferInplace         thrpt   10  5971.023 ± 100.145  ops/s
 * DataSetSerialiserBenchmark.serialiserRoundTripByteBufferNewDataSet      thrpt   10  5652.462 ± 114.474  ops/s
 * DataSetSerialiserBenchmark.serialiserRoundTripFastByteBufferInplace     thrpt   10  7468.360 ± 126.494  ops/s
 * DataSetSerialiserBenchmark.serialiserRoundTripFastByteBufferNewDataSet  thrpt   10  7272.097 ± 170.991  ops/s
 *
 * @author rstein
 */
@State(Scope.Benchmark)
public class DataSetSerialiserBenchmark {
    private static final IoClassSerialiser serialiserFastByteBuffer = new IoClassSerialiser(new FastByteBuffer(200_000), BinarySerialiser.class);
    private static final IoClassSerialiser serialiserByteBuffer = new IoClassSerialiser(new ByteBuffer(200_000), BinarySerialiser.class);
    private static final DataSet srcDataSet = new DoubleDataSet(new GaussFunction("Gauss-function", 10_000));
    private static final DataSet copyDataSet = new DoubleDataSet(srcDataSet);
    private static final TestClass source = new TestClass();
    private static final TestClass copy = new TestClass();

    @Benchmark
    @Warmup(iterations = 1)
    @Fork(value = 2, warmups = 2)
    public void serialiserRoundTripByteBufferInplace(Blackhole blackhole) {
        final IoClassSerialiser classSerialiser = serialiserByteBuffer;
        source.dataSet = srcDataSet;
        copy.dataSet = copyDataSet;

        // serialise-deserialise DataSet
        classSerialiser.getDataBuffer().reset(); // '0' writing at start of buffer
        classSerialiser.serialiseObject(source);

        classSerialiser.getDataBuffer().flip(); // reset to read position (==0)
        classSerialiser.deserialiseObject(copy);

        blackhole.consume(copy);
    }

    @Benchmark
    @Warmup(iterations = 1)
    @Fork(value = 2, warmups = 2)
    public void serialiserRoundTripByteBufferNewDataSet(Blackhole blackhole) {
        final IoClassSerialiser classSerialiser = serialiserByteBuffer;
        source.dataSet = srcDataSet;
        copy.dataSet = null;

        // serialise-deserialise DataSet
        classSerialiser.getDataBuffer().reset(); // '0' writing at start of buffer
        classSerialiser.serialiseObject(source);

        classSerialiser.getDataBuffer().flip(); // reset to read position (==0)
        classSerialiser.deserialiseObject(copy);

        blackhole.consume(copy);
    }

    @Benchmark
    @Warmup(iterations = 1)
    @Fork(value = 2, warmups = 2)
    public void serialiserRoundTripFastByteBufferInplace(Blackhole blackhole) {
        final IoClassSerialiser classSerialiser = serialiserFastByteBuffer;
        source.dataSet = srcDataSet;
        copy.dataSet = copyDataSet;

        // serialise-deserialise DataSet
        classSerialiser.getDataBuffer().reset(); // '0' writing at start of buffer
        classSerialiser.serialiseObject(source);

        classSerialiser.getDataBuffer().flip(); // reset to read position (==0)
        classSerialiser.deserialiseObject(copy);

        blackhole.consume(copy);
    }

    @Benchmark
    @Warmup(iterations = 1)
    @Fork(value = 2, warmups = 2)
    public void serialiserRoundTripFastByteBufferNewDataSet(Blackhole blackhole) {
        final IoClassSerialiser classSerialiser = serialiserFastByteBuffer;
        source.dataSet = srcDataSet;
        copy.dataSet = null;

        // serialise-deserialise DataSet
        classSerialiser.getDataBuffer().reset(); // '0' writing at start of buffer
        classSerialiser.serialiseObject(source);

        classSerialiser.getDataBuffer().flip(); // reset to read position (==0)
        classSerialiser.deserialiseObject(copy);

        blackhole.consume(copy);
    }

    static class TestClass {
        public DataSet dataSet;
    }
}
