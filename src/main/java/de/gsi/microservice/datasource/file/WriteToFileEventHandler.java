package de.gsi.microservice.datasource.file;

import com.lmax.disruptor.EventHandler;
import de.gsi.microservice.RingBufferEvent;
import de.gsi.microservice.filter.TimingCtx;
import de.gsi.serializer.IoClassSerialiser;
import de.gsi.serializer.spi.FastByteBuffer;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.file.Path;

/**
 * An event store processor, which safes every event to file.
 *
 * The contents of this directory can then be used by the {@link FileDataSource} for replay/testing/analysis.
 */
public class WriteToFileProcessor implements EventHandler<RingBufferEvent> {
    private static final int BUFFER_SIZE = 5000000;
    final IoClassSerialiser serialiser = new IoClassSerialiser(new FastByteBuffer(BUFFER_SIZE));
    final String path;

    public WriteToFileProcessor(final String path) {
        this.path = path;
    }

    @Override
    public void onEvent(final RingBufferEvent event, final long sequence, final boolean endOfBatch) throws Exception {
        // write events to file
        final TimingCtx timingCtx = event.getFilter(TimingCtx.class);
        // todo: get device/property from filter
        final String device = "";
        final String property = "";
        final Path filename = Path.of(path + '/' + device + '/' + property + '/' + timingCtx + ".bin.gz");
        try (OutputStream output = new FileOutputStream(filename.toFile())) {
            serialiser.getDataBuffer().reset();
            serialiser.serialiseObject(event.payload.get());
            output.write(serialiser.getDataBuffer().elements(), 0, serialiser.getDataBuffer().position());
        }
    }
}
