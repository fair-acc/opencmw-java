package io.opencmw.server;

import static io.opencmw.OpenCmwProtocol.MdpMessage;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import io.opencmw.serialiser.IoBuffer;
import io.opencmw.serialiser.IoClassSerialiser;
import io.opencmw.serialiser.spi.BinarySerialiser;
import io.opencmw.serialiser.spi.FastByteBuffer;

public class MajordomoTestClientSubscription<T> extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(MajordomoTestClientSubscription.class);
    private static final AtomicInteger CLIENT_V1_INSTANCE = new AtomicInteger();
    private static final AtomicBoolean run = new AtomicBoolean(false);
    private final IoBuffer ioBuffer = new FastByteBuffer(4000, true, null);
    private final IoClassSerialiser serialiser = new IoClassSerialiser(ioBuffer);
    private final String uniqueID;
    private final byte[] uniqueIdBytes;
    private final String broker;
    private final ZContext ctx;
    private ZMQ.Socket clientSocket;
    public List<MdpMessage> mdpMessages = Collections.synchronizedList(new ArrayList<>());
    public List<T> domainMessages = Collections.synchronizedList(new ArrayList<>());
    public final Class<T> classType;
    public long timeout = 100; // [ms] for testing purposes, production should choose something along the heartbeat period

    @SafeVarargs
    public MajordomoTestClientSubscription(final @NotNull String broker, final @NotNull String clientName, final @NotNull Class<T>... classType) {
        this.broker = broker;
        this.classType = classType.length > 0 ? classType[0] : null;
        ctx = new ZContext();
        uniqueID = clientName + "PID=" + ManagementFactory.getRuntimeMXBean().getName() + "-InstanceID=" + CLIENT_V1_INSTANCE.getAndIncrement();
        uniqueIdBytes = uniqueID.getBytes(ZMQ.CHARSET);

        serialiser.setAutoMatchSerialiser(false);
        serialiser.setMatchedIoSerialiser(BinarySerialiser.class);

        setDaemon(true);
        start();
        synchronized (run) {
            try {
                run.wait(TimeUnit.MILLISECONDS.toMillis(timeout));
            } catch (InterruptedException e) {
                LOGGER.atError().setCause(e).addArgument(clientName).log("could not initialise subscription client '{}'");
            }
        }
        LOGGER.atDebug().addArgument(broker).log("connecting to broker at: '{}'");
    }

    public void run() {
        try (ZMQ.Socket socket = ctx.createSocket(SocketType.SUB); ZMQ.Poller poller = ctx.createPoller(1)) {
            clientSocket = socket;
            socket.setHWM(0);
            socket.setIdentity(uniqueIdBytes);
            socket.connect(broker.replace("mds://", "tcp://"));
            poller.register(socket, ZMQ.Poller.POLLIN);
            synchronized (run) {
                run.set(true);
                run.notifyAll();
            }
            while (run.get() && !Thread.interrupted()) {
                if (poller.poll(timeout) == -1) {
                    return; // Interrupted
                }

                boolean dataReceived = true;
                while (dataReceived && !Thread.interrupted()) {
                    dataReceived = false;
                    final MdpMessage msg = MdpMessage.receive(socket, false);
                    if (msg == null) {
                        continue;
                    }
                    handleReceivedMessage(msg);
                    dataReceived = true;
                }
            }
        }

    }

    private void handleReceivedMessage(final MdpMessage msg) {
        // add msg to generic queue
        mdpMessages.add(0, msg);
        // add message to specific queue
        if (classType == null) {
            return;
        }
        if (!msg.errors.isBlank()) {
            throw new IllegalStateException("cannot deserialise message: " + msg);
        }
        serialiser.setDataBuffer(FastByteBuffer.wrap(msg.data));
        final T domainObject = serialiser.deserialiseObject(classType);
        domainMessages.add(0, domainObject);
    }

    public void clear() {
        mdpMessages.clear();
        domainMessages.clear();
    }

    public void stopClient() {
        run.set(false);
    }

    public void subscribe(final @NotNull String topic) {
        clientSocket.subscribe(topic);
    }

    public void unsubscribe(final @NotNull String topic) {
        clientSocket.unsubscribe(topic);
    }
}
