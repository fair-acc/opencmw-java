package io.opencmw.concepts;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;
import org.zeromq.ZMsg;

/**
 * Quick performance evaluation to see the impact of single large w.r.t. many small frames.
 */
@SuppressWarnings({ "PMD.DoNotUseThreads", "PMD.AvoidInstantiatingObjectsInLoops" })
public class ManyVsLargeFrameEvaluation {
    private static final AtomicBoolean RUN = new AtomicBoolean(true);
    private static final byte[] CLIENT_ID = "C".getBytes(StandardCharsets.UTF_8); // client name
    private static final byte[] WORKER_ID = "W".getBytes(StandardCharsets.UTF_8); // worker-service name
    public static final char TAG_INTERNAL = 'I';
    public static final char TAG_EXTERNAL = 'E';
    public static final String TAG_EXTERNAL_STRING = "E";
    public static final String TAG_EXTERNAL_INTERNAL = "I";
    private static final boolean VERBOSE_PRINTOUT = true;
    private static int sampleSize = 100_000;
    private static final int N_BUFFER_SIZE = 8;
    private static final int N_FRAMES = 10;
    public static final byte[] smallMessage = new byte[N_BUFFER_SIZE * N_FRAMES]; // NOPMD - volatile on purpose
    public static final byte[] largeMessage = new byte[N_BUFFER_SIZE]; // NOPMD - volatile on purpose

    private static final int N_LOOPS = 5;

    private ManyVsLargeFrameEvaluation() {
        // utility class
    }

    public static void main(String[] args) {
        if (args.length == 1) {
            sampleSize = Integer.parseInt(args[0]);
        }
        Thread brokerThread = new Thread(new Broker());
        Thread workerThread = new Thread(new RoundTripAndNotifyEvaluation.Worker());
        brokerThread.start();
        workerThread.start();

        Thread clientThread = new Thread(new Client());
        clientThread.start();

        try {
            clientThread.join();
            RUN.set(false);
            workerThread.interrupt();
            brokerThread.interrupt();

            // wait for threads to finish
            workerThread.join();
            brokerThread.join();
        } catch (InterruptedException e) {
            // finishes tests
            assert false : "should not reach here if properly executed";
        }

        if (VERBOSE_PRINTOUT) {
            System.out.println("finished tests");
        }
    }

    private static void measure(final String topic, final int nExec, final Runnable... runnable) {
        final long start = System.currentTimeMillis();

        for (Runnable run : runnable) {
            for (int i = 0; i < nExec; i++) {
                run.run();
            }
        }

        final long stop = System.currentTimeMillis();
        if (VERBOSE_PRINTOUT) {
            System.out.printf("%-40s:  %10d calls/second\n", topic, (1000L * nExec) / (stop - start));
        }
    }

    private static class Broker implements Runnable {
        private static final int TIMEOUT = 1000;

        @Override
        public void run() { // NOPMD single-loop broker ... simplifies reading
            try (ZContext ctx = new ZContext();
                    Socket tcpFrontend = ctx.createSocket(SocketType.ROUTER);
                    Socket tcpBackend = ctx.createSocket(SocketType.ROUTER);
                    Socket inprocBackend = ctx.createSocket(SocketType.ROUTER);
                    ZMQ.Poller items = ctx.createPoller(3)) {
                tcpFrontend.setHWM(0);
                tcpBackend.setHWM(0);
                inprocBackend.setHWM(0);
                tcpFrontend.bind("tcp://*:5555");
                tcpBackend.bind("tcp://*:5556");
                inprocBackend.bind("inproc://broker");

                Thread internalWorkerThread = new Thread(new InternalWorker(ctx));
                internalWorkerThread.setDaemon(true);
                internalWorkerThread.start();
                items.register(tcpFrontend, ZMQ.Poller.POLLIN);
                items.register(tcpBackend, ZMQ.Poller.POLLIN);
                items.register(inprocBackend, ZMQ.Poller.POLLIN);

                while (RUN.get() && !Thread.currentThread().isInterrupted()) {
                    if (items.poll(TIMEOUT) == -1) {
                        break; // Interrupted
                    }

                    if (items.pollin(0)) {
                        ZMsg msg = ZMsg.recvMsg(tcpFrontend);
                        if (msg == null) {
                            break; // Interrupted
                        }

                        final ZFrame address = msg.pop();
                        if (address.getData()[0] != CLIENT_ID[0]) {
                            address.destroy();
                            break;
                        }
                        final ZFrame internal = msg.pop();
                        if (TAG_EXTERNAL == internal.getData()[0]) {
                            msg.addFirst(new ZFrame(WORKER_ID)); // NOPMD - necessary to allocate inside loop
                            msg.send(tcpBackend);
                        } else if (TAG_INTERNAL == internal.getData()[0]) {
                            msg.addFirst(new ZFrame(WORKER_ID));
                            msg.send(inprocBackend);
                        }
                        address.destroy();
                    }

                    if (items.pollin(1)) {
                        ZMsg msg = ZMsg.recvMsg(tcpBackend);
                        if (msg == null) {
                            break; // Interrupted
                        }
                        ZFrame address = msg.pop();

                        if (address.getData()[0] == WORKER_ID[0]) {
                            msg.addFirst(new ZFrame(CLIENT_ID));
                        }
                        msg.send(tcpFrontend);
                        address.destroy();
                    }

                    if (items.pollin(2)) {
                        final ZMsg msg = ZMsg.recvMsg(inprocBackend);
                        if (msg == null) {
                            break; // Interrupted
                        }
                        final ZFrame address = msg.pop();

                        if (address.getData()[0] == WORKER_ID[0]) {
                            msg.addFirst(new ZFrame(CLIENT_ID));
                        }
                        address.destroy();
                        msg.send(tcpFrontend);
                    }

                    items.close();
                }

                internalWorkerThread.interrupt();
                if (!internalWorkerThread.isInterrupted()) {
                    internalWorkerThread.join();
                }
            } catch (InterruptedException | IllegalStateException e) {
                // terminated broker via interrupt
            }
        }

        private static class InternalWorker implements Runnable {
            private final ZContext ctx;

            private InternalWorker(ZContext ctx) {
                this.ctx = ctx;
            }

            @Override
            public void run() {
                try (Socket worker = ctx.createSocket(SocketType.DEALER)) {
                    worker.setHWM(0);
                    worker.setIdentity(WORKER_ID);
                    worker.connect("inproc://broker");
                    while (RUN.get() && !Thread.currentThread().isInterrupted()) {
                        ZMsg msg = ZMsg.recvMsg(worker);
                        msg.send(worker);
                    }
                } catch (ZMQException e) {
                    // terminate internal worker
                }
            }
        }
    }

    private static class Client implements Runnable {
        @Override
        public void run() { // NOPMD -- complexity
            try (ZContext ctx = new ZContext();
                    Socket client = ctx.createSocket(SocketType.DEALER);
                    Socket subClient = ctx.createSocket(SocketType.SUB)) {
                client.setHWM(0);
                client.setIdentity(CLIENT_ID);
                client.connect("tcp://localhost:5555");
                subClient.setHWM(0);
                subClient.connect("tcp://localhost:5557");

                if (VERBOSE_PRINTOUT) {
                    System.out.println("Setting up test");
                }

                for (int l = 0; l < N_LOOPS; l++) {
                    for (final boolean external : new boolean[] { true, false }) {
                        final String inOut = external ? "TCP   " : "InProc";
                        measure(" Synchronous round-trip test (" + inOut + ", large frames)", sampleSize, () -> {
                            ZMsg req = new ZMsg();
                            req.addString(external ? TAG_EXTERNAL_STRING : TAG_EXTERNAL_INTERNAL);
                            for (int i = 0; i < N_FRAMES; i++) {
                                req.add(smallMessage);
                            }
                            req.send(client);
                            ZMsg.recvMsg(client).destroy();
                        });

                        measure("Asynchronous round-trip test (" + inOut + ", large frames)", sampleSize, () -> {
                            // send messages
                            ZMsg req = new ZMsg();
                            req.addString(external? TAG_EXTERNAL_STRING : TAG_EXTERNAL_INTERNAL);
                            for (int i = 0; i < N_FRAMES; i++) {
                                req.add(smallMessage);
                            }
                            req.send(client); }, () -> {
                            // receive messages
                            ZMsg.recvMsg(client).destroy(); });

                        measure(" Synchronous round-trip test (" + inOut + ", many frames)", sampleSize, () -> {
                            ZMsg req = new ZMsg();
                            req.addString(external ? TAG_EXTERNAL_STRING : TAG_EXTERNAL_INTERNAL);
                            req.add(largeMessage);
                            req.send(client);
                            ZMsg.recvMsg(client).destroy();
                        });

                        measure("Asynchronous round-trip test (" + inOut + ", many frames)", sampleSize, () -> {
                            // send messages
                            ZMsg req = new ZMsg();
                            req.addString(external? TAG_EXTERNAL_STRING : TAG_EXTERNAL_INTERNAL);
                            req.add(largeMessage);
                            req.send(client); }, () -> {
                            // receive messages
                            ZMsg.recvMsg(client).destroy(); });
                    }
                }
            } catch (ZMQException e) {
                if (VERBOSE_PRINTOUT) {
                    System.out.println("terminate client");
                }
            }
        }
    }

    private static class Worker implements Runnable {
        @Override
        public void run() {
            try (ZContext ctx = new ZContext();
                    Socket worker = ctx.createSocket(SocketType.DEALER)) {
                worker.setHWM(0);
                worker.setIdentity(WORKER_ID);
                worker.connect("tcp://localhost:5556");
                while (RUN.get() && !Thread.currentThread().isInterrupted()) {
                    ZMsg msg = ZMsg.recvMsg(worker);
                    msg.send(worker);
                }
            } catch (ZMQException e) {
                // terminate worker
            }
        }
    }
}
