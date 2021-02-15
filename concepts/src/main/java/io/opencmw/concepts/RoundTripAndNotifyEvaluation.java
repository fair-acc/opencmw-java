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
 * Quick Router-Dealer Round-trip demonstrator.
 * Broker, Worker and Client are mocked as separate threads.
 *
 * Example output:
 * Setting up test
 * Synchronous round-trip test (TCP)       :        3838 calls/second
 * Synchronous round-trip test (InProc)    :       12224 calls/second
 * Asynchronous round-trip test (TCP)      :       33444 calls/second
 * Asynchronous round-trip test (InProc)   :       35587 calls/second
 * Subscription (SUB) test                 :      632911 calls/second
 * Subscription (DEALER) test (TCP)        :       43821 calls/second
 * Subscription (DEALER) test (InProc)     :       49900 calls/second
 * Subscription (direct DEALER) test       :     1351351 calls/second
 * finished tests
 *
 * N.B. for >200000 calls/second the code seems to depend largely on the broker/parameters
 * (ie. JIT, whether services are identified by single characters etc.)
 */
@SuppressWarnings("PMD.DoNotUseThreads")
class RoundTripAndNotifyEvaluation {
    // private static final String SUB_TOPIC = "x";
    private static final String SUB_TOPIC = "<domain>/<property>?<filter>#<ctx> - a very long topic to test the dependence of pub/sub pairs on topic lengths";
    private static final byte[] SUB_DATA = "D".getBytes(StandardCharsets.UTF_8); // custom minimal data
    private static final byte[] CLIENT_ID = "C".getBytes(StandardCharsets.UTF_8); // client name
    private static final byte[] WORKER_ID = "W".getBytes(StandardCharsets.UTF_8); // worker-service name
    private static final byte[] PUBLISH_ID = "P".getBytes(StandardCharsets.UTF_8); // publish-service name
    private static final byte[] SUBSCRIBER_ID = "S".getBytes(StandardCharsets.UTF_8); // subscriber name
    public static final char TAG_INTERNAL = 'I';
    public static final char TAG_EXTERNAL = 'E';
    public static final String TAG_EXTERNAL_STRING = "E";
    public static final String TAG_EXTERNAL_INTERNAL = "I";
    public static final String START = "start";
    public static final String DUMMY_DATA = "hello";
    public static final String TCP_LOCALHOST_5555 = "tcp://localhost:5555";
    private static final AtomicBoolean RUN = new AtomicBoolean(true);
    private static final boolean VERBOSE_PRINTOUT = false;
    private static int sampleSize = 10_000;
    private static int sampleSizePub = 100_000;

    private RoundTripAndNotifyEvaluation() {
        // utility class
    }

    public static void main(String[] args) {
        if (args.length == 1) {
            sampleSize = Integer.parseInt(args[0]);
            sampleSizePub = 10 * sampleSize;
        }
        Thread brokerThread = new Thread(new Broker());
        Thread workerThread = new Thread(new Worker());
        Thread publishThread = new Thread(new PublishWorker());
        Thread pubDealerThread = new Thread(new PublishDealerWorker());
        Thread directDealerThread = new Thread(new PublishDirectDealerWorker());

        brokerThread.start();
        workerThread.start();
        publishThread.start();
        pubDealerThread.start();
        directDealerThread.start();

        Thread clientThread = new Thread(new Client());
        clientThread.start();

        try {
            clientThread.join();
            RUN.set(false);
            workerThread.interrupt();
            brokerThread.interrupt();
            publishThread.interrupt();
            pubDealerThread.interrupt();
            directDealerThread.interrupt();

            // wait for threads to finish
            workerThread.join();
            publishThread.join();
            pubDealerThread.join();
            directDealerThread.join();
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
            try (ZContext ctx = new ZContext()) {
                final Socket tcpFrontend = ctx.createSocket(SocketType.ROUTER);
                final Socket tcpBackend = ctx.createSocket(SocketType.ROUTER);
                final Socket inprocBackend = ctx.createSocket(SocketType.ROUTER);
                tcpFrontend.setHWM(0);
                tcpBackend.setHWM(0);
                inprocBackend.setHWM(0);
                tcpFrontend.bind("tcp://*:5555");
                tcpBackend.bind("tcp://*:5556");
                inprocBackend.bind("inproc://broker");

                Thread internalWorkerThread = new Thread(new InternalWorker(ctx));
                internalWorkerThread.setDaemon(true);
                internalWorkerThread.start();
                Thread internalPublishDealerWorkerThread = new Thread(new InternalPublishDealerWorker(ctx));
                internalPublishDealerWorkerThread.setDaemon(true);
                internalPublishDealerWorkerThread.start();

                while (RUN.get() && !Thread.currentThread().isInterrupted()) {
                    // create poller
                    final ZMQ.Poller items = ctx.createPoller(3);
                    items.register(tcpFrontend, ZMQ.Poller.POLLIN);
                    items.register(tcpBackend, ZMQ.Poller.POLLIN);
                    items.register(inprocBackend, ZMQ.Poller.POLLIN);

                    if (items.poll(TIMEOUT) == -1) {
                        break; // Interrupted
                    }

                    if (items.pollin(0)) {
                        final ZMsg msg = ZMsg.recvMsg(tcpFrontend);
                        if (msg == null) {
                            break; // Interrupted
                        }
                        final ZFrame address = msg.pop();
                        final ZFrame internal = msg.pop();
                        if (address.getData()[0] == CLIENT_ID[0]) {
                            if (TAG_EXTERNAL == internal.getData()[0]) {
                                msg.addFirst(new ZFrame(WORKER_ID));
                                msg.send(tcpBackend);
                            } else if (TAG_INTERNAL == internal.getData()[0]) {
                                msg.addFirst(new ZFrame(WORKER_ID));
                                msg.send(inprocBackend);
                            }
                        } else {
                            if (TAG_EXTERNAL == internal.getData()[0]) {
                                msg.addFirst(new ZFrame(PUBLISH_ID)); // NOPMD - necessary to allocate inside loop
                                msg.send(tcpBackend);
                            } else if (TAG_INTERNAL == internal.getData()[0]) {
                                msg.addFirst(new ZFrame(PUBLISH_ID)); // NOPMD - necessary to allocate inside loop
                                msg.send(inprocBackend);
                            }
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
                            msg.addFirst(new ZFrame(CLIENT_ID)); // NOPMD - necessary to allocate inside loop
                        } else {
                            msg.addFirst(new ZFrame(SUBSCRIBER_ID)); // NOPMD - necessary to allocate inside loop
                        }
                        msg.send(tcpFrontend);
                        address.destroy();
                    }

                    if (items.pollin(2)) {
                        final ZMsg msg = ZMsg.recvMsg(inprocBackend);
                        if (msg == null) {
                            break; // Interrupted
                        }
                        ZFrame address = msg.pop();

                        if (address.getData()[0] == WORKER_ID[0]) {
                            msg.addFirst(new ZFrame(CLIENT_ID)); // NOPMD - necessary to allocate inside loop
                        } else {
                            msg.addFirst(new ZFrame(SUBSCRIBER_ID)); // NOPMD - necessary to allocate inside loop
                        }
                        address.destroy();
                        msg.send(tcpFrontend);
                    }

                    items.close();
                }

                internalWorkerThread.interrupt();
                internalPublishDealerWorkerThread.interrupt();
                if (!internalWorkerThread.isInterrupted()) {
                    internalWorkerThread.join();
                }
                if (!internalPublishDealerWorkerThread.isInterrupted()) {
                    internalPublishDealerWorkerThread.join();
                }
            } catch (InterruptedException | IllegalStateException e) {
                // terminated broker via interrupt
            }
        }

        private static class InternalWorker implements Runnable {
            private final ZContext ctx;
            public InternalWorker(ZContext ctx) {
                this.ctx = ctx;
            }

            @Override
            public void run() {
                try {
                    Socket worker = ctx.createSocket(SocketType.DEALER);
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

        private static class InternalPublishDealerWorker implements Runnable {
            private final ZContext ctx;
            public InternalPublishDealerWorker(ZContext ctx) {
                this.ctx = ctx;
            }

            @Override
            public void run() {
                try {
                    Socket worker = ctx.createSocket(SocketType.DEALER);
                    worker.setHWM(0);
                    worker.setIdentity(PUBLISH_ID);
                    worker.connect("inproc://broker");
                    while (RUN.get() && !Thread.currentThread().isInterrupted()) {
                        ZMsg msg = ZMsg.recvMsg(worker);
                        if (START.equals(msg.getFirst().getString(ZMQ.CHARSET))) {
                            // System.err.println("dealer (indirect): start pushing");
                            for (int requests = 0; requests < sampleSizePub; requests++) {
                                worker.send(SUB_TOPIC, ZMQ.SNDMORE);
                                worker.send(SUB_DATA);
                            }
                        }
                    }
                } catch (ZMQException | IllegalStateException e) {
                    // terminate internal publish worker
                }
            }
        }
    }

    protected static class Worker implements Runnable {
        @Override
        public void run() {
            try (ZContext ctx = new ZContext()) {
                Socket worker = ctx.createSocket(SocketType.DEALER);
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

    private static class PublishWorker implements Runnable {
        @Override
        public void run() {
            try (ZContext ctx = new ZContext()) {
                Socket worker = ctx.createSocket(SocketType.PUB);
                worker.setHWM(0);
                worker.bind("tcp://localhost:5557");
                // System.err.println("PublishWorker: start publishing");
                while (RUN.get() && !Thread.currentThread().isInterrupted()) {
                    worker.send(SUB_TOPIC, ZMQ.SNDMORE);
                    worker.send(SUB_DATA);
                }
            } catch (ZMQException | IllegalStateException e) {
                // terminate pub-Dealer worker
            }
        }
    }

    private static class PublishDealerWorker implements Runnable {
        @Override
        public void run() {
            try (ZContext ctx = new ZContext()) {
                Socket worker = ctx.createSocket(SocketType.DEALER);
                worker.setHWM(0);
                worker.setIdentity(PUBLISH_ID);
                //worker.bind("tcp://localhost:5558");
                worker.connect("tcp://localhost:5556");
                while (RUN.get() && !Thread.currentThread().isInterrupted()) {
                    ZMsg msg = ZMsg.recvMsg(worker);
                    if (START.equals(msg.getFirst().getString(ZMQ.CHARSET))) {
                        // System.err.println("dealer (indirect): start pushing");
                        for (int requests = 0; requests < sampleSizePub; requests++) {
                            worker.send(SUB_TOPIC, ZMQ.SNDMORE);
                            worker.send(SUB_DATA);
                        }
                    }
                }
            } catch (ZMQException | IllegalStateException e) {
                // terminate publish worker
            }
        }
    }

    private static class PublishDirectDealerWorker implements Runnable {
        @Override
        public void run() {
            try (ZContext ctx = new ZContext()) {
                Socket worker = ctx.createSocket(SocketType.DEALER);
                worker.setHWM(0);
                worker.setIdentity(PUBLISH_ID);
                worker.bind("tcp://localhost:5558");
                while (RUN.get() && !Thread.currentThread().isInterrupted()) {
                    ZMsg msg = ZMsg.recvMsg(worker);
                    if (START.equals(msg.getFirst().getString(ZMQ.CHARSET))) {
                        // System.err.println("dealer (direct): start pushing");
                        for (int requests = 0; requests < sampleSizePub; requests++) {
                            worker.send(SUB_TOPIC, ZMQ.SNDMORE);
                            worker.send(SUB_DATA);
                        }
                    }
                }
            } catch (ZMQException | IllegalStateException e) {
                // terminate publish worker
            }
        }
    }

    private static class Client implements Runnable {
        @Override
        public void run() {
            try (ZContext ctx = new ZContext()) {
                Socket client = ctx.createSocket(SocketType.DEALER);
                client.setHWM(0);
                client.setIdentity(CLIENT_ID);
                client.connect(TCP_LOCALHOST_5555);

                Socket subClient = ctx.createSocket(SocketType.SUB);
                subClient.setHWM(0);
                subClient.connect("tcp://localhost:5557");

                if (VERBOSE_PRINTOUT) {
                    System.out.println("Setting up test");
                }
                // check and wait until broker is up and running
                ZMsg.newStringMsg(TAG_EXTERNAL_STRING).addString(DUMMY_DATA).send(client);
                ZMsg.recvMsg(client).destroy();

                measure("Synchronous round-trip test (TCP)", sampleSize, () -> {
                    ZMsg req = new ZMsg();
                    req.addString(TAG_EXTERNAL_STRING);
                    req.addString(DUMMY_DATA);
                    req.send(client);
                    ZMsg.recvMsg(client).destroy();
                });

                measure("Synchronous round-trip test (InProc)", sampleSize, () -> {
                    ZMsg req = new ZMsg();
                    req.addString(TAG_EXTERNAL_INTERNAL);
                    req.addString(DUMMY_DATA);
                    req.send(client);
                    ZMsg.recvMsg(client).destroy();
                });

                measure("Asynchronous round-trip test (TCP)", sampleSize, () -> {
                    // send messages
                    ZMsg req = new ZMsg();
                    req.addString(TAG_EXTERNAL_STRING);
                    req.addString(DUMMY_DATA);
                    req.send(client); }, () -> {
                    // receive messages
                    ZMsg.recvMsg(client).destroy(); });

                measure("Asynchronous round-trip test (InProc)", sampleSize, () -> {
                    // send messages
                    ZMsg req = new ZMsg();
                    req.addString(TAG_EXTERNAL_INTERNAL);
                    req.addString(DUMMY_DATA);
                    req.send(client); }, () -> {
                    // receive messages
                    ZMsg.recvMsg(client).destroy(); });

                subClient.subscribe(SUB_TOPIC.getBytes(ZMQ.CHARSET));
                // first loop to empty potential queues/HWM
                for (int requests = 0; requests < sampleSizePub; requests++) {
                    ZMsg req = ZMsg.recvMsg(subClient);
                    req.destroy();
                }
                // start actual subscription loop
                measure("Subscription (SUB) test", sampleSizePub, () -> {
                    ZMsg req = ZMsg.recvMsg(subClient);
                    req.destroy();
                });
                subClient.unsubscribe(SUB_TOPIC.getBytes(ZMQ.CHARSET));

                client.disconnect(TCP_LOCALHOST_5555);
                client.setIdentity(SUBSCRIBER_ID);
                client.connect(TCP_LOCALHOST_5555);
                ZMsg.newStringMsg(START).addFirst(TAG_EXTERNAL_STRING).send(client);
                measure("Subscription (DEALER) test (TCP)", sampleSizePub, () -> {
                    ZMsg req = ZMsg.recvMsg(client);
                    req.destroy();
                });

                ZMsg.newStringMsg(START).addFirst(TAG_EXTERNAL_INTERNAL).send(client);
                measure("Subscription (DEALER) test (InProc)", sampleSizePub, () -> {
                    ZMsg req = ZMsg.recvMsg(client);
                    req.destroy();
                });

                client.disconnect(TCP_LOCALHOST_5555);
                client.connect("tcp://localhost:5558");
                ZMsg.newStringMsg(START).send(client);
                measure("Subscription (direct DEALER) test", sampleSizePub, () -> {
                    ZMsg req = ZMsg.recvMsg(client);
                    req.destroy();
                });

            } catch (ZMQException e) {
                if (VERBOSE_PRINTOUT) {
                    System.out.println("terminate client");
                }
            }
        }
    }
}
