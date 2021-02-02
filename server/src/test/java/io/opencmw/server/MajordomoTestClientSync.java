package io.opencmw.server;

import static java.nio.charset.StandardCharsets.UTF_8;

import static io.opencmw.OpenCmwProtocol.Command.GET_REQUEST;
import static io.opencmw.OpenCmwProtocol.MdpSubProtocol.PROT_CLIENT;

import java.lang.management.ManagementFactory;
import java.net.URI;
import java.util.Arrays;
import java.util.Formatter;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import io.opencmw.OpenCmwProtocol;

/**
* Majordomo Protocol Client API, implements the OpenCMW MDP variant
*
*/
public class MajordomoTestClientSync {
    private static final Logger LOGGER = LoggerFactory.getLogger(MajordomoTestClientSync.class);
    private static final AtomicInteger CLIENT_V1_INSTANCE = new AtomicInteger();
    private final String uniqueID;
    private final byte[] uniqueIdBytes;
    private String broker;
    private ZContext ctx;
    private ZMQ.Socket clientSocket;
    private long timeout = 2500;
    private int retries = 3;
    private Formatter log = new Formatter(System.out);
    private ZMQ.Poller poller;

    public MajordomoTestClientSync(String broker, String clientName) {
        this.broker = broker.replace("mdp://", "tcp://");
        ctx = new ZContext();

        uniqueID = clientName + "PID=" + ManagementFactory.getRuntimeMXBean().getName() + "-InstanceID=" + CLIENT_V1_INSTANCE.getAndIncrement();
        uniqueIdBytes = uniqueID.getBytes(ZMQ.CHARSET);

        reconnectToBroker();
    }

    /**
     * Connect or reconnect to broker
     */
    void reconnectToBroker() {
        if (clientSocket != null) {
            clientSocket.close();
        }
        clientSocket = ctx.createSocket(SocketType.DEALER);
        clientSocket.setHWM(0);
        clientSocket.setIdentity(uniqueIdBytes);
        clientSocket.connect(broker);

        if (poller != null) {
            poller.unregister(clientSocket);
            poller.close();
        }
        poller = ctx.createPoller(1);
        poller.register(clientSocket, ZMQ.Poller.POLLIN);
        LOGGER.atDebug().addArgument(broker).log("connecting to broker at: '{}'");
    }

    public void destroy() {
        ctx.destroy();
    }

    public int getRetries() {
        return retries;
    }

    public void setRetries(int retries) {
        this.retries = retries;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public String getUniqueID() {
        return uniqueID;
    }

    /**
     * Send request to broker and get reply by hook or crook takes ownership of
     * request message and destroys it when sent. Returns the reply message or
     * NULL if there was no reply.
     *
     * @param service UTF-8 encoded service name
     * @param msgs message(s) to be sent to OpenCmwProtocol broker (if more than one, than the last is assumed to be a RBAC-token
     * @return reply message or NULL if there was no reply
     */
    public OpenCmwProtocol.MdpMessage send(final String service, final byte[]... msgs) {
        ZMsg reply = null;

        int retriesLeft = retries;
        while (retriesLeft > 0 && !Thread.currentThread().isInterrupted()) {
            final URI topic = URI.create(service);
            final byte[] serviceBytes = StringUtils.stripStart(topic.getPath(), "/").getBytes(UTF_8);
            final byte[] rbacToken = msgs.length > 1 ? msgs[1] : null;
            if (!new OpenCmwProtocol.MdpMessage(null, PROT_CLIENT, GET_REQUEST, serviceBytes, "requestID".getBytes(UTF_8), topic, msgs[0], "", rbacToken).send(clientSocket)) {
                throw new IllegalStateException("could not send request " + Arrays.toString(msgs));
            }

            // Poll socket for a reply, with timeout
            if (poller.poll(timeout) == -1)
                break; // Interrupted

            if (poller.pollin(0)) {
                return OpenCmwProtocol.MdpMessage.receive(clientSocket, false);
            } else {
                if (--retriesLeft == 0) {
                    log.format("W: permanent error, abandoning\n");
                    break;
                }
                log.format("W: no reply, reconnecting\n");
                reconnectToBroker();
            }
        }
        return null;
    }
}
