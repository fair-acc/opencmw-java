package io.opencmw.server;

import static io.opencmw.OpenCmwProtocol.Command.GET_REQUEST;
import static io.opencmw.OpenCmwProtocol.Command.SUBSCRIBE;
import static io.opencmw.OpenCmwProtocol.Command.UNSUBSCRIBE;
import static io.opencmw.OpenCmwProtocol.EMPTY_FRAME;
import static io.opencmw.OpenCmwProtocol.MdpSubProtocol.PROT_CLIENT;

import java.net.URI;
import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import io.opencmw.OpenCmwProtocol;

/**
 * Majordomo Protocol Client API, asynchronous Java version. Implements the
 * OpenCmwProtocol/Worker spec at http://rfc.zeromq.org/spec:7.
 */
public class MajordomoTestClientAsync {
    private static final Logger LOGGER = LoggerFactory.getLogger(MajordomoTestClientAsync.class);
    private final String broker;
    private final ZContext ctx;
    private ZMQ.Socket clientSocket;
    private long timeout = 2500;
    private ZMQ.Poller poller;

    public MajordomoTestClientAsync(final URI broker) {
        this.broker = broker.toString().replace("mdp://", "tcp://");
        ctx = new ZContext();
        reconnectToBroker();
    }

    public void destroy() {
        ctx.destroy();
    }

    public long getTimeout() {
        return timeout;
    }

    /**
     * Returns the reply message or NULL if there was no reply. Does not attempt
     * to recover from a broker failure, this is not possible without storing
     * all unanswered requests and resending them all…
     * @return the MdpMessage
     */
    public OpenCmwProtocol.MdpMessage recv() {
        // Poll socket for a reply, with timeout
        if (poller.poll(timeout * 1000) == -1) {
            return null; // Interrupted
        }

        if (poller.pollin(0)) {
            return OpenCmwProtocol.MdpMessage.receive(clientSocket, false);
        }
        return null;
    }

    /**
     * Send request to broker and get reply by hook or crook Takes ownership of request message and destroys it when sent.
     *
     * @param service UTF-8 encoded service name byte array
     * @param msgs message(s) to be sent to OpenCmwProtocol broker (if more than one, than the last is assumed to be a RBAC-token
     */
    public boolean send(final byte[] service, final byte[]... msgs) {
        final String topic = new String(service, StandardCharsets.UTF_8);
        final byte[] rbacToken = msgs.length > 1 ? msgs[1] : null;
        return new OpenCmwProtocol.MdpMessage(null, PROT_CLIENT, GET_REQUEST, service, "requestID".getBytes(StandardCharsets.UTF_8), URI.create(topic), msgs[0], "", rbacToken).send(clientSocket);
    }

    /**
     * Send subscription request to broker
     *
     * @param service UTF-8 encoded service name byte array
     * @param rbacToken optional RBAC-token
     */
    public boolean subscribe(final byte[] service, final byte[]... rbacToken) {
        final String topic = new String(service, StandardCharsets.UTF_8);
        final byte[] rbacTokenByte = rbacToken.length > 0 ? rbacToken[0] : null;
        return new OpenCmwProtocol.MdpMessage(null, PROT_CLIENT, SUBSCRIBE, service, "requestID".getBytes(StandardCharsets.UTF_8), URI.create(topic), EMPTY_FRAME, "", rbacTokenByte).send(clientSocket);
    }

    /**
     * Send subscription request to broker
     *
     * @param service UTF-8 encoded service name byte array
     * @param rbacToken optional RBAC-token
     */
    public boolean unsubscribe(final byte[] service, final byte[]... rbacToken) {
        final String topic = new String(service, StandardCharsets.UTF_8);
        final byte[] rbacTokenByte = rbacToken.length > 0 ? rbacToken[0] : null;
        return new OpenCmwProtocol.MdpMessage(null, PROT_CLIENT, UNSUBSCRIBE, service, "requestID".getBytes(StandardCharsets.UTF_8), URI.create(topic), EMPTY_FRAME, "", rbacTokenByte).send(clientSocket);
    }

    /**
     * Send request to broker and get reply by hook or crook Takes ownership of request message and destroys it when sent.
     *
     * @param service UTF-8 encoded service name byte array
     * @param msgs message(s) to be sent to OpenCmwProtocol broker (if more than one, than the last is assumed to be a RBAC-token
     */
    public boolean send(final String service, final byte[]... msgs) {
        return send(service.getBytes(StandardCharsets.UTF_8), msgs);
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
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
        clientSocket.setIdentity("clientV2".getBytes(StandardCharsets.UTF_8));
        clientSocket.connect(broker);
        if (poller != null) {
            poller.unregister(clientSocket);
            poller.close();
        }
        poller = ctx.createPoller(1);
        poller.register(clientSocket, ZMQ.Poller.POLLIN);
        LOGGER.atDebug().addArgument(broker).log("connecting to broker at: '{}'");
    }
}
