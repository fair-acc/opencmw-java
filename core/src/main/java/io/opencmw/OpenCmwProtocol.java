package io.opencmw;

import static org.zeromq.ZMQ.Socket;

import static io.opencmw.OpenCmwProtocol.Command.*;
import static io.opencmw.utils.AnsiDefs.ANSI_RED;
import static io.opencmw.utils.AnsiDefs.ANSI_RESET;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import org.zeromq.util.ZData;

import io.opencmw.utils.AnsiDefs;

/**
 * Open Common Middle-Ware Protocol
 *
 * extended and based upon:
 * Majordomo Protocol (MDP) definitions and implementations according to https://rfc.zeromq.org/spec/7/
 *
 * For a non-programmatic protocol description see:
 * https://github.com/GSI-CS-CO/chart-fx/blob/master/microservice/docs/MajordomoProtocol.md
 *
 * @author rstein
 * @author Alexander Krimm
 */
@SuppressWarnings({ "PMD.TooManyMethods", "PMD.ArrayIsStoredDirectly", "PMD.CommentSize", "PMD.MethodReturnsInternalArray" })
public final class OpenCmwProtocol { // NOPMD - nomen est omen
    public static final String COMMAND_MUST_NOT_BE_NULL = "command must not be null";
    public static final byte[] EMPTY_FRAME = {};
    public static final URI EMPTY_URI = URI.create("");
    private static final byte[] PROTOCOL_NAME_CLIENT = "MDPC03".getBytes(StandardCharsets.UTF_8);
    private static final byte[] PROTOCOL_NAME_CLIENT_HTTP = "MDPH03".getBytes(StandardCharsets.UTF_8);
    private static final byte[] PROTOCOL_NAME_WORKER = "MDPW03".getBytes(StandardCharsets.UTF_8);
    private static final byte[] PROTOCOL_NAME_UNKNOWN = "UNKNOWN_PROTOCOL".getBytes(StandardCharsets.UTF_8);
    private static final int MAX_PRINT_LENGTH = 200; // unique client id, see ROUTER socket docs for info
    private static final int FRAME0_SOURCE_ID = 0; // unique client id, see ROUTER socket docs for info
    private static final int FRAME1_PROTOCOL_ID = 1; // 'MDPC0<x>' or 'MDPW0<x>'
    private static final int FRAME2_COMMAND_ID = 2;
    private static final int FRAME3_SERVICE_ID = 3;
    private static final int FRAME4_CLIENT_REQUEST_ID = 4;
    private static final int FRAME5_TOPIC = 5;
    private static final int FRAME6_DATA = 6;
    private static final int FRAME7_ERROR = 7;
    private static final int FRAME8_RBAC_TOKEN = 8;
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenCmwProtocol.class);
    private static final String SOCKET_MUST_NOT_BE_NULL = "socket must not be null";
    public static final int N_PROTOCOL_FRAMES = 8;

    /**
     * MDP sub-protocol V0.1
     */
    public enum MdpSubProtocol {
        PROT_CLIENT(PROTOCOL_NAME_CLIENT), // OpenCmwProtocol/Client protocol implementation version
        PROT_CLIENT_HTTP(PROTOCOL_NAME_CLIENT_HTTP), // OpenCmwProtocol/HTTP(REST) protocol implementation version
        PROT_WORKER(PROTOCOL_NAME_WORKER), // OpenCmwProtocol/Worker protocol implementation version
        UNKNOWN(PROTOCOL_NAME_UNKNOWN);

        private final byte[] data;
        private final String protocolName;
        MdpSubProtocol(final byte[] value) {
            this.data = value;
            protocolName = new String(data, StandardCharsets.UTF_8);
        }

        public byte[] getData() {
            return data;
        }

        @Override
        public String toString() {
            return "MdpSubProtocol{'" + protocolName + "'}";
        }

        public static MdpSubProtocol getProtocol(byte[] frame) {
            for (MdpSubProtocol knownProtocol : MdpSubProtocol.values()) {
                if (Arrays.equals(knownProtocol.data, frame)) {
                    if (knownProtocol == UNKNOWN) {
                        continue;
                    }
                    return knownProtocol;
                }
            }
            return UNKNOWN;
        }
    }

    /**
     * OpenCmwProtocol commands, as byte values
     */
    public enum Command {
        GET_REQUEST(0x01, true, true),
        SET_REQUEST(0x02, true, true),
        PARTIAL(0x03, true, true),
        FINAL(0x04, true, true),
        READY(0x05, true, true), // mandatory for worker, optional for client (ie. for optional initial RBAC authentication)
        DISCONNECT(0x06, true, true), // mandatory for worker, optional for client
        SUBSCRIBE(0x07, true, true), // client specific command
        UNSUBSCRIBE(0x08, true, true), // client specific command
        W_NOTIFY(0x09, false, true), // worker specific command
        W_HEARTBEAT(0x10, true, true), // worker specific command, optional for client
        UNKNOWN(-1, false, false);

        private final byte[] data;
        private final boolean isForClients;
        private final boolean isForWorkers;
        Command(final int value, boolean client, final boolean worker) { //watch for ints>255, will be truncated
            this.data = new byte[] { (byte) (value & 0xFF) };
            this.isForClients = client;
            this.isForWorkers = worker;
        }

        public byte[] getData() {
            return data;
        }

        public boolean isClientCompatible() {
            return isForClients;
        }

        public boolean isWorkerCompatible() {
            return isForWorkers;
        }

        public static Command getCommand(byte[] frame) {
            for (Command knownMdpCommand : values()) {
                if (Arrays.equals(knownMdpCommand.data, frame)) {
                    if (knownMdpCommand == UNKNOWN) {
                        continue;
                    }
                    return knownMdpCommand;
                }
            }
            return UNKNOWN;
        }
    }

    /**
     * MDP data object to store OpenCMW frames description
     *
     * For a non-programmatic protocol description see:
     * https://github.com/GSI-CS-CO/chart-fx/blob/master/microservice/docs/MajordomoProtocol.md
     */
    public static class MdpMessage {
        /** OpenCMW frame 0: sender source ID - usually the ID from the MDP broker ROUTER socket for the given connection */
        public byte[] senderID;
        /** OpenCMW frame 1: unique protocol identifier */
        public MdpSubProtocol protocol;
        /** OpenCMW frame 2: MDP command */
        public Command command;
        /** OpenCMW frame 3: service name (for client sub-protocols) or client source ID (for worker sub-protocol) */
        public byte[] serviceNameBytes; // UTF-8 encoded service name and/or clientID
        /** OpenCMW frame 4: custom client request ID (N.B. client-generated and transparently passed through broker and worker) */
        public byte[] clientRequestID;
        /** OpenCMW frame 5: request/reply topic -- follows URI syntax, ie. '<pre>scheme:[//authority]path[?query][#fragment]</pre>' see <a href="https://tools.ietf.org/html/rfc3986">documentation</a> */
        public URI topic; // request/reply topic - follows URI syntax, ie. '<pre>scheme:[//authority]path[?query][#fragment]</pre>'
        /** OpenCMW frame 6: data (may be null if error stack is not blank) */
        public byte[] data;
        /** OpenCMW frame 7: error stack -- UTF-8 string (may be blank if data is not null) */
        public String errors;
        /** OpenCMW frame 8 (optional): RBAC token */
        public byte[] rbacToken;

        private MdpMessage() {
            // private constructor
        }

        /**
         * generate new (immutable) MdpMessage representation
         * @param senderID OpenCMW frame 0: sender source ID - usually the ID from the MDP broker ROUTER socket for the given connection
         * @param protocol OpenCMW frame 1: unique protocol identifier (see: MdpSubProtocol)
         * @param command OpenCMW frame 2: command (see: Command)
         * @param serviceID OpenCMW frame 3: service name (for client sub-protocols) or client source ID (for worker sub-protocol)
         * @param clientRequestID OpenCMW frame 4: custom client request ID (N.B. client-generated and transparently passed through broker and worker)
         * @param topic openCMW frame 5: the request/reply topic - follows URI syntax, ie. '<pre>scheme:[//authority]path[?query][#fragment]</pre>' see <a href="https://tools.ietf.org/html/rfc3986">documentation</a>
         * @param data OpenCMW frame 6: data - may be null in case errors is not null
         * @param errors OpenCMW frame 7: error stack -- UTF-8 string may be blank only if data is not null
         * @param rbacToken OpenCMW frame 8 (optional): RBAC token
         */
        public MdpMessage(final byte[] senderID, @NotNull final MdpSubProtocol protocol, @NotNull final Command command,
                @NotNull final byte[] serviceID, @NotNull final byte[] clientRequestID, @NotNull final URI topic,
                final byte[] data, @NotNull final String errors, final byte[] rbacToken) {
            this.senderID = senderID == null ? EMPTY_FRAME : senderID;
            this.protocol = protocol;
            this.command = command;
            this.serviceNameBytes = serviceID;
            this.clientRequestID = clientRequestID;
            this.topic = topic;
            this.data = data == null ? EMPTY_FRAME : data;
            this.errors = errors;
            if (data == null && errors.isBlank()) {
                throw new IllegalArgumentException("data must not be null if errors are blank");
            }
            this.rbacToken = rbacToken == null ? EMPTY_FRAME : rbacToken;
        }

        /**
         * Copy constructor cloning other MdpMessage
         * @param other MdpMessage
         */
        public MdpMessage(@NotNull final MdpMessage other) {
            this(other, UNKNOWN);
        }

        /**
         * Copy constructor cloning other MdpMessage
         * @param other MdpMessage
         * @param fullCopy is UNKNOWN then a clone is generated, for all other cases a MdpMessage with
         *                 the specified command, mirrored frames, except 'data', 'errors' and 'rbacToken' is generated.
         */
        public MdpMessage(@NotNull final MdpMessage other, @NotNull final Command fullCopy) {
            this(copyOf(other.senderID), other.protocol, fullCopy == UNKNOWN ? other.command : fullCopy, copyOf(other.serviceNameBytes), copyOf(other.clientRequestID), other.topic,
                    fullCopy == UNKNOWN ? copyOf(other.data) : EMPTY_FRAME, fullCopy == UNKNOWN ? other.errors : "", fullCopy == UNKNOWN ? copyOf(other.rbacToken) : EMPTY_FRAME);
        }

        @Override
        public boolean equals(final Object obj) {
            if (this == obj) {
                return true;
            } else if (!(obj instanceof MdpMessage)) {
                return false;
            }
            final MdpMessage other = (MdpMessage) obj;
            // ignore senderID from comparison since not all socket provide/require this information
            if (/*!Arrays.equals(senderID, other.senderID) ||*/ (protocol != other.protocol) || (command != other.command)
                    || !Arrays.equals(serviceNameBytes, other.serviceNameBytes) || !Arrays.equals(clientRequestID, other.clientRequestID)
                    || (!Objects.equals(topic, other.topic))
                    || !Arrays.equals(data, other.data)
                    || (!Objects.equals(errors, other.errors))) {
                return false;
            }
            return Arrays.equals(rbacToken, other.rbacToken);
        }

        public String getSenderName() {
            return senderID == null ? "" : ZData.toString(senderID);
        }

        public String getServiceName() {
            return serviceNameBytes == null ? "" : ZData.toString(serviceNameBytes);
        }

        public boolean hasRbackToken() {
            return rbacToken.length != 0;
        }

        @Override
        public int hashCode() {
            int result = (protocol == null ? 0 : protocol.hashCode());
            result = 31 * result + Objects.hashCode(command);
            result = 31 * result + Arrays.hashCode(serviceNameBytes);
            result = 31 * result + Arrays.hashCode(clientRequestID);
            result = 31 * result + (topic == null ? 0 : topic.hashCode());
            result = 31 * result + Arrays.hashCode(data);
            result = 31 * result + (errors == null ? 0 : errors.hashCode());
            result = 31 * result + Arrays.hashCode(rbacToken);
            return result;
        }

        /**
         * Send MDP message to Socket
         *
         * @param socket ZeroMQ socket to send the message on
         * @return {@code true} if successful
         */
        public boolean send(final Socket socket) {
            // some assertions for debugging purposes - N.B. these should occur only when developing/refactoring the frame-work
            // N.B. to be enabled with '-ea' VM argument
            assert socket != null : SOCKET_MUST_NOT_BE_NULL;
            assert protocol != null : "protocol must not be null";
            assert !protocol.equals(MdpSubProtocol.UNKNOWN)
                : "protocol must not be UNKNOWN";
            assert command != null : COMMAND_MUST_NOT_BE_NULL;
            assert (protocol.equals(MdpSubProtocol.PROT_CLIENT) && command.isClientCompatible())
                    || (protocol.equals(MdpSubProtocol.PROT_WORKER) && command.isWorkerCompatible())
                : "command is client/worker compatible";
            assert serviceNameBytes != null : "serviceName must not be null";
            assert clientRequestID != null : "clientRequestID must not be null";
            assert topic != null : "topic must not be null";
            assert !(data == null && (errors == null || errors.isBlank()))
                : "data must not be null and errors be blank";

            ZMsg msg = new ZMsg();
            if (socket.getSocketType() == SocketType.ROUTER) {
                if (senderID == null) {
                    throw new IllegalArgumentException("senderID must be non-null when using ROUTER sockets");
                }
                msg.add(new ZFrame(senderID)); // frame 0: source ID (optional, only needed for broker sockets)
            }
            msg.add(new ZFrame(protocol.data)); // frame: 1
            msg.add(new ZFrame(command.data)); // frame: 2
            msg.add(new ZFrame(serviceNameBytes)); // frame: 3
            msg.add(new ZFrame(clientRequestID)); // frame: 4
            msg.addString(topic.toString()); // frame: 5
            msg.add(new ZFrame(data == null ? EMPTY_FRAME : data)); // frame: 6
            msg.addString(errors == null ? "" : errors); // frame: 7
            msg.add(new ZFrame(rbacToken)); // frame: 8 - rbac token

            if (LOGGER.isTraceEnabled()) {
                LOGGER.atTrace().addArgument(msg.toString()).log("sending message {}");
            }
            return msg.send(socket);
        }

        @Override
        public String toString() {
            final String errStr = errors == null || errors.isBlank() ? "no-exception" : ANSI_RED + " exception thrown: " + errors + ANSI_RESET;
            return "MdpMessage{senderID='" + ZData.toString(senderID) + "', " + protocol + ", " + command + ", serviceName='" + getServiceName()
                    + "', clientRequestID='" + ZData.toString(clientRequestID) + "', topic='" + topic
                    + "', data='" + dataToString(data) + "', " + errStr + ", rbac='" + ZData.toString(rbacToken) + "'}";
        }

        protected static String dataToString(byte[] data) {
            if (data == null) {
                return "";
            }
            // Dump message as text or hex-encoded string
            boolean isText = true;
            for (byte aData : data) {
                if (aData < AnsiDefs.MIN_PRINTABLE_CHAR) {
                    isText = false;
                    break;
                }
            }
            if (isText) {
                // always make full-print when there are only printable characters
                return new String(data, ZMQ.CHARSET);
            }
            if (data.length < MAX_PRINT_LENGTH) {
                return ZData.strhex(data);
            } else {
                return ZData.strhex(Arrays.copyOf(data, MAX_PRINT_LENGTH)) + "[" + (data.length - MAX_PRINT_LENGTH) + " more bytes]";
            }
        }

        /**
         * @param socket the socket to receive from (performs blocking call)
         * @return MdpMessage if valid, or {@code null} otherwise
         */
        public static MdpMessage receive(final Socket socket) {
            return receive(socket, true);
        }

        /**
         * @param socket the socket to receive from
         * @param wait setting the flag to ZMQ.DONTWAIT does a non-blocking recv.
         * @return MdpMessage if valid, or {@code null} otherwise
         */
        @SuppressWarnings("PMD.NPathComplexity")
        public static MdpMessage receive(@NotNull final Socket socket, final boolean wait) {
            final int flags = wait ? 0 : ZMQ.DONTWAIT;
            final ZMsg msg = ZMsg.recvMsg(socket, flags);
            if (msg == null) {
                return null;
            }
            if (socket.getSocketType() != SocketType.ROUTER) {
                msg.push(EMPTY_FRAME); // push empty client frame
            }

            if (socket.getSocketType() == SocketType.SUB || socket.getSocketType() == SocketType.XSUB) {
                msg.pollFirst(); // remove first subscription topic message -- not needed here since this is also encoded in the service/topic frame
            }

            final List<byte[]> rawFrames = msg.stream().map(ZFrame::getData).collect(Collectors.toUnmodifiableList());
            if (rawFrames.size() <= N_PROTOCOL_FRAMES) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.atWarn().addArgument(rawFrames.size()).addArgument(dataToString(rawFrames)).log("received message size is < " + N_PROTOCOL_FRAMES + ": {} rawMessage: {}");
                }
                return null;
            }

            // OpenCMW frame 0: sender source ID - usually the ID from the MDP broker ROUTER socket for the given connection
            final byte[] senderID = socket.getSocketType() == SocketType.ROUTER ? rawFrames.get(FRAME0_SOURCE_ID) : EMPTY_FRAME; // NOPMD
            // OpenCMW frame 1: unique protocol identifier
            final MdpSubProtocol protocol = MdpSubProtocol.getProtocol(rawFrames.get(FRAME1_PROTOCOL_ID));
            if (protocol.equals(MdpSubProtocol.UNKNOWN)) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.atWarn().addArgument(ZData.toString(rawFrames.get(FRAME1_PROTOCOL_ID))).addArgument(dataToString(rawFrames)).log("unknown protocol: '{}' rawMessage: {}");
                }
                return null;
            }

            // OpenCMW frame 2: command
            final Command command = getCommand(rawFrames.get(FRAME2_COMMAND_ID));
            if (command.equals(UNKNOWN)) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.atWarn().addArgument(ZData.toString(rawFrames.get(FRAME2_COMMAND_ID))).addArgument(dataToString(rawFrames)).log("unknown command: '{}' rawMessage: {}");
                }
                return null;
            }

            // OpenCMW frame 3: service name or client source ID
            final byte[] serviceNameBytes = rawFrames.get(FRAME3_SERVICE_ID);
            if (serviceNameBytes == null) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.atDebug().addKeyValue("a", "dd").log("");
                    LOGGER.atWarn().addArgument(dataToString(rawFrames)).log("serviceNameBytes is null, rawMessage: {}");
                }
                return null;
            }

            // OpenCMW frame 4: service name or client source ID
            final byte[] clientRequestID = rawFrames.get(FRAME4_CLIENT_REQUEST_ID); // NOPMD

            // OpenCMW frame 5: request/reply topic -- UTF-8 string
            final byte[] topicBytes = rawFrames.get(FRAME5_TOPIC);
            if (topicBytes == null) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.atWarn().addArgument(dataToString(rawFrames)).log("topic is null, rawMessage: {}");
                }
                return null;
            }

            final String topicString = new String(topicBytes, StandardCharsets.UTF_8);
            final URI topic;
            try {
                topic = new URI(topicString);
            } catch (URISyntaxException e) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.atWarn().setCause(e).addArgument(topicString).addArgument(topicString).log("topic URI cannot be parsed {}  - {}");
                }
                return null;
            }

            // OpenCMW frame 6: data
            final byte[] data = rawFrames.get(FRAME6_DATA);
            // OpenCMW frame 7: error stack -- UTF-8 string
            final byte[] errorBytes = rawFrames.get(FRAME7_ERROR);
            final String errors = errorBytes == null || errorBytes.length == 0 ? "" : new String(errorBytes, StandardCharsets.UTF_8);
            if (data == null && errors.isBlank()) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.atWarn().addArgument(dataToString(rawFrames)).log("data is null and errors is blank - {}");
                }
                return null;
            }

            // OpenCMW frame 8 (optional): RBAC token
            final byte[] rbacTokenByte = rawFrames.size() == 9 ? rawFrames.get(FRAME8_RBAC_TOKEN) : null;
            final byte[] rbacToken = rawFrames.size() == 9 && rbacTokenByte != null ? rbacTokenByte : EMPTY_FRAME;

            return new MdpMessage(senderID, protocol, command, serviceNameBytes, clientRequestID, topic, data, errors, rbacToken); // OpenCMW frame 8 (optional): RBAC token
        }

        public static boolean send(final Socket socket, final List<MdpMessage> replies) {
            assert socket != null : SOCKET_MUST_NOT_BE_NULL;
            assert replies != null;
            if (replies.isEmpty()) {
                return false;
            }
            boolean sendState = false;
            for (Iterator<MdpMessage> iter = replies.iterator(); iter.hasNext();) {
                MdpMessage reply = iter.next();
                reply.command = iter.hasNext() ? PARTIAL : FINAL;
                sendState |= reply.send(socket);
            }
            return sendState;
        }

        protected static byte[] copyOf(byte[] original) {
            return original == null ? EMPTY_FRAME : Arrays.copyOf(original, original.length);
        }

        protected static String dataToString(List<byte[]> data) {
            return data.stream().map(ZData::toString).collect(Collectors.joining(", ", "[#frames= " + data.size() + ": ", "]"));
        }
    }

    /**
     *  MDP reply/request context
     */
    public static class Context {
        public final MdpMessage req; // input request
        public MdpMessage rep; // return request

        private Context() {
            req = new MdpMessage();
        }

        public Context(@NotNull MdpMessage requestMsg) {
            req = requestMsg;
            rep = new MdpMessage(req, FINAL);
        }

        @Override
        public String toString() {
            return "OpenCmwProtocol.Context{req=" + req + ", rep=" + rep + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Context)) {
                return false;
            }
            Context context = (Context) o;

            if (!Objects.equals(req, context.req)) {
                return false;
            }
            return Objects.equals(rep, context.rep);
        }

        @Override
        public int hashCode() {
            int result = req == null ? 0 : req.hashCode();
            result = 31 * result + (rep == null ? 0 : rep.hashCode());
            return result;
        }
    }
}
