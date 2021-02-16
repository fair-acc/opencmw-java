package io.opencmw.client.cmwlight;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.jetbrains.annotations.NotNull;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import io.opencmw.serialiser.DataType;
import io.opencmw.serialiser.FieldDescription;
import io.opencmw.serialiser.IoBuffer;
import io.opencmw.serialiser.IoClassSerialiser;
import io.opencmw.serialiser.spi.CmwLightSerialiser;
import io.opencmw.serialiser.spi.FastByteBuffer;
import io.opencmw.serialiser.spi.WireDataFieldDescription;

/**
 * A lightweight implementation of the CMW RDA client protocol part.
 * Serializes CmwLightMessage to ZeroMQ messages and vice versa.
 */
@SuppressWarnings("PMD.UnusedLocalVariable") // Unused variables are taken from the protocol and should be available for reference
public class CmwLightProtocol { //NOPMD -- nomen est omen
    private static final String CONTEXT_ACQ_STAMP = "ContextAcqStamp";
    private static final String CONTEXT_CYCLE_STAMP = "ContextCycleStamp";
    private static final String MESSAGE = "Message";
    private static final String TYPE = "Type";
    private static final String EMPTY_CONTEXT = "empty context data for request type: ";
    private static final int MAX_MSG_SIZE = 4096 * 1024;
    private static final IoBuffer IO_BUFFER = new FastByteBuffer(MAX_MSG_SIZE);
    private static final CmwLightSerialiser SERIALISER = new CmwLightSerialiser(IO_BUFFER);
    private static final IoClassSerialiser IO_CLASS_SERIALISER = new IoClassSerialiser(IO_BUFFER);
    public static final String VERSION = "1.0.0"; // Protocol version used if msg.version is null or empty
    private static final int SERIALISER_QUIRK = 100; // there seems to be a bug in the serialiser which does not update the buffer position correctly, so send more

    private CmwLightProtocol() {
        // utility class
    }

    /**
     * The message specified by the byte contained in the first frame of a message defines what type of message is present
     */
    public enum MessageType {
        SERVER_CONNECT_ACK(0x01),
        SERVER_REP(0x02),
        SERVER_HB(0x03),
        CLIENT_CONNECT(0x20),
        CLIENT_REQ(0x21),
        CLIENT_HB(0x22);

        private static final int CLIENT_API_RANGE = 0x4;
        private static final int SERVER_API_RANGE = 0x20;
        private final byte value;

        MessageType(int value) {
            this.value = (byte) value;
        }

        public byte value() {
            return value;
        }

        public static MessageType of(int value) { // NOPMD -- nomen est omen
            if (value < CLIENT_API_RANGE) {
                return values()[value - 1];
            } else {
                return values()[value - SERVER_API_RANGE + CLIENT_CONNECT.ordinal()];
            }
        }
    }

    /**
     * Frame Types in the descriptor (Last frame of a message containing the type of each sub message)
     */
    public enum FrameType {
        HEADER(0),
        BODY(1),
        BODY_DATA_CONTEXT(2),
        BODY_REQUEST_CONTEXT(3),
        BODY_EXCEPTION(4);

        private final byte value;

        FrameType(int value) {
            this.value = (byte) value;
        }

        public byte value() {
            return value;
        }
    }

    /**
     * Field names for the Request Header
     */
    public enum FieldName {
        EVENT_TYPE_TAG("eventType"),
        MESSAGE_TAG("message"),
        ID_TAG("0"),
        DEVICE_NAME_TAG("1"),
        REQ_TYPE_TAG("2"),
        OPTIONS_TAG("3"),
        CYCLE_NAME_TAG("4"),
        ACQ_STAMP_TAG("5"),
        CYCLE_STAMP_TAG("6"),
        UPDATE_TYPE_TAG("7"),
        SELECTOR_TAG("8"),
        CLIENT_INFO_TAG("9"),
        NOTIFICATION_ID_TAG("a"),
        SOURCE_ID_TAG("b"),
        FILTERS_TAG("c"),
        DATA_TAG("x"),
        SESSION_ID_TAG("d"),
        SESSION_BODY_TAG("e"),
        PROPERTY_NAME_TAG("f");

        private final String name;

        FieldName(String name) {
            this.name = name;
        }

        public String value() {
            return name;
        }
    }

    /**
     * request type used in request header REQ_TYPE_TAG
     */
    public enum RequestType {
        GET(0),
        SET(1),
        CONNECT(2),
        REPLY(3),
        EXCEPTION(4),
        SUBSCRIBE(5),
        UNSUBSCRIBE(6),
        NOTIFICATION_DATA(7),
        NOTIFICATION_EXC(8),
        SUBSCRIBE_EXCEPTION(9),
        EVENT(10),
        SESSION_CONFIRM(11);

        private final byte value;

        RequestType(int value) {
            this.value = (byte) value;
        }

        public static RequestType of(int value) { // NOPMD - nomen est omen
            return values()[value];
        }

        public byte value() {
            return value;
        }
    }

    /**
     * UpdateType
     */
    public enum UpdateType {
        NORMAL(0),
        FIRST_UPDATE(1), // Initial update sent when the subscription is created.
        IMMEDIATE_UPDATE(2); // Update sent after the value has been modified by a set call.

        private final byte value;

        UpdateType(int value) {
            this.value = (byte) value;
        }

        public static UpdateType of(int value) { // NOPMD - nomen est omen
            return values()[value];
        }

        public byte value() {
            return value;
        }
    }

    public static CmwLightMessage recvMsg(final ZMQ.Socket socket, int tout) throws RdaLightException {
        return parseMsg(ZMsg.recvMsg(socket, tout));
    }

    public static CmwLightMessage parseMsg(final @NotNull ZMsg data) throws RdaLightException { // NOPMD - NPath complexity acceptable (complex protocol)
        assert data != null : "data";
        final ZFrame firstFrame = data.pollFirst();
        if (firstFrame != null && Arrays.equals(firstFrame.getData(), new byte[] { MessageType.SERVER_CONNECT_ACK.value() })) {
            final CmwLightMessage reply = new CmwLightMessage(MessageType.SERVER_CONNECT_ACK);
            final ZFrame versionData = data.pollFirst();
            assert versionData != null : "version data in connection acknowledgement frame";
            reply.version = versionData.getString(Charset.defaultCharset());
            return reply;
        }
        if (firstFrame != null && Arrays.equals(firstFrame.getData(), new byte[] { MessageType.CLIENT_CONNECT.value() })) {
            final CmwLightMessage reply = new CmwLightMessage(MessageType.CLIENT_CONNECT);
            final ZFrame versionData = data.pollFirst();
            assert versionData != null : "version data in connection acknowledgement frame";
            reply.version = versionData.getString(Charset.defaultCharset());
            return reply;
        }
        if (firstFrame != null && Arrays.equals(firstFrame.getData(), new byte[] { MessageType.SERVER_HB.value() })) {
            return CmwLightMessage.SERVER_HB;
        }
        if (firstFrame != null && Arrays.equals(firstFrame.getData(), new byte[] { MessageType.CLIENT_HB.value() })) {
            return CmwLightMessage.CLIENT_HB;
        }
        byte[] descriptor = checkDescriptor(data.pollLast(), firstFrame);
        final ZFrame headerMsg = data.poll();
        assert headerMsg != null : "message header";
        CmwLightMessage reply = getReplyFromHeader(firstFrame, headerMsg);
        switch (reply.requestType) {
        case REPLY:
            assertDescriptor(descriptor, FrameType.HEADER, FrameType.BODY, FrameType.BODY_DATA_CONTEXT);
            reply.bodyData = data.pollFirst();
            if (data.isEmpty()) {
                throw new RdaLightException(EMPTY_CONTEXT + reply.requestType);
            }
            reply.dataContext = parseContextData(data.pollFirst());
            return reply;
        case NOTIFICATION_DATA: // notification update
            assertDescriptor(descriptor, FrameType.HEADER, FrameType.BODY, FrameType.BODY_DATA_CONTEXT);
            if (reply.options != null && reply.options.containsKey(FieldName.NOTIFICATION_ID_TAG.value())) {
                reply.notificationId = (long) reply.options.get(FieldName.NOTIFICATION_ID_TAG.value());
            }
            reply.bodyData = data.pollFirst();
            if (data.isEmpty()) {
                throw new RdaLightException(EMPTY_CONTEXT + reply.requestType);
            }
            reply.dataContext = parseContextData(data.pollFirst());
            return reply;
        case EXCEPTION: // exception on get/set request
        case NOTIFICATION_EXC: // exception on notification, e.g null pointer in server notify code
        case SUBSCRIBE_EXCEPTION: // exception on subscribe e.g. nonexistent property, wrong filters
            assertDescriptor(descriptor, FrameType.HEADER, FrameType.BODY_EXCEPTION);
            reply.exceptionMessage = parseExceptionMessage(data.pollFirst());
            return reply;
        case GET:
            assertDescriptor(descriptor, FrameType.HEADER, FrameType.BODY_REQUEST_CONTEXT);
            if (data.isEmpty()) {
                throw new RdaLightException(EMPTY_CONTEXT + reply.requestType);
            }
            reply.requestContext = parseRequestContext(data.pollFirst());
            return reply;
        case SUBSCRIBE: // descriptor: [0] options: SOURCE_ID_TAG // seems to be sent after subscription is accepted
            if (reply.messageType == MessageType.SERVER_REP) {
                assertDescriptor(descriptor, FrameType.HEADER);
                if (reply.options != null && reply.options.containsKey(FieldName.SOURCE_ID_TAG.value())) {
                    reply.sourceId = (long) reply.options.get(FieldName.SOURCE_ID_TAG.value());
                }
            } else {
                assertDescriptor(descriptor, FrameType.HEADER, FrameType.BODY_REQUEST_CONTEXT);
                if (data.isEmpty()) {
                    throw new RdaLightException(EMPTY_CONTEXT + reply.requestType);
                }
                reply.requestContext = parseRequestContext(data.pollFirst());
            }
            return reply;
        case SESSION_CONFIRM: // descriptor: [0] options: SESSION_BODY_TAG
            assertDescriptor(descriptor, FrameType.HEADER);
            if (reply.options != null && reply.options.containsKey(FieldName.SESSION_BODY_TAG.value())) {
                final Object subMap = reply.options.get(FieldName.SESSION_BODY_TAG.value());
                final String fieldName = FieldName.SESSION_BODY_TAG.value();
                if (subMap instanceof Map) {
                    @SuppressWarnings("unchecked")
                    final Map<String, Object> castMap = (Map<String, Object>) reply.options.get(fieldName);
                    reply.sessionBody = castMap;
                } else {
                    throw new RdaLightException("field member '" + fieldName + "' not assignable to Map<String, Object>: " + subMap);
                }
            }
            return reply;
        case EVENT:
        case UNSUBSCRIBE:
        case CONNECT:
            assertDescriptor(descriptor, FrameType.HEADER);
            return reply;
        case SET:
            assertDescriptor(descriptor, FrameType.HEADER, FrameType.BODY, FrameType.BODY_REQUEST_CONTEXT);
            reply.bodyData = data.pollFirst();
            if (data.isEmpty()) {
                throw new RdaLightException(EMPTY_CONTEXT + reply.requestType);
            }
            reply.requestContext = parseRequestContext(data.pollFirst());
            return reply;
        default:
            throw new RdaLightException("received unknown or non-client request type: " + reply.requestType);
        }
    }

    public static void sendMsg(final ZMQ.Socket socket, final CmwLightMessage msg) throws RdaLightException {
        serialiseMsg(msg).send(socket);
    }

    public static ZMsg serialiseMsg(final CmwLightMessage msg) throws RdaLightException {
        final ZMsg result = new ZMsg();
        switch (msg.messageType) {
        case SERVER_CONNECT_ACK:
        case CLIENT_CONNECT:
            result.add(new ZFrame(new byte[] { msg.messageType.value() }));
            result.add(new ZFrame(msg.version == null || msg.version.isEmpty() ? VERSION : msg.version));
            return result;
        case CLIENT_HB:
        case SERVER_HB:
            result.add(new ZFrame(new byte[] { msg.messageType.value() }));
            return result;
        case SERVER_REP:
        case CLIENT_REQ:
            result.add(new byte[] { msg.messageType.value() });
            result.add(serialiseHeader(msg));
            switch (msg.requestType) {
            case CONNECT:
            case EVENT:
            case SESSION_CONFIRM:
            case UNSUBSCRIBE:
                addDescriptor(result, FrameType.HEADER);
                break;
            case GET:
            case SUBSCRIBE:
                if (msg.messageType == MessageType.CLIENT_REQ) {
                    assert msg.requestContext != null : "requestContext";
                    result.add(serialiseRequestContext(msg.requestContext));
                    addDescriptor(result, FrameType.HEADER, FrameType.BODY_REQUEST_CONTEXT);
                } else {
                    addDescriptor(result, FrameType.HEADER);
                }
                break;
            case SET:
                assert msg.bodyData != null : "bodyData";
                assert msg.requestContext != null : "requestContext";
                result.add(msg.bodyData);
                result.add(serialiseRequestContext(msg.requestContext));
                addDescriptor(result, FrameType.HEADER, FrameType.BODY, FrameType.BODY_REQUEST_CONTEXT);
                break;
            case REPLY:
            case NOTIFICATION_DATA:
                assert msg.bodyData != null : "bodyData";
                result.add(msg.bodyData);
                result.add(serialiseDataContext(msg.dataContext));
                addDescriptor(result, FrameType.HEADER, FrameType.BODY, FrameType.BODY_DATA_CONTEXT);
                break;
            case NOTIFICATION_EXC:
            case EXCEPTION:
            case SUBSCRIBE_EXCEPTION:
                assert msg.exceptionMessage != null : "exceptionMessage";
                result.add(serialiseExceptionMessage(msg.exceptionMessage));
                addDescriptor(result, FrameType.HEADER, FrameType.BODY_EXCEPTION);
                break;
            default:
            }
            return result;
        default:
        }

        throw new RdaLightException("Invalid cmwMessage: " + msg);
    }

    private static ZFrame serialiseExceptionMessage(final CmwLightMessage.ExceptionMessage exceptionMessage) {
        IO_BUFFER.reset();
        SERIALISER.setBuffer(IO_BUFFER);
        SERIALISER.putHeaderInfo();
        SERIALISER.put(CONTEXT_ACQ_STAMP, exceptionMessage.contextAcqStamp);
        SERIALISER.put(CONTEXT_CYCLE_STAMP, exceptionMessage.contextCycleStamp);
        SERIALISER.put(MESSAGE, exceptionMessage.message);
        SERIALISER.put(TYPE, exceptionMessage.type);
        IO_BUFFER.flip();
        return new ZFrame(Arrays.copyOfRange(IO_BUFFER.elements(), 0, IO_BUFFER.limit() + SERIALISER_QUIRK));
    }

    private static void addDescriptor(final ZMsg result, final FrameType... frametypes) {
        byte[] descriptor = new byte[frametypes.length];
        for (int i = 0; i < descriptor.length; i++) {
            descriptor[i] = frametypes[i].value();
        }
        result.add(new ZFrame(descriptor));
    }

    private static ZFrame serialiseHeader(final CmwLightMessage msg) throws RdaLightException {
        IO_BUFFER.reset();
        SERIALISER.setBuffer(IO_BUFFER);
        SERIALISER.putHeaderInfo();
        SERIALISER.put(FieldName.REQ_TYPE_TAG.value(), msg.requestType.value());
        SERIALISER.put(FieldName.ID_TAG.value(), msg.id);
        SERIALISER.put(FieldName.DEVICE_NAME_TAG.value(), msg.deviceName);
        SERIALISER.put(FieldName.PROPERTY_NAME_TAG.value(), msg.propertyName);
        if (msg.updateType != null) {
            SERIALISER.put(FieldName.UPDATE_TYPE_TAG.value(), msg.updateType.value());
        }
        SERIALISER.put(FieldName.SESSION_ID_TAG.value(), msg.sessionId);
        // StartMarker marks start of Data Object
        putMap(SERIALISER, FieldName.OPTIONS_TAG.value(), msg.options);
        IO_BUFFER.flip();
        return new ZFrame(Arrays.copyOfRange(IO_BUFFER.elements(), 0, IO_BUFFER.limit() + SERIALISER_QUIRK));
    }

    private static ZFrame serialiseRequestContext(final CmwLightMessage.RequestContext requestContext) throws RdaLightException {
        IO_BUFFER.reset();
        SERIALISER.putHeaderInfo();
        SERIALISER.put(FieldName.SELECTOR_TAG.value(), requestContext.selector);
        putMap(SERIALISER, FieldName.FILTERS_TAG.value(), requestContext.filters);
        putMap(SERIALISER, FieldName.DATA_TAG.value(), requestContext.data);
        IO_BUFFER.flip();
        return new ZFrame(Arrays.copyOfRange(IO_BUFFER.elements(), 0, IO_BUFFER.limit() + SERIALISER_QUIRK));
    }

    private static ZFrame serialiseDataContext(final CmwLightMessage.DataContext dataContext) throws RdaLightException {
        IO_BUFFER.reset();
        SERIALISER.putHeaderInfo();
        SERIALISER.put(FieldName.CYCLE_NAME_TAG.value(), dataContext.cycleName);
        SERIALISER.put(FieldName.CYCLE_STAMP_TAG.value(), dataContext.cycleStamp);
        SERIALISER.put(FieldName.ACQ_STAMP_TAG.value(), dataContext.acqStamp);
        putMap(SERIALISER, FieldName.DATA_TAG.value(), dataContext.data);
        IO_BUFFER.flip();
        return new ZFrame(Arrays.copyOfRange(IO_BUFFER.elements(), 0, IO_BUFFER.limit() + SERIALISER_QUIRK));
    }

    private static void putMap(final CmwLightSerialiser serialiser, final String fieldName, final Map<String, Object> map) throws RdaLightException {
        if (map != null) {
            final WireDataFieldDescription dataFieldMarker = new WireDataFieldDescription(serialiser, serialiser.getParent(), -1,
                    fieldName, DataType.START_MARKER, -1, -1, -1);
            serialiser.putStartMarker(dataFieldMarker);
            for (final Map.Entry<String, Object> entry : map.entrySet()) {
                if (entry.getValue() instanceof String) {
                    serialiser.put(entry.getKey(), (String) entry.getValue());
                } else if (entry.getValue() instanceof Integer) {
                    serialiser.put(entry.getKey(), (Integer) entry.getValue());
                } else if (entry.getValue() instanceof Long) {
                    serialiser.put(entry.getKey(), (Long) entry.getValue());
                } else if (entry.getValue() instanceof Boolean) {
                    serialiser.put(entry.getKey(), (Boolean) entry.getValue());
                } else if (entry.getValue() instanceof Map) {
                    @SuppressWarnings("unchecked")
                    final Map<String, Object> subMap = (Map<String, Object>) entry.getValue();
                    putMap(serialiser, entry.getKey(), subMap);
                } else {
                    throw new RdaLightException("unsupported map entry type: " + entry.getValue().getClass().getCanonicalName());
                }
            }
            serialiser.putEndMarker(dataFieldMarker);
        }
    }

    private static CmwLightMessage getReplyFromHeader(final ZFrame firstFrame, final ZFrame header) throws RdaLightException {
        CmwLightMessage reply = new CmwLightMessage(MessageType.of(firstFrame.getData()[0]));
        IO_CLASS_SERIALISER.setDataBuffer(FastByteBuffer.wrap(header.getData()));
        final FieldDescription headerMap;
        try {
            headerMap = IO_CLASS_SERIALISER.parseWireFormat().getChildren().get(0);
            for (FieldDescription field : headerMap.getChildren()) {
                if (field.getFieldName().equals(FieldName.REQ_TYPE_TAG.value()) && field.getType() == byte.class) {
                    reply.requestType = RequestType.of((byte) (((WireDataFieldDescription) field).data()));
                } else if (field.getFieldName().equals(FieldName.ID_TAG.value()) && field.getType() == long.class) {
                    reply.id = (long) ((WireDataFieldDescription) field).data();
                } else if (field.getFieldName().equals(FieldName.DEVICE_NAME_TAG.value()) && field.getType() == String.class) {
                    reply.deviceName = (String) ((WireDataFieldDescription) field).data();
                } else if (field.getFieldName().equals(FieldName.OPTIONS_TAG.value())) {
                    reply.options = readMap(field);
                } else if (field.getFieldName().equals(FieldName.UPDATE_TYPE_TAG.value()) && field.getType() == byte.class) {
                    reply.updateType = UpdateType.of((byte) ((WireDataFieldDescription) field).data());
                } else if (field.getFieldName().equals(FieldName.SESSION_ID_TAG.value()) && field.getType() == String.class) {
                    reply.sessionId = (String) ((WireDataFieldDescription) field).data();
                } else if (field.getFieldName().equals(FieldName.PROPERTY_NAME_TAG.value()) && field.getType() == String.class) {
                    reply.propertyName = (String) ((WireDataFieldDescription) field).data();
                } else {
                    throw new RdaLightException("Unknown CMW header field: " + field.getFieldName());
                }
            }
        } catch (IllegalStateException e) {
            throw new RdaLightException("unparsable header: " + Arrays.toString(header.getData()) + "(" + header.toString() + ")", e);
        }
        if (reply.requestType == null) {
            throw new RdaLightException("Header does not contain request type field");
        }
        return reply;
    }

    private static Map<String, Object> readMap(final FieldDescription field) {
        Map<String, Object> result = null;
        for (FieldDescription dataField : field.getChildren()) {
            if (result == null) {
                result = new HashMap<>(); // NOPMD - necessary to allocate inside loop
            }
            //if ( 'condition' ) {
            // find out how to see if the field is itself a map
            // result.put(dataField.getFieldName(), readMap(dataField))
            // } else {
            result.put(dataField.getFieldName(), ((WireDataFieldDescription) dataField).data());
            //}
        }
        return result;
    }

    private static CmwLightMessage.ExceptionMessage parseExceptionMessage(final ZFrame exceptionBody) throws RdaLightException {
        if (exceptionBody == null) {
            throw new RdaLightException("malformed subscription exception");
        }
        final CmwLightMessage.ExceptionMessage exceptionMessage = new CmwLightMessage.ExceptionMessage();
        IO_CLASS_SERIALISER.setDataBuffer(FastByteBuffer.wrap(exceptionBody.getData()));
        final FieldDescription exceptionFields = IO_CLASS_SERIALISER.parseWireFormat().getChildren().get(0);
        for (FieldDescription field : exceptionFields.getChildren()) {
            if (CONTEXT_ACQ_STAMP.equals(field.getFieldName()) && field.getType() == long.class) {
                exceptionMessage.contextAcqStamp = (long) ((WireDataFieldDescription) field).data();
            } else if (CONTEXT_CYCLE_STAMP.equals(field.getFieldName()) && field.getType() == long.class) {
                exceptionMessage.contextCycleStamp = (long) ((WireDataFieldDescription) field).data();
            } else if (MESSAGE.equals(field.getFieldName()) && field.getType() == String.class) {
                exceptionMessage.message = (String) ((WireDataFieldDescription) field).data();
            } else if (TYPE.equals(field.getFieldName()) && field.getType() == byte.class) {
                exceptionMessage.type = (byte) ((WireDataFieldDescription) field).data();
            } else {
                throw new RdaLightException("Unsupported field in exception body: " + field.getFieldName());
            }
        }
        return exceptionMessage;
    }

    private static CmwLightMessage.RequestContext parseRequestContext(final @NotNull ZFrame contextData) throws RdaLightException {
        assert contextData != null : "contextData";
        CmwLightMessage.RequestContext requestContext = new CmwLightMessage.RequestContext();
        IO_CLASS_SERIALISER.setDataBuffer(FastByteBuffer.wrap(contextData.getData()));
        final FieldDescription contextMap;
        try {
            contextMap = IO_CLASS_SERIALISER.parseWireFormat().getChildren().get(0);
            for (FieldDescription field : contextMap.getChildren()) {
                if (field.getFieldName().equals(FieldName.SELECTOR_TAG.value()) && field.getType() == String.class) {
                    requestContext.selector = (String) ((WireDataFieldDescription) field).data();
                } else if (field.getFieldName().equals(FieldName.FILTERS_TAG.value())) {
                    for (FieldDescription dataField : field.getChildren()) {
                        if (requestContext.filters == null) {
                            requestContext.filters = new HashMap<>(); // NOPMD - necessary to allocate inside loop
                        }
                        requestContext.filters.put(dataField.getFieldName(), ((WireDataFieldDescription) dataField).data());
                    }
                } else if (field.getFieldName().equals(FieldName.DATA_TAG.value())) {
                    for (FieldDescription dataField : field.getChildren()) {
                        if (requestContext.data == null) {
                            requestContext.data = new HashMap<>(); // NOPMD - necessary to allocate inside loop
                        }
                        requestContext.data.put(dataField.getFieldName(), ((WireDataFieldDescription) dataField).data());
                    }
                } else {
                    throw new UnsupportedOperationException("Unknown field: " + field.getFieldName());
                }
            }
        } catch (IllegalStateException e) {
            throw new RdaLightException("unparsable context data: " + Arrays.toString(contextData.getData()) + "(" + new String(contextData.getData()) + ")", e);
        }
        return requestContext;
    }

    private static CmwLightMessage.DataContext parseContextData(final @NotNull ZFrame contextData) throws RdaLightException {
        assert contextData != null : "contextData";
        CmwLightMessage.DataContext dataContext = new CmwLightMessage.DataContext();
        IO_CLASS_SERIALISER.setDataBuffer(FastByteBuffer.wrap(contextData.getData()));
        final FieldDescription contextMap;
        try {
            contextMap = IO_CLASS_SERIALISER.parseWireFormat().getChildren().get(0);
            for (FieldDescription field : contextMap.getChildren()) {
                if (field.getFieldName().equals(FieldName.CYCLE_NAME_TAG.value()) && field.getType() == String.class) {
                    dataContext.cycleName = (String) ((WireDataFieldDescription) field).data();
                } else if (field.getFieldName().equals(FieldName.ACQ_STAMP_TAG.value()) && field.getType() == long.class) {
                    dataContext.acqStamp = (long) ((WireDataFieldDescription) field).data();
                } else if (field.getFieldName().equals(FieldName.CYCLE_STAMP_TAG.value()) && field.getType() == long.class) {
                    dataContext.cycleStamp = (long) ((WireDataFieldDescription) field).data();
                } else if (field.getFieldName().equals(FieldName.DATA_TAG.value())) {
                    for (FieldDescription dataField : field.getChildren()) {
                        if (dataContext.data == null) {
                            dataContext.data = new HashMap<>(); // NOPMD - necessary to allocate inside loop
                        }
                        dataContext.data.put(dataField.getFieldName(), ((WireDataFieldDescription) dataField).data());
                    }
                } else {
                    throw new UnsupportedOperationException("Unknown field: " + field.getFieldName());
                }
            }
        } catch (IllegalStateException e) {
            throw new RdaLightException("unparsable context data: " + Arrays.toString(contextData.getData()) + "(" + new String(contextData.getData()) + ")", e);
        }
        return dataContext;
    }

    private static void assertDescriptor(final byte[] descriptor, final FrameType... frameTypes) throws RdaLightException {
        if (descriptor.length != frameTypes.length) {
            throw new RdaLightException("descriptor does not match message type: \n  " + Arrays.toString(descriptor) + "\n  " + Arrays.toString(frameTypes));
        }
        for (int i = 1; i < descriptor.length; i++) {
            if (descriptor[i] != frameTypes[i].value()) {
                throw new RdaLightException("descriptor does not match message type: \n  " + Arrays.toString(descriptor) + "\n  " + Arrays.toString(frameTypes));
            }
        }
    }

    private static byte[] checkDescriptor(final ZFrame descriptorMsg, final ZFrame firstFrame) throws RdaLightException {
        if (firstFrame == null || !(Arrays.equals(firstFrame.getData(), new byte[] { MessageType.SERVER_REP.value() }) || Arrays.equals(firstFrame.getData(), new byte[] { MessageType.CLIENT_REQ.value() }))) {
            throw new RdaLightException("Expecting only messages of type Heartbeat or Reply but got: " + firstFrame);
        }
        if (descriptorMsg == null) {
            throw new RdaLightException("Message does not contain descriptor");
        }
        final byte[] descriptor = descriptorMsg.getData();
        if (descriptor[0] != FrameType.HEADER.value()) {
            throw new RdaLightException("First message of SERVER_REP has to be of type MT_HEADER but is: " + descriptor[0]);
        }
        return descriptor;
    }

    public static class RdaLightException extends Exception {
        private static final long serialVersionUID = 5197623305559702319L;
        public RdaLightException(final String msg) {
            super(msg);
        }

        public RdaLightException(final String msg, final Throwable e) {
            super(msg, e);
        }
    }
}
