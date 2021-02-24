package io.opencmw.client;

import java.util.Objects;

import io.opencmw.Filter;
import io.opencmw.serialiser.IoSerialiser;

public class DataSourceFilter implements Filter {
    public ReplyType eventType = ReplyType.UNKNOWN;
    public Class<? extends IoSerialiser> protocolType;
    public String endpoint = "";
    public DataSourcePublisher.ThePromisedFuture<?, ?> future;
    public long arrivalTimestamp = -1L;

    @Override
    public void clear() {
        eventType = ReplyType.UNKNOWN;
        protocolType = null; // NOPMD - have to clear the future because the events are reused
        endpoint = ""; // NOPMD - have to clear the future because the events are reused
        future = null; // NOPMD - have to clear the future because the events are reused
        arrivalTimestamp = -1L;
    }

    @Override
    public void copyTo(final Filter other) {
        if (other instanceof DataSourceFilter) {
            final DataSourceFilter otherDSF = (DataSourceFilter) other;
            otherDSF.eventType = eventType;
            otherDSF.endpoint = endpoint;
            otherDSF.future = future;
            otherDSF.protocolType = protocolType;
            otherDSF.arrivalTimestamp = arrivalTimestamp;
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final DataSourceFilter that = (DataSourceFilter) o;
        return arrivalTimestamp == that.arrivalTimestamp && eventType == that.eventType && Objects.equals(protocolType, that.protocolType) && Objects.equals(endpoint, that.endpoint) && Objects.equals(future, that.future);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventType, protocolType, endpoint, future, arrivalTimestamp);
    }

    /**
     * internal enum to track different get/set/subscribe/... transactions
     */
    public enum ReplyType {
        SUBSCRIBE(0),
        GET(1),
        SET(2),
        UNSUBSCRIBE(3),
        UNKNOWN(-1);

        private final byte id;
        ReplyType(int id) {
            this.id = (byte) id;
        }

        public byte getID() {
            return id;
        }

        public static ReplyType valueOf(final int id) {
            for (ReplyType mode : ReplyType.values()) {
                if (mode.getID() == id) {
                    return mode;
                }
            }
            return UNKNOWN;
        }
    }
}
