package de.gsi.microservice.filter;

import de.gsi.microservice.Filter;
import de.gsi.microservice.datasource.DataSourcePublisher;
import de.gsi.serializer.IoSerialiser;

public class DataSourceFilter implements Filter {
    public ReplyType eventType = ReplyType.UNKNOWN;
    public Class<? extends IoSerialiser> protocolType;
    public String device;
    public String property;
    public DataSourcePublisher.ThePromisedFuture<?> future;
    public String context;

    @Override
    public void clear() {
        eventType = ReplyType.UNKNOWN;
        device = "UNKNOWN";
        property = "UNKNOWN";
        future = null;
        context = "";
    }

    @Override
    public void copyTo(final Filter other) {
        if (other instanceof DataSourceFilter) {
            final DataSourceFilter otherDSF = (DataSourceFilter) other;
            otherDSF.eventType = eventType;
            otherDSF.device = device;
            otherDSF.property = property;
            otherDSF.future = future;
            otherDSF.context = context;
        }
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
            this.id = (byte)id;
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
