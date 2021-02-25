package io.opencmw.filter;

import static io.opencmw.OpenCmwProtocol.Command;
import static io.opencmw.OpenCmwProtocol.EMPTY_URI;

import java.net.URI;
import java.util.Objects;
import java.util.function.Predicate;

import io.opencmw.Filter;

public class EvtTypeFilter implements Filter {
    public static final String KEY = "evtType";
    public DataType evtType = DataType.UNKNOWN;
    public Command updateType = Command.UNKNOWN;
    public URI property = EMPTY_URI;
    protected int hashCode = 0; // NOPMD

    public EvtTypeFilter() {
        // default constructor
    }

    public EvtTypeFilter(final String ctxValue) {
        try {
            final String[] subComponent = ctxValue.split(":");
            if (subComponent.length < 2 || subComponent.length > 3) {
                throw new IllegalArgumentException("cannot parse '" + ctxValue + '\'');
            }
            if (!EvtTypeFilter.class.getSimpleName().equalsIgnoreCase(subComponent[0])) {
                throw new IllegalArgumentException("cannot parse '" + ctxValue + '\'');
            }
            evtType = DataType.valueOf(subComponent[1]);
            if (subComponent.length == 3) { // NOPMD
                property = URI.create(subComponent[2]);
            }
        } catch (Exception e) { // NOPMD
            throw new IllegalArgumentException("cannot parse '" + ctxValue + '\'', e);
        }
    }

    public static Predicate<EvtTypeFilter> isTimingData() {
        return t -> t.evtType == DataType.TIMING_EVENT;
    }

    public static Predicate<EvtTypeFilter> isTimingData(final String propertyName) {
        return t -> t.evtType == DataType.TIMING_EVENT && Objects.equals(t.property.getPath(), propertyName);
    }

    public static Predicate<EvtTypeFilter> isDeviceData() {
        return t -> t.evtType == DataType.DEVICE_DATA;
    }

    public static Predicate<EvtTypeFilter> isDeviceData(final String propertyName) {
        return t -> t.evtType == DataType.DEVICE_DATA && Objects.equals(t.property.getPath(), propertyName);
    }

    public static Predicate<EvtTypeFilter> isSettingsData() {
        return t -> t.evtType == DataType.SETTING_SUPPLY_DATA;
    }

    public static Predicate<EvtTypeFilter> isSettingsData(final String propertyName) {
        return t -> t.evtType == DataType.SETTING_SUPPLY_DATA && Objects.equals(t.property.getPath(), propertyName);
    }

    @Override
    public void clear() {
        hashCode = 0;
        evtType = DataType.UNKNOWN;
        updateType = Command.UNKNOWN;
        property = EMPTY_URI;
    }

    @Override
    public void copyTo(final Filter other) {
        if (!(other instanceof EvtTypeFilter)) {
            return;
        }
        ((EvtTypeFilter) other).hashCode = this.hashCode;
        ((EvtTypeFilter) other).evtType = this.evtType;
        ((EvtTypeFilter) other).property = this.property;
        ((EvtTypeFilter) other).updateType = this.updateType;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof EvtTypeFilter)) {
            return false;
        }
        final EvtTypeFilter other = (EvtTypeFilter) obj;
        return evtType == other.evtType && updateType == other.updateType && Objects.equals(property, other.property);
    }

    @Override
    public String getKey() {
        return KEY;
    }

    @Override
    public String getValue() {
        return EvtTypeFilter.class.getSimpleName() + ":" + evtType + ":" + property;
    }

    @Override
    public EvtTypeFilter get(final String value) {
        try {
            return new EvtTypeFilter(value);
        } catch (Exception e) { // NOPMD
            return null;
        }
    }

    @Override
    public boolean matches(final Filter other) {
        return equals(other);
    }

    @Override
    public int hashCode() {
        return hashCode == 0 ? hashCode = Objects.hash(evtType, updateType, property) : hashCode;
    }

    @Override
    public String toString() {
        return '[' + EvtTypeFilter.class.getSimpleName() + ": evtType=" + evtType + " typeName='" + property + "']";
    }

    public enum DataType {
        TIMING_EVENT,
        AGGREGATE_DATA,
        DEVICE_DATA,
        SETTING_SUPPLY_DATA,
        PROCESSED_DATA,
        OTHER,
        UNKNOWN
    }
}
