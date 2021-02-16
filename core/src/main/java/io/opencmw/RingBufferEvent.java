package io.opencmw;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opencmw.utils.SharedPointer;

import com.lmax.disruptor.EventHandler;

@SuppressWarnings("PMD.TooManyMethods")
public class RingBufferEvent implements FilterPredicate, Cloneable {
    private final static Logger LOGGER = LoggerFactory.getLogger(RingBufferEvent.class);
    /**
     * local UTC event arrival time-stamp [ms]
     */
    public long arrivalTimeStamp;

    /**
     * reference to the parent's disruptor sequence ID number
     */
    public long parentSequenceNumber;

    /**
     * list of known filters. N.B. this
     */
    public final Filter[] filters;

    /**
     * domain object carried by this ring buffer event
     */
    public SharedPointer<Object> payload;

    /**
     * collection of exceptions that have been issued while handling this RingBuffer event
     */
    public final List<Throwable> throwables = new ArrayList<>();

    /**
     *
     * @param filterConfig static filter configuration
     */
    @SafeVarargs
    public RingBufferEvent(final Class<? extends Filter>... filterConfig) {
        assert filterConfig != null;
        this.filters = new Filter[filterConfig.length];
        for (int i = 0; i < filters.length; i++) {
            try {
                filters[i] = filterConfig[i].getConstructor().newInstance();
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                throw new IllegalArgumentException("filter initialisations error - could not instantiate class:" + filterConfig[i], e);
            }
        }
        clear();
    }

    @Override
    @SuppressWarnings({ "unchecked", "MethodDoesntCallSuperMethod" })
    public RingBufferEvent clone() { // NOSONAR NOPMD we do not want to call super (would be kind of stupid)
        final RingBufferEvent retVal = new RingBufferEvent(Arrays.stream(filters).map(Filter::getClass).toArray(Class[] ::new));
        this.copyTo(retVal);
        return retVal;
    }

    public void copyTo(RingBufferEvent other) {
        other.arrivalTimeStamp = arrivalTimeStamp;
        other.parentSequenceNumber = parentSequenceNumber;
        for (int i = 0; i < other.filters.length; i++) {
            filters[i].copyTo(other.filters[i]);
        }
        other.payload = payload == null ? null : payload.getCopy();
        other.throwables.clear();
        other.throwables.addAll(throwables);
    }

    public <T extends Filter> T getFilter(final Class<T> filterType) {
        for (Filter filter : filters) {
            if (filter.getClass().isAssignableFrom(filterType)) {
                return filterType.cast(filter);
            }
        }
        final StringBuilder builder = new StringBuilder();
        builder.append("requested filter type '").append(filterType.getSimpleName()).append(" not part of ").append(RingBufferEvent.class.getSimpleName()).append(" definition: ");
        printToStringArrayList(builder, "[", "]", (Object[]) filters);
        throw new IllegalArgumentException(builder.toString());
    }

    public boolean matches(final Predicate<RingBufferEvent> predicate) {
        return predicate.test(this);
    }

    public <T extends Filter> boolean matches(Class<T> filterType, final Predicate<T> predicate) {
        return predicate.test(getFilter(filterType));
    }

    /**
     * @param payloadType required payload class-type
     * @return {@code true} if payload is defined and matches type
     */
    public boolean matches(Class<?> payloadType) {
        return payload != null && payload.getType() != null && payloadType.isAssignableFrom(payload.getType());
    }

    public final void clear() {
        arrivalTimeStamp = 0L;
        parentSequenceNumber = -1L;
        for (Filter filter : filters) {
            filter.clear();
        }
        throwables.clear();
        if (payload != null) {
            payload.release();
        }
        payload = null; // NOPMD - null use on purpose (faster/easier than an Optional)
    }

    @Override
    public <R extends Filter> boolean test(final Class<R> filterClass, final Predicate<R> filterPredicate) {
        return filterPredicate.test(filterClass.cast(getFilter(filterClass)));
    }

    @Override
    public String toString() {
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX", Locale.UK);
        final StringBuilder builder = new StringBuilder();
        builder.append(RingBufferEvent.class.getSimpleName()).append(": arrivalTimeStamp ").append(arrivalTimeStamp).append(" (").append(sdf.format(arrivalTimeStamp)).append(") parent sequence number: ").append(parentSequenceNumber).append(" - filter: ");
        printToStringArrayList(builder, "[", "]", (Object[]) filters);
        if (!throwables.isEmpty()) {
            builder.append(" - exceptions (n=").append(throwables.size()).append("):\n");
            for (Throwable t : throwables) {
                builder.append(getPrintableStackTrace(t)).append('\n');
            }
        }
        return builder.toString();
    }

    public static void printToStringArrayList(final StringBuilder builder, final String prefix, final String postFix, final Object... items) {
        if (prefix != null && !prefix.isBlank()) {
            builder.append(prefix);
        }
        boolean more = false;
        for (Object o : items) {
            if (more) {
                builder.append(", ");
            }
            builder.append(o.getClass().getSimpleName()).append(':').append(o.toString());
            more = true;
        }
        if (postFix != null && !postFix.isBlank()) {
            builder.append(postFix);
        }
        //TODO: refactor into a common utility class
    }

    public static String getPrintableStackTrace(final Throwable t) {
        if (t == null) {
            return "<null stack trace>";
        }
        final StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        return sw.toString();
        //TODO: refactor into a common utility class
    }

    /**
     * default buffer element clearing handler
     */
    public static class ClearEventHandler implements EventHandler<RingBufferEvent> {
        @Override
        public void onEvent(RingBufferEvent event, long sequence, boolean endOfBatch) {
            LOGGER.atTrace().addArgument(sequence).addArgument(endOfBatch).log("clearing RingBufferEvent sequence = {} endOfBatch = {}");
            event.clear();
        }
    }

    @Override
    public boolean equals(final Object obj) { // NOSONAR NOPMD - npath complexity unavoidable for performance reasons
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof RingBufferEvent)) {
            return false;
        }
        final RingBufferEvent other = (RingBufferEvent) obj;
        if (hashCode() != other.hashCode()) {
            return false;
        }
        if (arrivalTimeStamp != other.arrivalTimeStamp) {
            return false;
        }
        if (parentSequenceNumber != other.parentSequenceNumber) {
            return false;
        }
        if (!Arrays.equals(filters, other.filters)) {
            return false;
        }
        if (!Objects.equals(payload, other.payload)) {
            return false;
        }
        return throwables.equals(other.throwables);
    }

    @Override
    public int hashCode() {
        int result = (int) (arrivalTimeStamp ^ (arrivalTimeStamp >>> 32));
        result = 31 * result + (int) (parentSequenceNumber ^ (parentSequenceNumber >>> 32));
        result = 31 * result + Arrays.hashCode(filters);
        result = 31 * result + (payload == null ? 0 : payload.hashCode());
        result = 31 * result + throwables.hashCode();
        return result;
    }
}
