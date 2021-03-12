package io.opencmw.client;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

import org.jetbrains.annotations.NotNull;
import org.zeromq.ZContext;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

import io.opencmw.serialiser.IoSerialiser;
import io.opencmw.utils.NoDuplicatesList;

/**
 * Interface for DataSources to be added to an EventStore by a single event loop.
 * Should provide a static boolean matches(String address) function to determine whether
 * it is eligible for a given address.
 */
public abstract class DataSource implements AutoCloseable {
    private static final List<Factory> IMPLEMENTATIONS = Collections.synchronizedList(new NoDuplicatesList<>());

    private DataSource() {
        // prevent implementers from implementing default constructor
    }

    /**
     * Constructor
     * @param endpoint Endpoint to subscribe to
     */
    protected DataSource(final @NotNull URI endpoint) {
        if (!getFactory().matches(endpoint)) {
            throw new UnsupportedOperationException(this.getClass().getName() + " DataSource Implementation does not support endpoint: " + endpoint);
        }
    }

    /**
     * Get Socket to wait for in the event loop.
     * The main event thread will wait for data becoming available on this socket.
     * The socket might be used to receive the actual data or it might just be used to notify the main thread.
     * @return a Socket for the event loop to wait upon
     */
    public abstract Socket getSocket();

    /**
     * Gets called whenever data is available on the DataSource's socket.
     * Should then try to receive data and return any results back to the calling event loop.
     * @return null if there is no more data available, a Zero length ZMsg if there was data which was only used internally
     * or a ZMsg with [reqId, endpoint, byte[] data, [byte[] optional RBAC token]]
     */
    public abstract ZMsg getMessage();

    /**
     * Perform housekeeping tasks like connection management, heartbeats, subscriptions, etc
     * @return UTC time-stamp in [ms] when the next housekeeping duties should be performed
     */
    public abstract long housekeeping();

    /**
     * Subscribe to this endpoint
     * @param reqId the id to join the result of this subscribe with
     * @param rbacToken byte array containing signed body hash-key and corresponding RBAC role
     */
    public abstract void subscribe(final String reqId, final URI endpoint, final byte[] rbacToken);

    /**
     * Unsubscribe from the endpoint of this DataSource.
     */
    public abstract void unsubscribe(final String reqId);

    /**
     * Perform a get request on this endpoint.
     * @param requestId request id which later allows to match the returned value to this query.
     *                  This is the only mandatory parameter, all the following may be null.
     * @param endpoint extend the filters originally supplied to the endpoint e.g. "ctx=selector&amp;channel=channelA"
     * @param data The serialised data which can be used by the get call
     * @param rbacToken byte array containing signed body hash-key and corresponding RBAC role
     */
    public abstract void get(final String requestId, final URI endpoint, final byte[] data, final byte[] rbacToken);

    /**
     * Perform a set request on this endpoint using additional filters
     * @param requestId request id which later allows to match the returned value to this query.
     *                  This is the only mandatory parameter, all the following may be null.
     * @param endpoint extend the filters originally supplied to the endpoint e.g. "ctx=selector&amp;channel=channelA"
     * @param data The serialised data which can be used by the get call
     * @param rbacToken byte array containing signed body hash-key and corresponding RBAC role
     */
    public abstract void set(final String requestId, final URI endpoint, final byte[] data, final byte[] rbacToken);

    protected abstract Factory getFactory();

    /**
     * Factory method to get a DataSource for a given endpoint
     * @param endpoint endpoint address
     * @return if there is a DataSource implementation for the protocol of the endpoint return a Factory to create a new
     *         Instance of this DataSource
     * @throws UnsupportedOperationException in case there is no valid implementation
     */
    public static Factory getFactory(final @NotNull URI endpoint) {
        for (Factory factory : IMPLEMENTATIONS) {
            if (factory.matches(endpoint)) {
                return factory;
            }
        }
        throw new UnsupportedOperationException("No DataSource implementation available for endpoint: " + endpoint);
    }

    public static void register(final Factory factory) {
        IMPLEMENTATIONS.add(0, factory); // custom added implementations are added in front to be discovered first
    }

    protected interface Factory {
        /**
         * @return returns the list of applicable schemes (and protocols this resolver can handle) this resolver can handle
         */
        List<String> getApplicableSchemes();
        List<DnsResolver> getRegisteredDnsResolver();
        Class<? extends IoSerialiser> getMatchingSerialiserType(final @NotNull URI endpoint);
        DataSource newInstance(final ZContext context, final @NotNull URI endpoint, final @NotNull Duration timeout, final @NotNull ExecutorService executorService, final @NotNull String clientId);

        default boolean matches(final @NotNull URI endpoint) {
            final String scheme = Objects.requireNonNull(endpoint.getScheme(), "required URI has no scheme defined: " + endpoint);
            return getApplicableSchemes().stream().anyMatch(s -> s.equalsIgnoreCase(scheme));
        }

        default void registerDnsResolver(final @NotNull DnsResolver resolver) {
            final ArrayList<String> list = new ArrayList<>(getApplicableSchemes());
            list.retainAll(resolver.getApplicableSchemes());
            if (list.isEmpty()) {
                throw new IllegalArgumentException("resolver schemes not compatible with this DataSource: " + resolver);
            }
            getRegisteredDnsResolver().add(resolver);
        }
    }
}
