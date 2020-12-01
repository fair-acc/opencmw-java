package de.gsi.microservice.datasource;

import java.time.Duration;
import java.util.List;

import de.gsi.dataset.utils.NoDuplicatesList;
import de.gsi.serializer.IoSerialiser;
import org.zeromq.ZContext;
import org.zeromq.ZMsg;
import org.zeromq.ZMQ.Socket;

/**
 * Interface for DataSources to be added to an EventStore by a single event loop.
 * Should provide a static boolean matches(String address) function to determine whether
 * it is eligible for a given address.
 */
public abstract class DataSource {
    private static final List<Factory> IMPLEMENTATIONS = new NoDuplicatesList<>();

    private DataSource() {
        // prevent implementers from implementing default constructor
    }

    /**
     * Constructor
     * @param context ZeroMQ context to use
     * @param endpoint Endpoint to subscribe to
     * @param timeOut after which the request defaults to a time-out exception (no data)
     * @param clientId client id to be able to process the notification updates.
     * @param filters The serialised filters which will determine which data to update
     */
    public DataSource(final ZContext context, final String endpoint, final Duration timeOut, final String clientId, final byte[] filters) {
        if (endpoint == null || endpoint.isBlank() || !getFactory().matches(endpoint)) {
            throw new UnsupportedOperationException(this.getClass().getName() + " DataSource Implementation does not support endpoint: " + endpoint);
        }
    }

    /**
     * Factory method to get a DataSource for a given endpoint
     * @param endpoint endpoint address
     * @return if there is a DataSource implementation for the protocol of the endpoint return a Factory to create a new Instance of this DataSource
     * @throws UnsupportedOperationException in case there is no valid implementation
     */
    public static Factory getFactory(final String endpoint) {
        for (Factory factory : IMPLEMENTATIONS) {
            if (factory.matches(endpoint)) {
                return factory;
            }
        }
        throw new UnsupportedOperationException("No DataSource implementation available for endpoint: " + endpoint);
    }

    public static void register(final Factory factory) {
        IMPLEMENTATIONS.add(factory);
    }

    /**
     * Get Socket to wait for in the event loop.
     * The main event thread will wait for data becoming available on this socket.
     * The socket might be used to receive the actual data or it might just be used to notify the main thread.
     * @return a Socket for the event loop to wait upon
     */
    public abstract Socket getSocket();

    protected abstract Factory getFactory();

    /**
     * Address format is protocol://[server[:port]/]device/property?ctx=selector&amp;filter=stringvalue&amp;anotherFilter=int:5.
     * If the server part is omitted, a name service should be consulted to get the server address for the device.
     * @return the address this DataSource is connected to/reading from.
     */
    public abstract String getEndpoint();

    /**
     * Gets called whenever data is available on the DataSoure's socket.
     * Should then try to receive data and return any results back to the calling event loop.
     * @return null if there is no more data available, a Zero length Zmsg if there was data which was only used internally
     * or a ZMsg with [reqId, endpoint, byte[] data, [byte[] optional RBAC token]]
     */
    public abstract ZMsg getMessage();

    /**
     * Perform housekeeping tasks like connection management, heartbeats, subscriptions, etc
     * @return next time housekeeping duties should be performed
     */
    public abstract long housekeeping();

    /**
     * Subscribe to this endpoint
     * @param reqId the id to join the result of this subscribe with
     * @param rbacToken byte array containing signed body hash-key and corresponding RBAC role
     */
    public abstract void subscribe(final String reqId, final byte[] rbacToken);

    /**
     * Unsubscribe from the endpoint of this DataSource.
     */
    public abstract void unsubscribe(); // N.B. no argument needed since DataSource == 1 get/set/subscribe topic

    /**
     * Perform a get request on this endpoint.
     * @param requestId request id which later allows to match the returned value to this query.
     *                  This is the only mandatory parameter, all the following may be null.
     * @param filterPattern extend the filters originally supplied to the endpoint e.g. "ctx=selector&amp;channel=chanA"
     * @param filters The serialised filters which will determine which data to update
     * @param data The serialised data which can be used by the get call
     * @param rbacToken byte array containing signed body hash-key and corresponding RBAC role
     */
    public abstract void get(final String requestId, final String filterPattern, final byte[] filters, final byte[] data, final byte[] rbacToken);

    /**
     * Perform a set request on this endpoint using additional filters
     * @param requestId request id which later allows to match the returned value to this query.
     *                  This is the only mandatory parameter, all the following may be null.
     * @param filterPattern extend the filters originally supplied to the endpoint e.g. "ctx=selector&amp;channel=chanA"
     * @param filters The serialised filters which will determine which data to update
     * @param data The serialised data which can be used by the get call
     * @param rbacToken byte array containing signed body hash-key and corresponding RBAC role
     */
    public abstract void set(final String requestId, final String filterPattern, final byte[] filters, final byte[] data, final byte[] rbacToken);

    protected interface Factory {
        boolean matches(final String endpoint);
        Class<? extends IoSerialiser> getMatchingSerialiserType(final String endpoint);
        DataSource newInstance(final ZContext context, final String endpoint, final Duration timeout, final String clientId, final byte[] filters);
    }
}