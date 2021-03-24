package io.opencmw;

import static io.opencmw.OpenCmwProtocol.MdpSubProtocol.PROT_CLIENT;

import java.net.*;
import java.util.Locale;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.zeromq.ZMQ;

import io.opencmw.utils.SystemProperties;

/**
 * OpenCMW global constant definitions:
 *
 * <p>
 * The broker, client and worker are controlled by the following environment variables:
 * <ul>
 * <li> 'OpenCMW.heartBeat' [ms]: default (2500 ms) heart-beat time-out [ms]</li>
 * <li> 'OpenCMW.heartBeatLiveness' []: default (3) heart-beat liveness - 3-5 is reasonable
 * <small>N.B. heartbeat expires when last heartbeat message is more than HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS ms ago.
 * this implies also, that worker must either return their message within 'HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS ms'
 * or decouple their secondary handler interface into another thread.</small></li>
 *
 * <li>'OpenCMW.clientTimeOut' [s]: default (3600, i.e. 1h) time-out after which unanswered client messages/infos are being deleted</li>
 * <li>'OpenCMW.nIoThreads' []: default (2) IO threads dedicated to network IO (ZeroMQ recommendation 1 thread per 1 GBit/s)</li>
 * <li>'OpenCMW.dnsTimeOut' [s]: default (60) DNS time-out after which an unresponsive client is dropped from the DNS table
 * <small>N.B. if registered, a HEARTBEAT challenge will be send that needs to be replied with a READY command/re-registering</small></li>
 * </ul>
 */
public final class OpenCmwConstants {
    public static final String WILDCARD = "*";
    public static final String SCHEME_INPROC = "inproc";
    public static final String SCHEME_HTTP = "http";
    public static final String SCHEME_HTTPS = "https";
    public static final String SCHEME_MDP = "mdp";
    public static final String SCHEME_MDS = "mds";
    public static final String SCHEME_TCP = "tcp";
    public static final String HEARTBEAT = "OpenCMW.heartBeat";
    public static final long HEARTBEAT_DEFAULT = 1000L;
    public static final String HEARTBEAT_LIVENESS = "OpenCMW.heartBeatLiveness";
    public static final int HEARTBEAT_LIVENESS_DEFAULT = 3;
    public static final String SUBSCRIPTION_TIMEOUT = "OpenCMW.subscriptionTimeOut";
    public static final long SUBSCRIPTION_TIMEOUT_DEFAULT = 1000L;
    public static final String N_IO_THREADS = "OpenCMW.nIoThreads";
    public static final int N_IO_THREADS_DEFAULT = 1; //
    public static final String HIGH_WATER_MARK = "OpenCMW.hwm";
    public static final int HIGH_WATER_MARK_DEFAULT = 0; //
    public static final String CLIENT_TIMEOUT = "OpenCMW.clientTimeOut"; // [s]
    public static final long CLIENT_TIMEOUT_DEFAULT = 0L; // [s]
    public static final String DNS_TIMEOUT = "OpenCMW.dnsTimeOut"; // [s]
    public static final long DNS_TIMEOUT_DEFAULT = 10L; // [s]
    public static final String ADDRESS_GIVEN = "address given: ";
    public static final String RECONNECT_THRESHOLD1 = "OpenCMW.reconnectThreshold1"; // []
    public static final int DEFAULT_RECONNECT_THRESHOLD1 = 3; // []
    public static final String RECONNECT_THRESHOLD2 = "OpenCMW.reconnectThreshold2"; // []
    public static final int DEFAULT_RECONNECT_THRESHOLD2 = 6; // []

    private OpenCmwConstants() {
        // this is a utility class
    }

    public static String getDeviceName(final @NotNull URI endpoint) {
        return StringUtils.stripStart(endpoint.getPath(), "/").split("/", 2)[0];
    }

    public static String getLocalHostName() {
        String ip;
        try (DatagramSocket socket = new DatagramSocket()) {
            socket.connect(InetAddress.getByName("8.8.8.8"), 10_002); // NOPMD - bogus hardcoded IP acceptable in this context
            if (socket.getLocalAddress() == null) {
                throw new UnknownHostException("bogus exception can be ignored");
            }
            ip = socket.getLocalAddress().getHostAddress();

            if (ip != null) {
                return ip;
            }
            return "localhost";
        } catch (final SocketException | UnknownHostException e) {
            throw new IllegalStateException("cannot resolve own host IP address", e);
        }
    }

    public static String getPropertyName(final @NotNull URI endpoint) {
        return StringUtils.stripStart(endpoint.getPath(), "/").split("/", 2)[1];
    }

    public static URI replacePath(final @NotNull URI address, final @NotNull String pathReplacement) {
        if (pathReplacement.equals(address.getPath())) {
            return address;
        }
        try {
            return new URI(address.getScheme(), address.getAuthority(), pathReplacement, address.getQuery(), null);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(ADDRESS_GIVEN + address, e);
        }
    }

    public static URI replaceQuery(final @NotNull URI address, final String queryReplacement) {
        if (queryReplacement != null && queryReplacement.equals(address.getQuery())) {
            return address;
        }
        try {
            return new URI(address.getScheme(), address.getAuthority(), address.getPath(), queryReplacement, null);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(ADDRESS_GIVEN + address, e);
        }
    }

    public static URI replaceScheme(final @NotNull URI address, final @NotNull String schemeReplacement) {
        if (address.getScheme() != null && address.getScheme().toLowerCase(Locale.UK).equals(SCHEME_INPROC)) {
            return address;
        }
        try {
            return new URI(schemeReplacement, address.getAuthority(), address.getPath(), address.getQuery(), null);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(ADDRESS_GIVEN + address, e);
        }
    }

    public static URI replaceSchemeKeepOnlyAuthority(final @NotNull URI address, final @NotNull String schemeReplacement) {
        return URI.create(schemeReplacement + "://" + Objects.requireNonNull(address.getAuthority(), "authority is null: " + address));
    }

    public static URI resolveHost(final @NotNull URI address, final @NotNull String hostName) {
        if (((address.getScheme() != null && address.getScheme().toLowerCase(Locale.UK).equals(SCHEME_INPROC)) || (address.getAuthority() == null || !address.getAuthority().toLowerCase(Locale.UK).contains(WILDCARD)))) {
            return address;
        }
        try {
            final String[] splitAuthority = StringUtils.split(address.getAuthority(), ":");
            final int port = splitAuthority.length >= 2 ? Integer.parseInt(splitAuthority[1]) : address.getPort();
            return new URI(address.getScheme(), null, hostName, port, address.getPath(), address.getQuery(), null);
        } catch (URISyntaxException | NumberFormatException e) {
            throw new IllegalArgumentException(ADDRESS_GIVEN + address, e);
        }
    }

    public static void setDefaultSocketParameters(final @NotNull ZMQ.Socket socket) {
        final int heartBeatInterval = (int) SystemProperties.getValueIgnoreCase(HEARTBEAT, HEARTBEAT_DEFAULT);
        final int liveness = SystemProperties.getValueIgnoreCase(HEARTBEAT_LIVENESS, HEARTBEAT_LIVENESS_DEFAULT);
        socket.setHWM(SystemProperties.getValueIgnoreCase(HIGH_WATER_MARK, HIGH_WATER_MARK_DEFAULT));
        socket.setHeartbeatContext(PROT_CLIENT.getData());
        socket.setHeartbeatTtl(heartBeatInterval * liveness);
        socket.setHeartbeatTimeout(heartBeatInterval * liveness);
        socket.setHeartbeatIvl(heartBeatInterval);
        socket.setLinger(heartBeatInterval);
    }

    public static URI stripPathTrailingSlash(final @NotNull URI address) {
        try {
            return new URI(address.getScheme(), address.getAuthority(), StringUtils.stripEnd(address.getPath(), "/"), address.getQuery(), null);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(ADDRESS_GIVEN + address, e);
        }
    }
}
