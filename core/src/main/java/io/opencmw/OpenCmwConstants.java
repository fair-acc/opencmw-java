package io.opencmw;

import static io.opencmw.OpenCmwProtocol.MdpSubProtocol.PROT_CLIENT;

import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Locale;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.zeromq.ZMQ;

import io.opencmw.serialiser.annotations.MetaInfo;
import io.opencmw.utils.Settings;
import io.opencmw.utils.SystemProperties;

import com.google.auto.service.AutoService;

/**
 * OpenCMW global constant definitions:
 *
 * <p>
 * The broker, client and worker are controlled by the following environment variables:
 * <ul>
 * <li> 'OpenCMW.heartBeat' [ms]: default (1000 ms) heart-beat time-out [ms]</li>
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
@SuppressWarnings("PMD.UseUtilityClass")
@AutoService(Settings.class)
public final class OpenCmwConstants implements Settings {
    public static final String WILDCARD = "*";
    public static final String SCHEME_INPROC = "inproc";
    public static final String SCHEME_HTTP = "http";
    public static final String SCHEME_HTTPS = "https";
    public static final String SCHEME_MDP = "mdp";
    public static final String SCHEME_MDS = "mds";
    public static final String SCHEME_TCP = "tcp";
    public static final String ADDRESS_GIVEN = "address given: ";
    /** heart-beat interval in milli-seconds */
    @MetaInfo(unit = "ms", description = "heart-beat interval in milli-seconds")
    public static final Integer HEARTBEAT_INTERVAL = 1000;
    /** heart-beat liveness - 3-5 is reasonable
     * N.B. heartbeat expires when last heartbeat message is more than 'heartBeat' * 'heartBeatLiveness' ms ago.*/
    @MetaInfo(unit = "int", description = "heart-beat liveness (3-5 is reasonable)\nN.B. heartbeat expires when last heartbeat message is more than 'heartBeat' * 'heartBeatLiveness' ms ago.")
    public static final Integer HEARTBEAT_LIVENESS = 3;
    /** subscription time-out in milli-seconds */
    @MetaInfo(unit = "ms", description = "subscription time-out in milli-seconds")
    public static final Integer SUBSCRIPTION_TIMEOUT = 1000;
    /** number IO threads dedicated to network IO (ZeroMQ recommendation: 1 thread per 1 GBit/s) */
    @MetaInfo(unit = "int", description = "number IO threads dedicated to network IO (ZeroMQ recommendation: 1 thread per 1 GBit/s)")
    public static final Integer N_IO_THREADS = 1;
    /** high-water-mark ie. the number of message frames stored per socket before being dropped */
    private static final String HWM_DESCRIPTION = //
            "high-water-mark ie. the number of message frames stored per socket before being dropped\n"
            + "N.B. '0' implies that the number of frames is only limited by the available memory.\n"
            + "This is a ZeroMQ tuning parameter. A typical OpenCMW message consists of at least 8-9 frames";
    //** time-out in seconds after which unanswered client messages are being deleted */
    @MetaInfo(unit = "<int>", description = HWM_DESCRIPTION)
    public static final Integer HIGH_WATER_MARK = 0;
    /** time-out in seconds after which unanswered client messages are being deleted */
    @MetaInfo(unit = "s", description = "time-out in seconds after which unanswered client messages are being deleted\nN.B. '0' or negative disables this time-out.")
    public static final Integer CLIENT_TIMEOUT = 0; // [s]
    /** DNS time-out in seconds after which an unresponsive client is dropped from the DNS table */
    @MetaInfo(unit = "s", description = "DNS time-out in seconds after which an unresponsive client is dropped from the DNS table\nN.B. if registered, a heart-beat challenge will be send that needs to be replied with a READY command")
    public static final Integer DNS_TIMEOUT = 10;
    /** after n1 failed attempts the retry-'heartBeat interval is increased  10-fold */
    @MetaInfo(unit = "n1", description = "after <n1> failed attempts the retry-'heartBeat interval is increased  10-fold")
    public static final Integer RECONNECT_THRESHOLD1 = 3;
    /** after n2 failed attempts the retry-'heartBeat interval is increased 100-fold */
    @MetaInfo(unit = "n2", description = "after <n2> failed attempts the retry-'heartBeat interval is increased 100-fold")
    public static final Integer RECONNECT_THRESHOLD2 = 6;

    public static void init() {
        SystemProperties.addCommandOptions(OpenCmwConstants.class);
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
        socket.setHWM(HIGH_WATER_MARK);
        socket.setHeartbeatContext(PROT_CLIENT.getData());
        socket.setHeartbeatTtl(HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS);
        socket.setHeartbeatTimeout(HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS);
        socket.setHeartbeatIvl(HEARTBEAT_INTERVAL);
        socket.setLinger(HEARTBEAT_INTERVAL);
    }

    public static URI stripPathTrailingSlash(final @NotNull URI address) {
        try {
            return new URI(address.getScheme(), address.getAuthority(), StringUtils.stripEnd(address.getPath(), "/"), address.getQuery(), null);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(ADDRESS_GIVEN + address, e);
        }
    }
}
