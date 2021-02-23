package io.opencmw;

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
    public static final String SCHEME_HTTP = "http://";
    public static final String SCHEME_HTTPS = "https://";
    public static final String SCHEME_MDP = "mdp://";
    public static final String SCHEME_MDS = "mds://";
    public static final String SCHEME_TCP = "tcp://";
    public static final String HEARTBEAT = "OpenCMW.heartBeat";
    public static final long HEARTBEAT_DEFAULT = 1000L;
    public static final String HEARTBEAT_LIVENESS = "OpenCMW.heartBeatLiveness";
    public static final int HEARTBEAT_LIVENESS_DEFAULT = 3;
    public static final String SUBSCRIPTION_TIMEOUT = "OpenCMW.subscriptionTimeOut";
    public static final long SUBSCRIPTION_TIMEOUT_DEFAULT = 1000L;
    public static final String N_IO_THREADS = "OpenCMW.nIoThreads";
    public static final int N_IO_THREADS_DEFAULT = 1; //
    public static final String CLIENT_TIMEOUT = "OpenCMW.clientTimeOut"; // [s]
    public static final long CLIENT_TIMEOUT_DEFAULT = 0L; // [s]
}
