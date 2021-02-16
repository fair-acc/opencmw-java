package io.opencmw.filter;

import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Predicate;

import org.apache.commons.lang3.StringUtils;

import io.opencmw.Filter;

import com.jsoniter.spi.JsoniterSpi;

@SuppressWarnings({ "PMD.TooManyMethods" }) // - the nature of this class definition
public class TimingCtx implements Filter {
    public static final String WILD_CARD = "ALL";
    public static final int WILD_CARD_VALUE = -1;
    public static final String SELECTOR_PREFIX = "FAIR.SELECTOR.";
    /** selector string, e.g.: 'FAIR.SELECTOR.C=0:S=1:P=3:T=101' */
    public String selector = "";
    /** Beam-Production-Chain (BPC) ID - uninitialised/wildcard value = -1 */
    public int cid;
    /** Sequence ID -- N.B. this is the timing sequence number not the disruptor sequence ID */
    public int sid;
    /** Beam-Process ID (PID) - uninitialised/wildcard value = -1 */
    public int pid;
    /** timing group ID - uninitialised/wildcard value = -1 */
    public int gid;
    /** Beam-Production-Chain-Time-Stamp - UTC in [us] since 1.1.1980 */
    public long bpcts;
    /** stores the settings-supply related ctx name */
    public String ctxName;
    protected int hashCode = 0; // NOPMD cached hash code
    static {
        // custom JsonIter decoder
        JsoniterSpi.registerTypeDecoder(TimingCtx.class, iter -> TimingCtx.get(iter.readString()));
    }
    public TimingCtx() {
        clear(); // NOPMD -- called during initialisation
    }

    @Override
    public void clear() {
        hashCode = 0;
        selector = "";
        cid = -1;
        sid = -1;
        pid = -1;
        gid = -1;
        bpcts = -1;
        ctxName = "";
    }

    @Override
    public void copyTo(final Filter other) {
        if (!(other instanceof TimingCtx)) {
            return;
        }
        final TimingCtx otherCtx = (TimingCtx) other;
        otherCtx.selector = this.selector;
        otherCtx.cid = this.cid;
        otherCtx.sid = this.sid;
        otherCtx.pid = this.pid;
        otherCtx.gid = this.gid;
        otherCtx.bpcts = this.bpcts;
        otherCtx.ctxName = this.ctxName;
        otherCtx.hashCode = this.hashCode;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final TimingCtx otherCtx = (TimingCtx) o;
        if (hashCode != otherCtx.hashCode() || cid != otherCtx.cid || sid != otherCtx.sid || pid != otherCtx.pid || gid != otherCtx.gid || bpcts != otherCtx.bpcts) {
            return false;
        }

        return Objects.equals(selector, otherCtx.selector);
    }

    @Override
    public int hashCode() {
        if (hashCode != 0) {
            return hashCode;
        }
        hashCode = selector == null ? 0 : selector.hashCode();
        hashCode = 31 * hashCode + cid;
        hashCode = 31 * hashCode + sid;
        hashCode = 31 * hashCode + pid;
        hashCode = 31 * hashCode + gid;
        hashCode = 31 * hashCode + Long.hashCode(bpcts);
        return hashCode;
    }

    public Predicate<TimingCtx> matches(final TimingCtx other) {
        return t -> this.equals(other);
    }

    /**
     * TODO: add more thorough documentation or reference
     *
     * @param selector new selector to be parsed, e.g. 'FAIR.SELECTOR.ALL', 'FAIR.SELECTOR.C=1:S=3:P:3:T:103'
     * @param bpcts beam-production-chain time-stamp [us]
     */
    @SuppressWarnings("PMD.NPathComplexity") // -- parser/format has intrinsically large number of possible combinations
    public void setSelector(final String selector, final long bpcts) {
        if (bpcts < 0) {
            throw new IllegalArgumentException("BPCTS time stamp < 0 :" + bpcts);
        }
        try {
            clear();
            this.selector = Objects.requireNonNull(selector, "selector string must not be null");
            this.bpcts = bpcts;

            final String selectorUpper = selector.toUpperCase(Locale.UK);
            if (selector.isBlank() || WILD_CARD.equals(selectorUpper)) {
                return;
            }

            final String[] identifiers = StringUtils.replace(selectorUpper, SELECTOR_PREFIX, "", 1).split(":");
            if (identifiers.length == 1 && WILD_CARD.equals(identifiers[0])) {
                return;
            }

            for (String tag : identifiers) {
                final String[] splitSubComponent = tag.split("=");
                assert splitSubComponent.length == 2 : "invalid selector: " + selector; // NOPMD NOSONAR assert only while debugging
                final int value = splitSubComponent[1].equals(WILD_CARD) ? -1 : Integer.parseInt(splitSubComponent[1]);
                switch (splitSubComponent[0]) {
                case "C":
                    this.cid = value;
                    break;
                case "S":
                    this.sid = value;
                    break;
                case "P":
                    this.pid = value;
                    break;
                case "T":
                    this.gid = value;
                    break;
                default:
                    clear();
                    throw new IllegalArgumentException("cannot parse selector: '" + selector + "' sub-tag: " + tag);
                }
            }
        } catch (Throwable t) { // NOPMD NOSONAR should catch Throwable
            clear();
            throw new IllegalArgumentException("Invalid selector or bpcts: " + selector, t);
        }
    }

    public static TimingCtx get(final String ctxString) {
        final TimingCtx ctx = new TimingCtx();
        ctx.setSelector(ctxString, 0);
        return ctx;
    }

    @Override
    public String toString() {
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX", Locale.UK);
        return '[' + TimingCtx.class.getSimpleName() + ": bpcts=" + bpcts + " (\"" + sdf.format(bpcts / 1_000_000) + "\"), selector='" + selector + "', cid=" + cid + ", sid=" + sid + ", pid=" + pid + ", gid=" + gid + ']';
    }

    public static Predicate<TimingCtx> matches(final int cid, final int sid, final int pid, final long bpcts) {
        return t -> t.bpcts == bpcts && t.cid == cid && wildCardMatch(t.sid, sid) && wildCardMatch(t.pid, pid);
    }

    public static Predicate<TimingCtx> matches(final int cid, final int sid, final long bpcts) {
        return t -> t.bpcts == bpcts && wildCardMatch(t.cid, cid) && wildCardMatch(t.sid, sid);
    }

    public static Predicate<TimingCtx> matches(final int cid, final long bpcts) {
        return t -> t.bpcts == bpcts && wildCardMatch(t.cid, cid);
    }

    public static Predicate<TimingCtx> matches(final int cid, final int sid, final int pid) {
        return t -> wildCardMatch(t.cid, cid) && wildCardMatch(t.sid, sid) && wildCardMatch(t.pid, pid);
    }

    public static Predicate<TimingCtx> matches(final int cid, final int sid) {
        return t -> wildCardMatch(t.cid, cid) && wildCardMatch(t.sid, sid);
    }

    public static Predicate<TimingCtx> matchesBpcts(final long bpcts) {
        return t -> t.bpcts == bpcts;
    }

    public static Predicate<TimingCtx> isOlderBpcts(final long bpcts) {
        return t -> t.bpcts < bpcts;
    }

    public static Predicate<TimingCtx> isNewerBpcts(final long bpcts) {
        return t -> t.bpcts > bpcts;
    }

    protected static boolean wildCardMatch(final int a, final int b) {
        return a == b || a == WILD_CARD_VALUE || b == WILD_CARD_VALUE;
    }
}
