package io.opencmw.server.rest;

import static io.opencmw.OpenCmwProtocol.MdpMessage;

import java.util.concurrent.atomic.AtomicLong;

import javax.validation.constraints.NotNull;

import io.opencmw.server.MajordomoWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;

import io.opencmw.rbac.RbacRole;

/**
 * Majordomo Broker REST/HTTP plugin.
 * This opens an http port and converts and forwards incoming request to the OpenCMW protocol and provides s
 * @author rstein
 */
public class MajordomoRestPlugin extends MajordomoWorker {
    private static final Logger LOGGER = LoggerFactory.getLogger(MajordomoRestPlugin.class);
    private static final AtomicLong REQUEST_COUNTER = new AtomicLong();
    private static final byte[] RBAC = new byte[] {}; // TODO: implement RBAC between Majordomo and Worker
    private final String httpAddress;

    public MajordomoRestPlugin(ZContext ctx, String httpAddress,
            final RbacRole<?>... rbacRoles) {
        super(ctx, MajordomoRestPlugin.class.getSimpleName(), rbacRoles);
        assert (ctx != null);
        assert (httpAddress != null);
        this.httpAddress = httpAddress;
    }

    public void notify(@NotNull final MdpMessage notifyMessage) {
        assert notifyMessage != null : "notify message must not be null";
        notifyRaw(notifyMessage);
    }
}
