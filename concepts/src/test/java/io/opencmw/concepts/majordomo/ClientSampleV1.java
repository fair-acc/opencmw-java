package io.opencmw.concepts.majordomo;

import java.nio.charset.StandardCharsets;

import org.zeromq.ZMsg;

import io.opencmw.rbac.BasicRbacRole;
import io.opencmw.rbac.RbacToken;

/**
* Majordomo Protocol client example. Uses the mdcli API to hide all OpenCmwProtocol aspects
*/
public final class ClientSampleV1 { // nomen est omen
    private static final int N_SAMPLES = 50_000;

    private ClientSampleV1() {
        // requires only static methods for testing
    }

    public static void main(String[] args) {
        MajordomoClientV1 clientSession = new MajordomoClientV1("tcp://localhost:5555", "customClientName");
        final byte[] serviceBytes = "mmi.echo".getBytes(StandardCharsets.UTF_8);
        // final byte[] serviceBytes = "inproc.echo".getBytes(StandardCharsets.UTF_8)
        // final byte[] serviceBytes = "echo".getBytes(StandardCharsets.UTF_8)

        int count;
        long start = System.currentTimeMillis();
        for (count = 0; count < N_SAMPLES; count++) {
            final String requestMsg = "Hello world - sync - " + count;
            final byte[] request = requestMsg.getBytes(StandardCharsets.UTF_8);
            final byte[] rbacToken = new RbacToken(BasicRbacRole.ADMIN, "HASHCODE").getBytes(); // NOPMD
            final ZMsg reply = clientSession.send(serviceBytes, request, rbacToken); // with RBAC
            // final ZMsg reply = clientSession.send(serviceBytes, request); // w/o RBAC
            if (count < 10 || count % 10_000 == 0 || count >= (N_SAMPLES - 10)) {
                System.err.println("client iteration = " + count + " - received: " + reply);
            }
            if (reply == null) {
                break; // Interrupt or failure
            }
            reply.destroy();
        }

        long mark1 = System.currentTimeMillis();
        double diff2 = 1e-3 * (mark1 - start);
        System.err.printf("%d requests/replies processed in %d ms -> %f op/s\n", count, mark1 - start, count / diff2);
        clientSession.destroy();
    }
}
