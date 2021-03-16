package io.opencmw.server.rest.samples;

import static io.opencmw.OpenCmwProtocol.EMPTY_URI;

import java.io.IOException;
import java.net.URI;
import java.util.Objects;

import org.zeromq.ZContext;

import io.opencmw.domain.NoData;
import io.opencmw.rbac.BasicRbacRole;
import io.opencmw.rbac.RbacRole;
import io.opencmw.serialiser.annotations.MetaInfo;
import io.opencmw.server.MajordomoBroker;
import io.opencmw.server.MajordomoWorker;
import io.opencmw.server.rest.MajordomoRestPlugin;

import zmq.util.Utils;

public class BasicSample {
    public static void main(String[] argv) throws IOException {
        final MajordomoBroker broker = new MajordomoBroker("PrimaryBroker", EMPTY_URI, BasicRbacRole.values());
        final URI brokerRouterAddress = broker.bind(URI.create("mdp://*:" + Utils.findOpenPort()));
        final URI brokerSubscriptionAddress = broker.bind(URI.create("mds://*:" + Utils.findOpenPort()));
        broker.start();
        new MajordomoRestPlugin(broker.getContext(), "Test HTTP/REST Server", "*:8080").start();
        // instantiating and starting custom user-service
        new HelloWorldWorker(broker.getContext(), "helloWorld", BasicRbacRole.ANYONE).start();
    }

    @MetaInfo(description = "My first 'Hello World!' Service")
    public static class HelloWorldWorker extends MajordomoWorker<BasicRequestCtx, NoData, ReplyData> {
        public HelloWorldWorker(final ZContext ctx, final String serviceName, final RbacRole<?>... rbacRoles) {
            super(ctx, serviceName, BasicRequestCtx.class, NoData.class, ReplyData.class, rbacRoles);

            // the custom used code:
            this.setHandler((rawCtx, requestContext, requestData, replyContext, replyData) -> {
                final String name = Objects.requireNonNullElse(requestContext.name, "");
                replyData.returnValue = name.isBlank() ? "Hello World" : "Hello, " + name + "!";
                replyContext.name = name.isBlank() ? "At" : (name + ", at") + " your service!";
            });
        }
    }

    @MetaInfo(description = "arbitrary request domain contex object", direction = "IN")
    public static class BasicRequestCtx {
        @MetaInfo(description = " optional 'name' OpenAPI documentation")
        public String name;
    }

    @MetaInfo(description = "arbitrary reply domain object", direction = "OUT")
    public static class ReplyData {
        @MetaInfo(description = " optional 'returnValue' OpenAPI documentation", unit = "a string")
        public String returnValue;
    }
}
