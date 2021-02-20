package io.opencmw.server.rest.samples;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opencmw.rbac.BasicRbacRole;
import io.opencmw.server.MajordomoBroker;
import io.opencmw.server.rest.MajordomoRestPlugin;
import io.opencmw.server.rest.test.HelloWorldService;
import io.opencmw.server.rest.test.ImageService;

import zmq.util.Utils;

public class MajordomoRestPluginSample {
    private static final Logger LOGGER = LoggerFactory.getLogger(MajordomoRestPluginSample.class);
    public static MajordomoBroker primaryBroker;
    public static MajordomoRestPlugin restPlugin;

    public static void launchBroker() throws IOException {
        primaryBroker = new MajordomoBroker("PrimaryBroker", "", BasicRbacRole.values());
        final String brokerRouterAddress = primaryBroker.bind("tcp://*:" + Utils.findOpenPort());
        primaryBroker.bind("mds://*:" + Utils.findOpenPort());
        restPlugin = new MajordomoRestPlugin(primaryBroker.getContext(), "My test REST server", "*:8080", BasicRbacRole.ADMIN);
        primaryBroker.start();
        restPlugin.start();
        LOGGER.atInfo().log("Broker and REST plugin started");

        // second broker to test DNS functionalities
        MajordomoBroker secondaryBroker = new MajordomoBroker("SecondaryTestBroker", brokerRouterAddress, BasicRbacRole.values());
        secondaryBroker.bind("tcp://*:" + Utils.findOpenPort());
        secondaryBroker.start();
    }

    public static void main(String[] args) throws IOException {
        launchBroker();

        // start simple test services/properties
        final HelloWorldService helloWorldService = new HelloWorldService(primaryBroker.getContext());
        helloWorldService.start();
        final ImageService imageService = new ImageService(primaryBroker.getContext(), 2000);
        imageService.start();

        LOGGER.atInfo().log("added HelloWorldService");
    }
}
