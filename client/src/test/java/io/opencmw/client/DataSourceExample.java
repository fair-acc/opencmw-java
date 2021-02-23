package io.opencmw.client;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opencmw.EventStore;
import io.opencmw.client.cmwlight.CmwLightExample;
import io.opencmw.client.cmwlight.DirectoryLightClient;
import io.opencmw.filter.EvtTypeFilter;
import io.opencmw.filter.TimingCtx;

public class DataSourceExample {
    private final static Logger LOGGER = LoggerFactory.getLogger(DataSourceExample.class);
    private final static String DEV_NAME = "GSCD002";
    private final static String DEV2_NAME = "GSCD001";
    private final static String PROP = "AcquisitionDAQ";
    private final static String SELECTOR = "FAIR.SELECTOR.ALL";

    public static void main(String[] args) throws DirectoryLightClient.DirectoryClientException, URISyntaxException {
        // create and start a simple event store which just prints everything written to it to stdout
        final EventStore eventStore = EventStore.getFactory().setFilterConfig(TimingCtx.class, EvtTypeFilter.class).build();
        eventStore.register((e, s, last) -> {
            System.out.println(e);
            System.out.println(e.payload.get());
        });
        eventStore.start();
        // create a data source publisher and add a subscription
        final DataSourcePublisher dataSourcePublisher = new DataSourcePublisher(eventStore, null, null, "testSubPublisher");
        if (args.length == 0) {
            LOGGER.atError().log("no directory server supplied");
            return;
        }
        LOGGER.atInfo().addArgument(args[0]).log("directory server: {}");
        final DirectoryLightClient directoryLightClient = new DirectoryLightClient(args[0]);
        final List<DirectoryLightClient.Device> devInfo = directoryLightClient.getDeviceInfo(List.of(DEV_NAME, DEV2_NAME));
        final String address = devInfo.get(0).getAddress().substring("tcp://".length());
        final String address2 = devInfo.get(1).getAddress().substring("tcp://".length());
        // run the publisher's main loop
        new Thread(dataSourcePublisher).start();
        try (final DataSourcePublisher.Client client = dataSourcePublisher.getClient()) {
            client.subscribe(new URI("rda3", address, '/' + DEV_NAME + '/' + PROP, "?ctx=" + SELECTOR + '&' + "acquisitionModeFilter=int:0" + '&' + "channelNameFilter=GS11MU2:Current_1@10Hz", null), CmwLightExample.AcquisitionDAQ.class);
            client.subscribe(new URI("rda3", address2, '/' + DEV2_NAME + '/' + PROP, "?ctx=FAIR.SELECTOR.ALL" + '&' + "acquisitionModeFilter=int:4&channelNameFilter=GS02P:SumY:Triggered@25MHz", null), CmwLightExample.AcquisitionDAQ.class);
        }
    }
}
