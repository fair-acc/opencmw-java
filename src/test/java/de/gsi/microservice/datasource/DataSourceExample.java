package de.gsi.microservice.datasource;

import de.gsi.microservice.EventStore;
import de.gsi.microservice.datasource.cmwlight.CmwLightExample;
import de.gsi.microservice.datasource.cmwlight.DirectoryLightClient;
import de.gsi.microservice.filter.EvtTypeFilter;
import de.gsi.microservice.filter.TimingCtx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class DataSourceExample {
    private final static Logger LOGGER = LoggerFactory.getLogger(DataSourceExample.class);
    private final static String DEV_NAME = "GSCD002";
    private final static String DEV2_NAME = "GSCD001";
    private final static String PROP = "AcquisitionDAQ";
    private final static String SELECTOR = "FAIR.SELECTOR.ALL";

    public static void main(String[] args) throws DirectoryLightClient.DirectoryClientException, InterruptedException {
        // create and start a simple event store which just prints everything written to it to stdout
        final EventStore eventStore = EventStore.getFactory().setFilterConfig(TimingCtx.class, EvtTypeFilter.class).build();
        eventStore.register((e, s,last) -> {
            System.out.println(e);
            System.out.println(e.payload.get());
        });
        eventStore.start();
        // create a data source publisher and add a subscription
        final DataSourcePublisher dataSourcePublisher = new DataSourcePublisher(null, eventStore);
        if (args.length == 0) {
            LOGGER.atError().log("no directory server supplied");
            return;
        }
        LOGGER.atInfo().addArgument(args[0]).log("directory server: {}");
        final DirectoryLightClient directoryLightClient = new DirectoryLightClient(args[0]);
        final List<DirectoryLightClient.Device> devInfo = directoryLightClient.getDeviceInfo(List.of(DEV_NAME, DEV2_NAME));
        final String address = devInfo.get(0).getAddress().replace("tcp://", "rda3://");
        final String address2 = devInfo.get(1).getAddress().replace("tcp://", "rda3://");
        // run the publisher's main loop
        new Thread(dataSourcePublisher).start();
        dataSourcePublisher.subscribe(address + '/' + DEV_NAME + '/' + PROP + "?ctx=" + SELECTOR + '&' + "acquisitionModeFilter=int:0" +'&' + "channelNameFilter=GS11MU2:Current_1@10Hz", CmwLightExample.AcquisitionDAQ.class);
        dataSourcePublisher.subscribe(address2 + '/' + DEV2_NAME + '/' + PROP + "?ctx=FAIR.SELECTOR.ALL" + '&' + "acquisitionModeFilter=int:4&channelNameFilter=GS02P:SumY:Triggered@25MHz", CmwLightExample.AcquisitionDAQ.class);
    }
}
