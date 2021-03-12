package io.opencmw.client.cmwlight;

import static org.junit.jupiter.api.Assertions.fail;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import io.opencmw.client.DataSource;
import io.opencmw.client.DataSourcePublisher;
import io.opencmw.client.cmwlight.CmwLightExample.AcquisitionDAQ;
import io.opencmw.domain.NoData;

public class CmwLightViaPublisherExample {
    private final static String DEVICE = "GSCD002";
    // private final static String PROPERTY = "SnoopTriggerEvents";
    private final static String PROPERTY = "AcquisitionDAQ";
    private final static String SELECTOR = "FAIR.SELECTOR.ALL";

    public static void main(String[] args) throws URISyntaxException {
        if (args.length == 0) {
            System.out.println("no directory server supplied");
            return;
        }

        String filtersString = "acquisitionModeFilter=int:0&channelNameFilter=GS11MU2:Current_1@10Hz";
        final URI subURI = new URI("rda3", null, '/' + DEVICE + '/' + PROPERTY, "ctx=" + SELECTOR + "&" + filtersString, null);

        try (DataSourcePublisher dataSource = new DataSourcePublisher(null, null, "test-client");
                DataSourcePublisher.Client client = dataSource.getClient()) {
            // set DNS reference
            DataSource.getFactory(URI.create("rda3:/")).registerDnsResolver(new DirectoryLightClient(args[0]));

            AtomicInteger notificationCounter = new AtomicInteger();
            client.subscribe(subURI, AcquisitionDAQ.class, null, NoData.class, new DataSourcePublisher.NotificationListener<>() {
                @Override
                public void dataUpdate(final AcquisitionDAQ updatedObject, final NoData contextObject) {
                    System.out.println(notificationCounter.get() + ": notified property with " + updatedObject);
                    notificationCounter.getAndIncrement();
                }

                @Override
                public void updateException(final Throwable exception) {
                    fail("subscription exception occurred ", exception);
                }
            });
            System.out.println("start monitoring");
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(10));
            System.out.println("counted notification: " + notificationCounter.get());
        }
    }
}
