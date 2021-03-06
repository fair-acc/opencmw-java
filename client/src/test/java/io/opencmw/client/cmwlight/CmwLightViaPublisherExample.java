package io.opencmw.client.cmwlight;

import static io.opencmw.client.cmwlight.CmwLightExample.*;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import io.opencmw.client.DataSource;
import io.opencmw.client.DataSourcePublisher;
import io.opencmw.client.cmwlight.CmwLightExample.AcquisitionDAQ;
import io.opencmw.domain.NoData;

/**
 * Create a DataSourcePublisher and demonstrate subscription with with the listener interface and get with the future
 * interface. Also demonstrates error handling.
 * For the same example using the event store api, see {@link CmwLightEventStoreSample}
 */
public class CmwLightViaPublisherExample {
    private final static String DEVICE = "GSCD002";
    private final static String PROPERTY = "AcquisitionDAQ";
    private static final String CONFIG_PROPERTY = "ChannelConfigDAQ";
    private final static String SELECTOR = "FAIR.SELECTOR.ALL";

    public static void main(String[] args) throws URISyntaxException {
        if (args.length == 0) {
            System.out.println("no directory server supplied");
            return;
        }

        final URI getURIWithError = new URI("rda3", null, '/' + DEVICE + '/' + "NonexistentProperty", null, null);
        final URI getURI = new URI("rda3", null, '/' + DEVICE + '/' + CONFIG_PROPERTY, null, null);
        String filtersStringWithError = "acquisitionModeFilter=int:0&channelNameFilter=nonexistentChannel";
        final URI subURIWithError = new URI("rda3", null, '/' + DEVICE + '/' + PROPERTY, "ctx=" + SELECTOR + "&" + filtersStringWithError, null);
        String filtersString = "acquisitionModeFilter=int:0&channelNameFilter=GS11MU2:Current_1@10Hz";
        final URI subURI = new URI("rda3", null, '/' + DEVICE + '/' + PROPERTY, "ctx=" + SELECTOR + "&" + filtersString, null);

        try (final DataSourcePublisher dataSourcePublisher = new DataSourcePublisher(null, null, "test-client");
                final DataSourcePublisher.Client client = dataSourcePublisher.getClient()) {
            // set DNS reference
            DataSource.getFactory(URI.create("rda3:/")).registerDnsResolver(new DirectoryLightClient(args[0]));

            // subscription with illegal properties to test exception handling
            System.out.println("Subscription with error");
            final AtomicInteger errorNotificationCounter = new AtomicInteger();
            final String sub = client.subscribe(subURIWithError, AcquisitionDAQ.class, null, NoData.class, new DataSourcePublisher.NotificationListener<>() {
                @Override
                public void dataUpdate(final AcquisitionDAQ updatedObject, final NoData contextObject) {
                    System.out.println(errorNotificationCounter.getAndIncrement() + ": notified property with " + updatedObject);
                }

                @Override
                public void updateException(final Throwable exception) {
                    System.out.println(errorNotificationCounter.getAndIncrement() + ": notification exception: " + exception);
                }
            });
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(2));
            client.unsubscribe(sub); // unsubscribe invalid subscription

            // subscription
            System.out.println("Correct subscription");
            final AtomicInteger notificationCounter = new AtomicInteger();
            final String sub2 = client.subscribe(subURI, AcquisitionDAQ.class, null, NoData.class, new DataSourcePublisher.NotificationListener<>() {
                @Override
                public void dataUpdate(final AcquisitionDAQ updatedObject, final NoData contextObject) {
                    System.out.println(notificationCounter.getAndIncrement() + ": notified property (unexpected, should be exception) with " + updatedObject);
                }

                @Override
                public void updateException(final Throwable exception) {
                    System.out.println(notificationCounter.getAndIncrement() + ": notification exception (expected): " + exception);
                }
            });
            System.out.println("start monitoring");
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(5));
            System.out.println("counted notification: " + notificationCounter.get());
            client.unsubscribe(sub2);

            // get request with error
            System.out.println("Get request with error");
            final Future<ChannelConfig> futureError = client.get(getURIWithError, null, ChannelConfig.class);
            try {
                final ChannelConfig result = futureError.get(1, TimeUnit.SECONDS);
                System.out.println("Result of get request: " + result);
            } catch (Exception e) {
                System.out.println("Error during get request: " + e);
            }
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));

            // get request with error
            System.out.println("Get request");
            final Future<ChannelConfig> futureResult = client.get(getURI, null, ChannelConfig.class);
            try {
                final ChannelConfig result = futureResult.get(1, TimeUnit.SECONDS);
                System.out.println("Result of get request (unexpected, should have thrown exception): " + result);
            } catch (Exception e) {
                System.out.println("Error during get request (expected): " + e);
            }

            dataSourcePublisher.stop();
            dataSourcePublisher.getContext().destroy();
        }
    }
}
