package io.opencmw.client.cmwlight;

import static io.opencmw.client.cmwlight.CmwLightExample.ChannelConfig;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

import io.opencmw.EventStore;
import io.opencmw.client.DataSource;
import io.opencmw.client.DataSourcePublisher;
import io.opencmw.client.cmwlight.CmwLightExample.AcquisitionDAQ;
import io.opencmw.domain.NoData;
import io.opencmw.filter.EvtTypeFilter;
import io.opencmw.filter.TimingCtx;

/**
 * Create a DataSourcePublisher and demonstrate subscription with with the event store interface. Also demonstrates error handling.
 * For the same example using the listener/future api, see {@link CmwLightViaPublisherExample}
 */
public class CmwLightEventStoreSample {
    private final static String DEVICE = "GSCD002";
    private final static String PROPERTY = "AcquisitionDAQ";
    private static final String CONFIG_PROPERTY = "ChannelConfigDAQ";
    private final static String SELECTOR = "FAIR.SELECTOR.ALL";

    public static void main(String[] args) throws URISyntaxException {
        if (args.length == 0) {
            System.out.println("no directory server supplied");
            return;
        }
        // setup event store and listeners
        final EventStore eventStore = EventStore.getFactory().setFilterConfig(TimingCtx.class, EvtTypeFilter.class).build();
        AtomicInteger eventStoreCounter = new AtomicInteger(0);
        eventStore.register((ev, id, isLast) -> {
            final int n = eventStoreCounter.getAndIncrement();
            if (ev.throwables.size() == 0) {
                System.out.println(n + ": store event with payload " + ev.payload.get());
            } else {
                System.out.println(n + ": store event with exceptions\n" + ev.throwables.stream().map(Throwable::toString).collect(Collectors.joining(", ", "[", "]")));
            }
        });
        eventStore.start();

        final URI getURIWithError = new URI("rda3", null, '/' + DEVICE + '/' + "NonexistentProperty", null, null);
        final URI getURI = new URI("rda3", null, '/' + DEVICE + '/' + CONFIG_PROPERTY, null, null);
        String filtersStringWithError = "acquisitionModeFilter=int:0&channelNameFilter=nonexistentChannel";
        final URI subURIWithError = new URI("rda3", null, '/' + DEVICE + '/' + PROPERTY, "ctx=" + SELECTOR + "&" + filtersStringWithError, null);
        String filtersString = "acquisitionModeFilter=int:0&channelNameFilter=GS11MU2:Current_1@10Hz";
        final URI subURI = new URI("rda3", null, '/' + DEVICE + '/' + PROPERTY, "ctx=" + SELECTOR + "&" + filtersString, null);

        try (final DataSourcePublisher dataSourcePublisher = new DataSourcePublisher(null, eventStore, null, null, "test-client");
                final DataSourcePublisher.Client client = dataSourcePublisher.getClient()) {
            dataSourcePublisher.start();

            // set DNS reference
            DataSource.getFactory(URI.create("rda3:/")).registerDnsResolver(new DirectoryLightClient(args[0]));

            // subscription with illegal properties to test exception handling
            System.out.println("Subscription with error");
            final AtomicInteger errorNotificationCounter = new AtomicInteger();
            final String sub = client.subscribe(subURIWithError, AcquisitionDAQ.class, null, NoData.class, null);
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(2));
            client.unsubscribe(sub); // unsubscribe invalid subscription

            // subscription
            System.out.println("Correct subscription");
            final AtomicInteger notificationCounter = new AtomicInteger();
            final String sub2 = client.subscribe(subURI, AcquisitionDAQ.class, null, NoData.class, null);
            System.out.println("start monitoring");
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(5));
            System.out.println("counted notification: " + notificationCounter.get());
            client.unsubscribe(sub2);

            // get request with error
            System.out.println("Get request with error");
            client.get(getURIWithError, null, ChannelConfig.class);
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));

            // get request with error
            System.out.println("Get request");
            final Future<ChannelConfig> futureResult = client.get(getURI, null, ChannelConfig.class);
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));

            dataSourcePublisher.stop();
            dataSourcePublisher.getContext().destroy();
        }
        eventStore.stop();
    }
}
