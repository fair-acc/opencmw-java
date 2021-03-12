package io.opencmw.client.cmwlight;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import io.opencmw.serialiser.IoClassSerialiser;
import io.opencmw.serialiser.spi.CmwLightSerialiser;
import io.opencmw.serialiser.spi.FastByteBuffer;

/**
 * Small sample to test subscription using the CmwLightClient without the DataSourcePublisher.
 * Most of the times this is not what you want to use, see DataSourceExample instead.
 * The program needs the location of a cmw nameserver (host:port) as a command line argument.
 */
public class CmwLightExample { // NOPMD is not a utility class but a sample
    private final static String DEVICE = "GSCD002";
    // private final static String PROPERTY = "SnoopTriggerEvents";
    private final static String PROPERTY = "AcquisitionDAQ";
    private final static String SELECTOR = "FAIR.SELECTOR.ALL";

    public static void main(String[] args) throws DirectoryLightClient.DirectoryClientException, URISyntaxException, UnknownHostException {
        if (args.length == 0) {
            System.out.println("no directory server supplied");
            return;
        }
        subscribeAcqFromDigitizer(args[0]);
    }

    public static void subscribeAcqFromDigitizer(final String nameserver) throws DirectoryLightClient.DirectoryClientException, URISyntaxException, UnknownHostException {
        // mini DirectoryLightClient tests
        final DirectoryLightClient directoryClient = new DirectoryLightClient(nameserver);
        final URI queryDevice = URI.create("rda3:/" + DEVICE);
        System.out.println("resolve name " + queryDevice + " to: " + directoryClient.resolveNames(List.of(queryDevice)));
        DirectoryLightClient.Device device = directoryClient.getDeviceInfo(Collections.singletonList(DEVICE)).get(0);
        System.out.println(device);
        final String address = device.servers.stream().findFirst().orElseThrow().get("Address:").replace("tcp://", "rda3://");
        System.out.println("connect client to " + address);
        // mini DirectoryLightClient tests -- done

        // without DNS resolver:
        // final CmwLightDataSource client = new CmwLightDataSource(new ZContext(1), URI.create(address + '/' + DEVICE), Executors.newCachedThreadPool(), "testclient")
        // with DNS resolver:
        final CmwLightDataSource client = new CmwLightDataSource(new ZContext(1), URI.create("rda3:/" + DEVICE), Executors.newCachedThreadPool(), "testclient");
        client.getFactory().registerDnsResolver(new DirectoryLightClient(nameserver)); // direct DNS registration - can be done also via DefaultDataSource

        final ZMQ.Poller poller = client.getContext().createPoller(1);
        poller.register(client.getSocket(), ZMQ.Poller.POLLIN);
        client.connect();

        System.out.println("starting subscription");
        // 4 = Triggered Acquisition Mode; 0 = Continuous Acquisition mode
        String filtersString = "acquisitionModeFilter=int:0&channelNameFilter=GS11MU2:Current_1@10Hz";
        String filters2String = "acquisitionModeFilter=int:0&channelNameFilter=GS11MU2:Voltage_1@10Hz";
        client.subscribe("r1", new URI("rda3", null, '/' + DEVICE + '/' + PROPERTY, "ctx=" + SELECTOR + "&" + filtersString, null), null);
        client.subscribe("r2", new URI("rda3", null, '/' + DEVICE + '/' + PROPERTY, "ctx=" + SELECTOR + "&" + filters2String, null), null);
        client.subscriptions.forEach((id, c) -> System.out.println(id + " -> " + c));

        int i = 0;
        while (i < 45) {
            client.housekeeping();
            poller.poll(1000);
            final ZMsg result = client.getMessage();
            if (result != null && result.size() == 4) {
                final String reqId = Objects.requireNonNull(result.poll()).getString(Charset.defaultCharset());
                final String endpoint = Objects.requireNonNull(result.poll()).getString(Charset.defaultCharset());
                System.out.println("Update (" + reqId + ") for endpoint: " + endpoint);
                final byte[] bytes = Objects.requireNonNull(result.poll()).getData();
                final String exc = Objects.requireNonNull(result.poll()).getString(Charset.defaultCharset());
                if (!exc.isBlank()) {
                    System.out.println("exception: " + exc);
                } else {
                    final IoClassSerialiser classSerialiser = new IoClassSerialiser(FastByteBuffer.wrap(bytes), CmwLightSerialiser.class);
                    final AcquisitionDAQ acq = classSerialiser.deserialiseObject(AcquisitionDAQ.class);
                    System.out.println("body: " + acq);
                }
            } else {
                if (result != null)
                    System.out.println(result);
            }
            if (i == 15) {
                client.subscriptions.forEach((id, c) -> System.out.println(id + " -> " + c));
                System.out.println("unsubscribe");
                client.unsubscribe("r1");
                client.unsubscribe("r2");
            }
            i++;
        }
    }

    public static class AcquisitionDAQ {
        public String refTriggerName;
        public long refTriggerStamp;
        public float[] channelTimeSinceRefTrigger;
        public float channelUserDelay;
        public float channelActualDelay;
        public String channelName;
        public float[] channelValue;
        public float[] channelError;
        public String channelUnit;
        public int status;
        public float channelRangeMin;
        public float channelRangeMax;
        public float temperature;
        public int processIndex;
        public int sequenceIndex;
        public int chainIndex;
        public int eventNumber;
        public int timingGroupId;
        public long acquisitionStamp;
        public long eventStamp;
        public long processStartStamp;
        public long sequenceStartStamp;
        public long chainStartStamp;

        @Override
        public String toString() {
            return "AcquisitionDAQ{refTriggerName='" + refTriggerName + '\'' + ", refTriggerStamp=" + refTriggerStamp + ", channelTimeSinceRefTrigger(n=" + channelTimeSinceRefTrigger.length + ")=" + Arrays.toString(Arrays.copyOfRange(channelTimeSinceRefTrigger, 0, 3)) + ", channelUserDelay=" + channelUserDelay + ", channelActualDelay=" + channelActualDelay + ", channelName='" + channelName + '\'' + ", channelValue(n=" + channelValue.length + ")=" + Arrays.toString(Arrays.copyOfRange(channelValue, 0, 3)) + ", channelError(n=" + channelError.length + ")=" + Arrays.toString(Arrays.copyOfRange(channelError, 0, 3)) + ", channelUnit='" + channelUnit + '\'' + ", status=" + status + ", channelRangeMin=" + channelRangeMin + ", channelRangeMax=" + channelRangeMax + ", temperature=" + temperature + ", processIndex=" + processIndex + ", sequenceIndex=" + sequenceIndex + ", chainIndex=" + chainIndex + ", eventNumber=" + eventNumber + ", timingGroupId=" + timingGroupId + ", acquisitionStamp=" + acquisitionStamp + ", eventStamp=" + eventStamp + ", processStartStamp=" + processStartStamp + ", sequenceStartStamp=" + sequenceStartStamp + ", chainStartStamp=" + chainStartStamp + '}';
        }
    }

    public static class SnoopAcquisition {
        public String TriggerEventName;
        public long acquisitionStamp;
        public int chainIndex;
        public long chainStartStamp;
        public int eventNumber;
        public long eventStamp;
        public int processIndex;
        public long processStartStamp;
        public int sequenceIndex;
        public long sequenceStartStamp;
        public int timingGroupID;

        @Override
        public String toString() {
            return "SnoopAcquisition{TriggerEventName='" + TriggerEventName + '\'' + ", acquisitionStamp=" + acquisitionStamp + ", chainIndex=" + chainIndex + ", chainStartStamp=" + chainStartStamp + ", eventNumber=" + eventNumber + ", eventStamp=" + eventStamp + ", processIndex=" + processIndex + ", processStartStamp=" + processStartStamp + ", sequenceIndex=" + sequenceIndex + ", sequenceStartStamp=" + sequenceStartStamp + ", timingGroupID=" + timingGroupID + '}';
        }
    }
}
