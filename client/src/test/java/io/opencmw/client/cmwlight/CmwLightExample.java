package io.opencmw.client.cmwlight;

import io.opencmw.serialiser.IoClassSerialiser;
import io.opencmw.serialiser.spi.CmwLightSerialiser;
import io.opencmw.serialiser.spi.FastByteBuffer;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public class CmwLightExample { // NOPMD is not a utility class but a sample
    private final static String CMW_NAMESERVER = "cmwpro00a.acc.gsi.de:5021";
    private final static String DEVICE = "GSCD002";
    // private final static String PROPERTY = "SnoopTriggerEvents";
    private final static String PROPERTY = "AcquisitionDAQ";
    private final static String SELECTOR = "FAIR.SELECTOR.ALL";

    public static void main(String[] args) throws DirectoryLightClient.DirectoryClientException, CmwLightProtocol.RdaLightException {
        subscribeAcqFromDigitizer();
    }

    public static void subscribeAcqFromDigitizer() throws DirectoryLightClient.DirectoryClientException, CmwLightProtocol.RdaLightException {
        final DirectoryLightClient directoryClient = new DirectoryLightClient(CMW_NAMESERVER);
        DirectoryLightClient.Device device = directoryClient.getDeviceInfo(Collections.singletonList(DEVICE)).get(0);
        System.out.println(device);
        final String address = device.servers.stream().findFirst().orElseThrow().get("Address:");
        System.out.println("connect client to " + address);
        final CmwLightClient client = new CmwLightClient(new ZContext(1), address, Duration.ofMillis(100), "testclient", null);
        final ZMQ.Poller poller = client.getContext().createPoller(1);
        poller.register(client.getSocket(), ZMQ.Poller.POLLIN);
        client.connect();

        System.out.println("starting subscription");
        // 4 = Triggered Acquisition Mode; 0 = Continuous Acquisition mode
        Map<String, Object> filters = Map.of("acquisitionModeFilter", 0, "channelNameFilter", "GS11MU2:Current_1@10Hz");
        Map<String, Object> filters2 = Map.of("acquisitionModeFilter", 0, "channelNameFilter", "GS11MU2:Voltage_1@10Hz");
        final CmwLightClient.Subscription subscription = new CmwLightClient.Subscription(DEVICE, PROPERTY, SELECTOR, filters);
        client.subscribe(subscription);
        final CmwLightClient.Subscription subscription2 = new CmwLightClient.Subscription(DEVICE, PROPERTY, SELECTOR, filters2);
        client.subscribe(subscription2);
        client.subscriptions.forEach((id, c) -> System.out.println(id + " -> " + c));

        int i = 0;
        while (i < 45) {
            client.housekeeping();
            poller.poll();
            final CmwLightMessage result = client.receiveData();
            if (result != null && result.requestType == CmwLightProtocol.RequestType.NOTIFICATION_DATA) {
                System.out.println(result);
                final byte[] bytes = result.bodyData.getData();
                final IoClassSerialiser classSerialiser = new IoClassSerialiser(FastByteBuffer.wrap(bytes), CmwLightSerialiser.class);
                final AcquisitionDAQ acq = classSerialiser.deserialiseObject(AcquisitionDAQ.class);
                System.out.println("body: " + acq);
            } else {
                if (result != null)
                    System.out.println(result);
            }
            if (i == 15) {
                client.subscriptions.forEach((id, c) -> System.out.println(id + " -> " + c));
                System.out.println("unsubscribe");
                client.unsubscribe(subscription);
                client.unsubscribe(subscription2);
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
            return "AcquisitionDAQ{"
                    + "refTriggerName='" + refTriggerName + '\'' + ", refTriggerStamp=" + refTriggerStamp + ", channelTimeSinceRefTrigger(n=" + channelTimeSinceRefTrigger.length + ")=" + Arrays.toString(Arrays.copyOfRange(channelTimeSinceRefTrigger, 0, 3)) + ", channelUserDelay=" + channelUserDelay + ", channelActualDelay=" + channelActualDelay + ", channelName='" + channelName + '\'' + ", channelValue(n=" + channelValue.length + ")=" + Arrays.toString(Arrays.copyOfRange(channelValue, 0, 3)) + ", channelError(n=" + channelError.length + ")=" + Arrays.toString(Arrays.copyOfRange(channelError, 0, 3)) + ", channelUnit='" + channelUnit + '\'' + ", status=" + status + ", channelRangeMin=" + channelRangeMin + ", channelRangeMax=" + channelRangeMax + ", temperature=" + temperature + ", processIndex=" + processIndex + ", sequenceIndex=" + sequenceIndex + ", chainIndex=" + chainIndex + ", eventNumber=" + eventNumber + ", timingGroupId=" + timingGroupId + ", acquisitionStamp=" + acquisitionStamp + ", eventStamp=" + eventStamp + ", processStartStamp=" + processStartStamp + ", sequenceStartStamp=" + sequenceStartStamp + ", chainStartStamp=" + chainStartStamp + '}';
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
            return "SnoopAcquisition{"
                    + "TriggerEventName='" + TriggerEventName + '\'' + ", acquisitionStamp=" + acquisitionStamp + ", chainIndex=" + chainIndex + ", chainStartStamp=" + chainStartStamp + ", eventNumber=" + eventNumber + ", eventStamp=" + eventStamp + ", processIndex=" + processIndex + ", processStartStamp=" + processStartStamp + ", sequenceIndex=" + sequenceIndex + ", sequenceStartStamp=" + sequenceStartStamp + ", timingGroupID=" + timingGroupID + '}';
        }
    }
}
