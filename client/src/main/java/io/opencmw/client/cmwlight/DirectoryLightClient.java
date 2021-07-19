package io.opencmw.client.cmwlight;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.URLDecoder;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import io.opencmw.OpenCmwConstants;
import io.opencmw.client.DnsResolver;

/**
 * Obtain device info from the directory server
 */
public class DirectoryLightClient implements DnsResolver {
    public static final String GET_DEVICE_INFO = "get-device-info";
    // public static final String GET_SERVER_INFO = "get-server-info";
    // private static final String SUPPORTED_CHARACTERS = "\\.\\-\\+_a-zA-Z0-9";
    // private static final String NAME_REGEX = "[a-zA-Z0-9][" + SUPPORTED_CHARACTERS + "]*";
    // private static final String CLIENT_INFO_SUPPORTED_CHARACTERS = "\\x20-\\x7E"; // ASCII := {32-126}
    private static final String ERROR_STRING = "ERROR";
    private static final String HOST_PORT_SEPARATOR = ":";

    private static final String NOT_BOUND_LOCATION = "*NOT_BOUND*";
    // static final String UNKNOWN_SERVER = "*UNKNOWN*";
    private static final String CLIENT_INFO = "DirectoryLightClient";
    private static final String VERSION = "2.0.0";
    private final String nameserver;
    private final int nameserverPort;

    public DirectoryLightClient(final String... nameservers) {
        if (nameservers.length != 1) {
            throw new IllegalArgumentException("only one nameserver supported at the moment");
        }
        final String[] hostPort = nameservers[0].split(HOST_PORT_SEPARATOR);
        if (hostPort.length != 2) {
            throw new IllegalArgumentException("nameserver address has wrong format: " + nameservers[0]);
        }
        nameserver = hostPort[0];
        nameserverPort = Integer.parseInt(hostPort[1]);
    }

    @Override
    public void close() {
        // nothing to be closed here
    }

    @Override
    public List<String> getApplicableSchemes() {
        return List.of("rda3");
    }

    @Override
    public Map<URI, List<URI>> resolveNames(final List<URI> devicesToResolve) throws UnknownHostException {
        final List<String> deviceList = devicesToResolve.stream().map(OpenCmwConstants::getDeviceName).collect(Collectors.toList());
        try {
            final List<Device> deviceInfos = getDeviceInfo(deviceList);
            final Map<URI, List<URI>> map = new ConcurrentHashMap<>();
            for (Device device : deviceInfos) {
                map.put(URI.create("rda3:/" + device.name), List.of(URI.create(device.getAddress())));
            }
            return map;
        } catch (Exception e) { // NOPMD
            throw new UnknownHostException("resolveNames : " + e.getMessage()); // NOPMD - exception retained in message
        }
    }

    /**
     * Build the request message to query a number of devices
     *
     * @param devices The devices to query information for
     * @return The request message to send to the server
     **/
    private String getDeviceMsg(final List<String> devices) {
        final var sb = new StringBuilder();
        sb.append(GET_DEVICE_INFO).append("\n@client-info ").append(CLIENT_INFO).append("\n@version ").append(VERSION).append('\n');
        // msg.append("@prefer-proxy\n");
        // msg.append("@direct ").append(this.properties.directServers.getValue()).append("\n");
        // msg.append("@domain ");
        // for (Domain domain : domains) {
        //     msg.append(domain.getName());
        //     msg.append(",");
        // }
        // msg.deleteCharAt(msg.length()-1);
        // msg.append("\n");
        for (final String dev : devices) {
            sb.append(dev).append('\n');
        }
        sb.append('\n');
        return sb.toString();
    }

    // /**
    //  * Build the request message to query a number of servers
    //  *
    //  * @param servers The servers to query information for
    //  * @return The request message to send to the server
    //  **/
    // private String getServerMsg(final List<String> servers) {
    //     final StringBuilder sb = new StringBuilder();
    //     sb.append(GET_SERVER_INFO).append("\n");
    //     sb.append("@client-info ").append(CLIENT_INFO).append("\n");
    //     sb.append("@version ").append(VERSION).append("\n");
    //     // msg.append("@prefer-proxy\n");
    //     // msg.append("@direct ").append(this.properties.directServers.getValue()).append("\n");
    //     // msg.append("@domain ");
    //     // for (Domain domain : domains) {
    //     //     msg.append(domain.getName());
    //     //     msg.append(",");
    //     // }
    //     // msg.deleteCharAt(msg.length()-1);
    //     // msg.append("\n");
    //     for (final String dev : servers) {
    //         sb.append(dev).append('\n');
    //     }
    //     sb.append('\n');
    //     return sb.toString();
    // }

    /**
     * Query Server information for a given list of devices.
     *
     * @param devices The devices to query information for
     * @return a list of device information for the queried devices
     **/
    public List<Device> getDeviceInfo(final List<String> devices) throws DirectoryClientException {
        final ArrayList<Device> result = new ArrayList<>();
        try (var socket = new Socket()) {
            socket.connect(new InetSocketAddress(nameserver, nameserverPort));
            try (var writer = new PrintWriter(socket.getOutputStream(), true, StandardCharsets.UTF_8);
                    var bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))) {
                writer.write(getDeviceMsg(devices));
                writer.flush();
                // read query result, one line per requested device or ERROR followed by error message
                while (true) {
                    final String line = bufferedReader.readLine();
                    if (line == null) {
                        break;
                    }
                    if (line.equals(ERROR_STRING)) {
                        final String errorMsg = bufferedReader.lines().collect(Collectors.joining("\n")).strip();
                        throw new DirectoryClientException(errorMsg);
                    }
                    result.add(parseDeviceInfo(line));
                }
            }
        } catch (IOException e) {
            throw new DirectoryClientException("Nameserver error: ", e);
        }
        return result;
    }

    private Device parseDeviceInfo(final String line) throws DirectoryClientException {
        String[] tokens = line.split(" ");
        if (tokens.length < 2) {
            throw new DirectoryClientException("Malformed reply line: " + line);
        }
        if (tokens[1].equals(NOT_BOUND_LOCATION)) {
            throw new DirectoryClientException("Requested device not bound: " + tokens[0]);
        }
        final ArrayList<Map<String, String>> servers = new ArrayList<>();
        for (var j = 2; j < tokens.length; j++) {
            final HashMap<String, String> server = new HashMap<>(); // NOPMD - necessary to allocate inside loop
            servers.add(server);
            final String[] servertokens = tokens[j].split("#");
            server.put("protocol", servertokens[0]);
            var k = 1;
            while (k + 3 < servertokens.length) {
                if ("string".equals(servertokens[k + 1])) {
                    final var length = Integer.parseInt(servertokens[k + 2]);
                    final String value = URLDecoder.decode(servertokens[k + 3], Charset.defaultCharset());
                    if (length == value.length()) {
                        server.put(servertokens[k], value);
                    } else {
                        throw new DirectoryClientException("Error parsing string: " + servertokens[k] + "(" + length + ") = " + value);
                    }
                    k += 4;
                } else if ("int".equals(servertokens[k + 1]) || "long".equals(servertokens[k + 1])) { // NOPMD
                    server.put(servertokens[k], servertokens[k + 2]);
                    k += 3;
                } else {
                    throw new DirectoryClientException("Error parsing argument: " + k + ": " + Arrays.toString(servertokens));
                }
            }
        }
        return new Device(tokens[0], tokens[1], servers);
    }

    public static class Device {
        public final String name;
        private final String deviceClass;
        public final List<Map<String, String>> servers;

        public Device(final String name, final String deviceClass, final List<Map<String, String>> servers) {
            this.name = name;
            this.deviceClass = deviceClass;
            this.servers = servers;
        }

        @Override
        public String toString() {
            return "Device{name='" + name + '\'' + ", deviceClass='" + deviceClass + '\'' + ", servers=" + servers + '}';
        }

        public String getAddress() {
            // N.B. here the '9' in 'rda3://9' is an indicator that the entry has 9 fields
            // useful snippet for manual queries:
            // echo -e "get-device-info\nGSCD025\n\n" | nc cmwpro00a.acc.gsi.de 5021 | sed -e "s%#%\n#%g"
            return servers.stream().filter(s -> "rda3://9".equals(s.get("protocol"))).map(s -> s.get("Address:")).findFirst().orElseThrow();
        }
    }

    public static class DirectoryClientException extends Exception {
        private static final long serialVersionUID = -4452775634393421952L;
        public DirectoryClientException(final String errorMsg) {
            super(errorMsg);
        }
        public DirectoryClientException(final String errorMsg, final Exception cause) {
            super(errorMsg, cause);
        }
    }
}
