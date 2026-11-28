/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.udp;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;

import static org.apache.seatunnel.connectors.seatunnel.udp.UdpSourceOptions.BIND_HOST;
import static org.apache.seatunnel.connectors.seatunnel.udp.UdpSourceOptions.FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.udp.UdpSourceOptions.IDLE_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.udp.UdpSourceOptions.MAX_DATAGRAMS_PER_POLL;
import static org.apache.seatunnel.connectors.seatunnel.udp.UdpSourceOptions.MAX_DATAGRAM_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.udp.UdpSourceOptions.MAX_RECORDS_PER_DATAGRAM;
import static org.apache.seatunnel.connectors.seatunnel.udp.UdpSourceOptions.ON_MALFORMED;
import static org.apache.seatunnel.connectors.seatunnel.udp.UdpSourceOptions.PORTS;
import static org.apache.seatunnel.connectors.seatunnel.udp.UdpSourceOptions.RECEIVE_BUFFER_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.udp.UdpSourceOptions.SEPARATOR;

/**
 * Validated source configuration shared by the coordinator and worker. Contains no open resources.
 */
final class UdpConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    final List<Integer> ports;
    final InetAddress address;
    final UdpPacketFormat format;
    final byte[] separator;
    final int maxDatagram;
    final int receiveBuffer;
    final int maxPoll;
    final int maxRecords;
    final int idleMs;
    final boolean failMalformed;

    UdpConfig(ReadonlyConfig config) {
        List<Integer> requested = config.get(PORTS);
        if (requested == null
                || requested.isEmpty()
                || requested.size() > 64
                || requested.stream().anyMatch(port -> port == null || port < 1 || port > 65535)
                || new HashSet<>(requested).size() != requested.size()) {
            throw invalid("ports must contain 1 to 64 distinct UDP ports between 1 and 65535");
        }
        List<Integer> sorted = new ArrayList<>(requested);
        Collections.sort(sorted);
        ports = Collections.unmodifiableList(sorted);
        address = address(config.get(BIND_HOST));
        try {
            format = UdpPacketFormat.valueOf(config.get(FORMAT).toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw invalid("format must be TEXT, BINARY, SYSLOG, COLLECTD, or NETFLOW_V5");
        }
        String delim = config.get(SEPARATOR);
        if (!StandardCharsets.UTF_8.newEncoder().canEncode(delim)) {
            throw invalid("separator must be valid UTF-8");
        }
        separator = delim.getBytes(StandardCharsets.UTF_8);
        if (separator.length > 64
                || (separator.length != 0
                        && format != UdpPacketFormat.TEXT
                        && format != UdpPacketFormat.BINARY)) {
            throw invalid(
                    "separator supports up to 64 UTF-8 bytes and applies only to TEXT or BINARY");
        }
        maxDatagram = bounded(config, MAX_DATAGRAM_BYTES, 1, 65527);
        receiveBuffer = bounded(config, RECEIVE_BUFFER_BYTES, 1024, 16777216);
        maxPoll = bounded(config, MAX_DATAGRAMS_PER_POLL, 1, 128);
        maxRecords = bounded(config, MAX_RECORDS_PER_DATAGRAM, 1, 1024);
        idleMs = bounded(config, IDLE_TIMEOUT_MS, 0, 86400000);
        String malformed = config.get(ON_MALFORMED);
        if (!"FAIL".equalsIgnoreCase(malformed) && !"DROP".equalsIgnoreCase(malformed)) {
            throw invalid("on_malformed must be DROP or FAIL");
        }
        failMalformed = "FAIL".equalsIgnoreCase(malformed);
    }

    private static InetAddress address(String value) {
        // Numeric bind addresses keep DNS resolution outside the worker lifecycle.
        boolean ipv4 = value.matches("[0-9]{1,3}(\\.[0-9]{1,3}){3}");
        boolean ipv6 = value.contains(":") && value.matches("[0-9a-fA-F:.]+");
        if (!(ipv4 || ipv6)) {
            throw invalid("bind_host must be a numeric IPv4 or IPv6 address without a scope ID");
        }
        try {
            if (ipv6) {
                return InetAddress.getByName(value);
            }
            byte[] bytes = new byte[4];
            String[] octets = value.split("\\.");
            for (int index = 0; index < bytes.length; index++) {
                int octet = Integer.parseInt(octets[index]);
                if (octet > 255) {
                    throw invalid("invalid bind_host address");
                }
                bytes[index] = (byte) octet;
            }
            return InetAddress.getByAddress(bytes);
        } catch (UnknownHostException e) {
            throw invalid("invalid bind_host address");
        }
    }

    private static int bounded(
            ReadonlyConfig config, Option<Integer> option, int minimum, int maximum) {
        int value = config.get(option);
        if (value < minimum || value > maximum) {
            throw invalid(option.key() + " must be between " + minimum + " and " + maximum);
        }
        return value;
    }

    static IllegalArgumentException invalid(String message) {
        return new IllegalArgumentException("UDP source: " + message);
    }
}
