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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;

import static org.apache.seatunnel.connectors.seatunnel.udp.UdpPacketFormat.InvalidDatagram;

/**
 * Decodes plaintext collectd Network plugin metrics. Metadata parts apply to later VALUES parts in
 * the same datagram only.
 */
final class UdpCollectd {
    private static final int HOST = 0x0000;
    private static final int TIME = 0x0001;
    private static final int PLUGIN = 0x0002;
    private static final int PLUGIN_INSTANCE = 0x0003;
    private static final int TYPE = 0x0004;
    private static final int TYPE_INSTANCE = 0x0005;
    private static final int VALUES = 0x0006;
    private static final int INTERVAL = 0x0007;
    private static final int TIME_HIRES = 0x0008;
    private static final int INTERVAL_HIRES = 0x0009;
    private static final int MESSAGE = 0x0100;
    private static final int SEVERITY = 0x0101;
    private static final int SIGNED = 0x0200;
    private static final int ENCRYPTED = 0x0210;
    private static final int PART_HEADER_BYTES = 4;
    private static final long FRACTION_MASK = (1L << 30) - 1;

    private UdpCollectd() {}

    /*
     * Reset metadata for each packet and validate every part before the caller emits any rows.
     * Unknown well-framed parts are skipped, while protected/notification packets fail explicitly.
     */
    static void decode(ByteBuffer data, List<Object[]> rows, int maxRecords)
            throws InvalidDatagram {
        Metadata metadata = new Metadata();
        while (data.hasRemaining()) {
            if (data.remaining() < PART_HEADER_BYTES) {
                throw new InvalidDatagram("truncated collectd part header");
            }
            int type = Short.toUnsignedInt(data.getShort());
            int length = Short.toUnsignedInt(data.getShort()) - PART_HEADER_BYTES;
            if (length < 0 || length > data.remaining()) {
                throw new InvalidDatagram("invalid collectd part length");
            }
            ByteBuffer part = data.slice();
            part.limit(length);
            data.position(data.position() + length);
            switch (type) {
                case HOST:
                    metadata.host = string(part);
                    break;
                case TIME:
                case TIME_HIRES:
                    metadata.time = milliseconds(part, type == TIME_HIRES);
                    break;
                case PLUGIN:
                    metadata.plugin = string(part);
                    break;
                case PLUGIN_INSTANCE:
                    metadata.pluginInstance = string(part);
                    break;
                case TYPE:
                    metadata.metricType = string(part);
                    break;
                case TYPE_INSTANCE:
                    metadata.typeInstance = string(part);
                    break;
                case INTERVAL:
                case INTERVAL_HIRES:
                    metadata.interval = milliseconds(part, type == INTERVAL_HIRES);
                    break;
                case VALUES:
                    values(part, metadata, rows, maxRecords);
                    break;
                case SIGNED:
                case ENCRYPTED:
                    throw new InvalidDatagram("protected collectd datagrams are unsupported");
                case MESSAGE:
                case SEVERITY:
                    throw new InvalidDatagram("collectd notifications are unsupported");
                default:
                    // The protocol permits forward-compatible unknown parts with valid lengths.
                    break;
            }
        }
        if (rows.isEmpty()) {
            throw new InvalidDatagram("collectd datagram contains no metric values");
        }
    }

    private static String string(ByteBuffer part) throws InvalidDatagram {
        if (!part.hasRemaining() || part.get(part.limit() - 1) != 0) {
            throw new InvalidDatagram("collectd string part must end in NUL");
        }
        part.limit(part.limit() - 1);
        String result = UdpPacketFormat.text(part);
        if (result.indexOf('\0') >= 0) {
            throw new InvalidDatagram("collectd string part contains an embedded NUL");
        }
        return result;
    }

    private static long milliseconds(ByteBuffer part, boolean hires) throws InvalidDatagram {
        if (part.remaining() != Long.BYTES) {
            throw new InvalidDatagram("invalid collectd time part length");
        }
        long time = part.getLong();
        if (hires) {
            return (time >>> 30) * 1000L + (((time & FRACTION_MASK) * 1000L) >>> 30);
        }
        if (time < 0 || time > Long.MAX_VALUE / 1000L) {
            throw new InvalidDatagram("collectd timestamp is outside the supported range");
        }
        return time * 1000L;
    }

    /**
     * The type array is followed by eight bytes per sample. Gauges are little endian and integer
     * samples are in network byte order. Store unsigned values without narrowing.
     */
    private static void values(ByteBuffer part, Metadata meta, List<Object[]> rows, int maxRecords)
            throws InvalidDatagram {
        if (part.remaining() < Short.BYTES
                || empty(meta.host)
                || empty(meta.plugin)
                || empty(meta.metricType)
                || meta.time == null) {
            throw new InvalidDatagram("collectd values require host, time, plugin and type");
        }
        int count = Short.toUnsignedInt(part.getShort());
        if (count == 0 || count > maxRecords - rows.size() || part.remaining() != 9 * count) {
            throw new InvalidDatagram("collectd values length or record count is invalid");
        }
        int typeOffset = part.position();
        part.position(typeOffset + count);
        for (int index = 0; index < count; index++) {
            int type = Byte.toUnsignedInt(part.get(typeOffset + index));
            long raw = part.getLong();
            String name;
            BigDecimal integer = null;
            Double gauge = null;
            switch (type) {
                case 0:
                case 3:
                    name = type == 0 ? "COUNTER" : "ABSOLUTE";
                    integer = new BigDecimal(Long.toUnsignedString(raw));
                    break;
                case 1:
                    name = "GAUGE";
                    gauge = Double.longBitsToDouble(Long.reverseBytes(raw));
                    break;
                case 2:
                    name = "DERIVE";
                    integer = BigDecimal.valueOf(raw);
                    break;
                default:
                    throw new InvalidDatagram("unknown collectd value type");
            }
            rows.add(
                    new Object[] {
                        meta.host,
                        meta.time,
                        meta.interval,
                        meta.plugin,
                        meta.pluginInstance,
                        meta.metricType,
                        meta.typeInstance,
                        index,
                        name,
                        integer,
                        gauge
                    });
        }
    }

    private static boolean empty(String value) {
        return value == null || value.isEmpty();
    }

    /**
     * Carries metadata between parts of one packet until a later part replaces it. A new instance
     * for each datagram prevents one sender from inheriting another packet's host or timestamp.
     */
    private static final class Metadata {
        String host;
        Long time;
        Long interval;
        String plugin;
        String pluginInstance;
        String metricType;
        String typeInstance;
    }
}
