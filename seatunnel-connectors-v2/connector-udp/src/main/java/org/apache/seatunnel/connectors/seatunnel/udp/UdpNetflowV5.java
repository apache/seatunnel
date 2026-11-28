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

import java.nio.ByteBuffer;
import java.util.List;

import static org.apache.seatunnel.connectors.seatunnel.udp.UdpPacketFormat.InvalidDatagram;

/** Decoder for Cisco NetFlow v5 fixed-length, network-byte-order export packets. */
final class UdpNetflowV5 {
    private static final int HEADER_BYTES = 24;
    private static final int RECORD_BYTES = 48;

    private UdpNetflowV5() {}

    /** Require the exact packet size up front. Unsigned network counters are preserved as longs. */
    static void decode(ByteBuffer data, List<Object[]> rows, int limit) throws InvalidDatagram {
        if (data.remaining() < HEADER_BYTES || u16(data) != 5) {
            throw new InvalidDatagram("expected NetFlow v5 packet header");
        }
        int count = u16(data);
        if (count < 1
                || count > 30
                || count > limit
                || data.remaining() != HEADER_BYTES - 4 + count * RECORD_BYTES) {
            throw new InvalidDatagram(
                    "NetFlow v5 count and datagram length disagree or exceed the limit");
        }
        long uptime = u32(data);
        long seconds = u32(data);
        long nanos = u32(data);
        if (nanos >= 1000000000L) {
            throw new InvalidDatagram("invalid NetFlow v5 nanoseconds field");
        }
        long sequence = u32(data);
        int engineType = u8(data);
        int engineId = u8(data);
        int sampling = u16(data);
        for (int index = 0; index < count; index++) {
            String source = ipv4(data);
            String destination = ipv4(data);
            String nextHop = ipv4(data);
            int input = u16(data);
            int output = u16(data);
            long packets = u32(data);
            long octets = u32(data);
            long first = u32(data);
            long last = u32(data);
            int sourcePort = u16(data);
            int destinationPort = u16(data);
            data.get(); // Padding before the flags.
            int flags = u8(data);
            int protocol = u8(data);
            int tos = u8(data);
            int sourceAs = u16(data);
            int destinationAs = u16(data);
            int sourceMask = u8(data);
            int destinationMask = u8(data);
            data.getShort(); // Trailing padding.
            if (sourceMask > 32 || destinationMask > 32) {
                throw new InvalidDatagram("invalid NetFlow v5 IPv4 mask length");
            }
            rows.add(
                    new Object[] {
                        uptime,
                        seconds,
                        nanos,
                        sequence,
                        engineType,
                        engineId,
                        sampling >>> 14,
                        sampling & 0x3fff,
                        source,
                        destination,
                        nextHop,
                        input,
                        output,
                        packets,
                        octets,
                        first,
                        last,
                        sourcePort,
                        destinationPort,
                        flags,
                        protocol,
                        tos,
                        sourceAs,
                        destinationAs,
                        sourceMask,
                        destinationMask
                    });
        }
    }

    private static String ipv4(ByteBuffer data) {
        return u8(data) + "." + u8(data) + "." + u8(data) + "." + u8(data);
    }

    private static int u8(ByteBuffer data) {
        return Byte.toUnsignedInt(data.get());
    }

    private static int u16(ByteBuffer data) {
        return Short.toUnsignedInt(data.getShort());
    }

    private static long u32(ByteBuffer data) {
        return Integer.toUnsignedLong(data.getInt());
    }
}
