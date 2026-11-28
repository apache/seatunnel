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

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Owns fixed output schemas and complete-datagram conversion for the selected wire formats.
 *
 * <p>Expansion limits are checked before any decoded row is exposed to the collector.
 */
enum UdpPacketFormat {
    TEXT(columns("payload", BasicType.STRING_TYPE)),
    BINARY(columns("payload", PrimitiveByteArrayType.INSTANCE)),
    SYSLOG(
            columns(
                    "facility severity version event_time hostname app_name process_id message_id structured_data message",
                    BasicType.INT_TYPE,
                    BasicType.INT_TYPE,
                    BasicType.INT_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE)),
    COLLECTD(
            columns(
                    "hostname time_ms interval_ms plugin plugin_instance metric_type type_instance value_index value_type integer_value gauge_value",
                    BasicType.STRING_TYPE,
                    BasicType.LONG_TYPE,
                    BasicType.LONG_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.INT_TYPE,
                    BasicType.STRING_TYPE,
                    new DecimalType(20, 0),
                    BasicType.DOUBLE_TYPE)),
    NETFLOW_V5(
            columns(
                    "uptime_ms unix_seconds unix_nanoseconds flow_sequence engine_type engine_id sampling_mode sampling_interval source_ip destination_ip next_hop input_interface output_interface packets octets first_ms last_ms source_port destination_port tcp_flags protocol tos source_as destination_as source_mask destination_mask",
                    BasicType.LONG_TYPE,
                    BasicType.LONG_TYPE,
                    BasicType.LONG_TYPE,
                    BasicType.LONG_TYPE,
                    BasicType.INT_TYPE,
                    BasicType.INT_TYPE,
                    BasicType.INT_TYPE,
                    BasicType.INT_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.INT_TYPE,
                    BasicType.INT_TYPE,
                    BasicType.LONG_TYPE,
                    BasicType.LONG_TYPE,
                    BasicType.LONG_TYPE,
                    BasicType.LONG_TYPE,
                    BasicType.INT_TYPE,
                    BasicType.INT_TYPE,
                    BasicType.INT_TYPE,
                    BasicType.INT_TYPE,
                    BasicType.INT_TYPE,
                    BasicType.INT_TYPE,
                    BasicType.INT_TYPE,
                    BasicType.INT_TYPE,
                    BasicType.INT_TYPE));

    final SeaTunnelRowType schema;

    UdpPacketFormat(SeaTunnelRowType schema) {
        this.schema = schema;
    }

    /**
     * Decode the entire datagram before emitting any of its rows. A malformed tail cannot leak a
     * successful prefix to the pipeline.
     */
    List<Object[]> decode(ByteBuffer data, byte[] separator, int[] prefix, int limit)
            throws InvalidDatagram {
        List<Object[]> rows = new ArrayList<>();
        switch (this) {
            case SYSLOG:
                append(rows, UdpSyslog.decode(text(data)), limit);
                break;
            case COLLECTD:
                UdpCollectd.decode(data, rows, limit);
                break;
            case NETFLOW_V5:
                UdpNetflowV5.decode(data, rows, limit);
                break;
            default:
                decodeRaw(data, separator, prefix, rows, limit);
        }
        return rows;
    }

    static String text(ByteBuffer data) throws InvalidDatagram {
        try {
            return StandardCharsets.UTF_8.newDecoder().decode(data).toString();
        } catch (CharacterCodingException e) {
            throw new InvalidDatagram("invalid UTF-8");
        }
    }

    static void append(List<Object[]> rows, Object[] values, int limit) throws InvalidDatagram {
        if (rows.size() >= limit) {
            throw new InvalidDatagram("record count exceeds max_records_per_datagram");
        }
        rows.add(values);
    }

    private void decodeRaw(
            ByteBuffer data, byte[] separator, int[] prefix, List<Object[]> rows, int limit)
            throws InvalidDatagram {
        if (separator.length == 0) {
            append(rows, raw(data), limit);
            return;
        }
        int start = 0;
        int matched = 0;
        // Linear prefix matching keeps separator-heavy datagrams bounded in time.
        for (int i = 0; i < data.limit(); i++) {
            byte next = data.get(i);
            while (matched > 0 && next != separator[matched]) {
                matched = prefix[matched - 1];
            }
            if (next == separator[matched] && ++matched == separator.length) {
                int end = i + 1 - matched;
                appendChunk(data, start, end, rows, limit);
                start = i + 1;
                matched = 0;
            }
        }
        appendChunk(data, start, data.limit(), rows, limit);
    }

    private void appendChunk(ByteBuffer data, int start, int end, List<Object[]> rows, int limit)
            throws InvalidDatagram {
        if (end > start) {
            ByteBuffer slice = data.duplicate();
            slice.position(start);
            slice.limit(end);
            append(rows, raw(slice), limit);
        }
    }

    private Object[] raw(ByteBuffer data) throws InvalidDatagram {
        if (this == TEXT) {
            return new Object[] {text(data)};
        }
        byte[] bytes = new byte[data.remaining()];
        data.get(bytes);
        return new Object[] {bytes};
    }

    private static SeaTunnelRowType columns(String names, SeaTunnelDataType<?>... types) {
        String[] fields = names.split(" ");
        int size = fields.length;
        if (types.length != size) {
            throw new IllegalArgumentException("UDP decoder field names and types must match");
        }
        String[] allFields = Arrays.copyOf(fields, size + 4);
        allFields[size] = "sender_host";
        allFields[size + 1] = "sender_port";
        allFields[size + 2] = "listen_port";
        allFields[size + 3] = "record_index";
        SeaTunnelDataType<?>[] allTypes = Arrays.copyOf(types, size + 4);
        allTypes[size] = BasicType.STRING_TYPE;
        Arrays.fill(allTypes, size + 1, size + 4, BasicType.INT_TYPE);
        return new SeaTunnelRowType(allFields, allTypes);
    }

    /**
     * Carries a local validation reason for atomic datagram rejection, without retaining input
     * bytes.
     *
     * <p>DROP counts the failure; FAIL may expose only this safe reason through the job error.
     */
    static final class InvalidDatagram extends Exception {
        private static final long serialVersionUID = 1L;

        InvalidDatagram(String message) {
            super(message);
        }
    }
}
