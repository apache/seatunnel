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

package org.apache.seatunnel.connectors.seatunnel.coap;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** NetFlow v5 records and v9 template, options and data FlowSets carried inside a request. */
final class CoapNetflow {
    static final int MAX_FIELDS = 128;
    static final int TEMPLATE_BUDGET_BYTES = (MAX_FIELDS * 2 * Integer.BYTES) + 128;

    private final int maximumTemplates;
    private final long templateTtl;
    private final LinkedHashMap<String, Template> templates = new LinkedHashMap<>();

    CoapNetflow(int maximumTemplates, int timeoutMs) {
        this.maximumTemplates = maximumTemplates;
        this.templateTtl = TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    }

    List<ObjectNode> decode(byte[] body, InetSocketAddress peer) throws IOException {
        ByteBuffer data = ByteBuffer.wrap(body);
        if (data.remaining() < 4) {
            throw invalid();
        }
        int version = shortValue(data);
        int count = shortValue(data);
        if (version == 5) {
            return v5(data, count);
        }
        if (version == 9) {
            return v9(data, count, peer);
        }
        throw invalid();
    }

    private static List<ObjectNode> v5(ByteBuffer wire, int count) throws IOException {
        if (count == 0 || count > 30 || wire.remaining() != 20 + 48 * count) {
            throw invalid();
        }
        long uptime = intValue(wire);
        long seconds = intValue(wire);
        long nanos = intValue(wire);
        long sequence = intValue(wire);
        int engineType = byteValue(wire);
        int engineId = byteValue(wire);
        int sampling = shortValue(wire);
        if (nanos >= 1000000000L) {
            throw invalid();
        }
        List<ObjectNode> records = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            ObjectNode flow =
                    CoapPayload.JSON
                            .createObjectNode()
                            .put("version", 5)
                            .put("uptime", uptime)
                            .put("export_time", seconds)
                            .put("export_nanos", nanos)
                            .put("sequence", sequence)
                            .put("engine_type", engineType)
                            .put("engine_id", engineId)
                            .put("sampling_mode", sampling >>> 14)
                            .put("sampling_interval", sampling & 0x3fff)
                            .put("src_addr", address(take(wire, 4)))
                            .put("dst_addr", address(take(wire, 4)))
                            .put("next_hop", address(take(wire, 4)))
                            .put("input", shortValue(wire))
                            .put("output", shortValue(wire))
                            .put("packets", intValue(wire))
                            .put("octets", intValue(wire))
                            .put("first", intValue(wire))
                            .put("last", intValue(wire))
                            .put("src_port", shortValue(wire))
                            .put("dst_port", shortValue(wire));
            byteValue(wire);
            flow.put("tcp_flags", byteValue(wire))
                    .put("protocol", byteValue(wire))
                    .put("tos", byteValue(wire))
                    .put("src_as", shortValue(wire))
                    .put("dst_as", shortValue(wire))
                    .put("src_mask", byteValue(wire))
                    .put("dst_mask", byteValue(wire));
            shortValue(wire);
            records.add(flow);
        }
        return records;
    }

    private List<ObjectNode> v9(ByteBuffer wire, int count, InetSocketAddress peer)
            throws IOException {
        if (wire.remaining() < 16 || count == 0 || count > CoapSettings.MAX_ROWS) {
            throw invalid();
        }
        long uptime = intValue(wire);
        long seconds = intValue(wire);
        long sequence = intValue(wire);
        long source = intValue(wire);
        long now = System.nanoTime();
        templates.values().removeIf(template -> now - template.received >= templateTtl);
        String exporter = peer.getAddress().getHostAddress() + '/' + source + '/';
        Map<String, Template> currentPacket = new LinkedHashMap<>();
        List<DataSet> sets = new ArrayList<>();
        int declared = 0;
        int minimum = 0;
        int maximum = 0;
        while (wire.hasRemaining()) {
            if (wire.remaining() < 4 || declared + minimum >= count) {
                throw invalid();
            }
            int setId = shortValue(wire);
            int size = shortValue(wire);
            if (size <= 4 || size - 4 > wire.remaining()) {
                throw invalid();
            }
            ByteBuffer set = wire.slice();
            set.limit(size - 4);
            wire.position(wire.position() + size - 4);
            if (setId == 0 || setId == 1) {
                declared += templates(set, setId == 1, exporter, now, currentPacket);
                padding(set);
            } else if (setId >= 256) {
                String key = exporter + setId;
                Template template = currentPacket.getOrDefault(key, templates.get(key));
                if (template == null) {
                    throw new IOException(
                            "NetFlow v9 data has no active template for its exporter");
                }
                DataSet block = new DataSet(setId, set, template);
                minimum += block.minimum;
                maximum += block.maximum;
                sets.add(block);
            } else {
                throw invalid();
            }
        }
        int expected = count - declared;
        if (expected < minimum || expected > maximum) {
            throw invalid();
        }
        List<ObjectNode> records = new ArrayList<>(expected);
        for (DataSet set : sets) {
            int least = Math.max(set.minimum, expected - maximum + set.maximum);
            int most = Math.min(set.maximum, expected - minimum + set.minimum);
            if (least != most) {
                throw new IOException("NetFlow v9 packet has ambiguous FlowSet padding");
            }
            for (int i = 0; i < least; i++) {
                records.add(set.record(uptime, seconds, sequence, source));
            }
            padding(set.bytes);
        }
        // A malformed or incomplete message may not replace working templates.
        currentPacket.forEach(
                (key, template) -> {
                    templates.remove(key);
                    if (templates.size() == maximumTemplates) {
                        templates.remove(templates.keySet().iterator().next());
                    }
                    templates.put(key, template);
                });
        return records;
    }

    private int templates(
            ByteBuffer set,
            boolean options,
            String exporter,
            long now,
            Map<String, Template> packet)
            throws IOException {
        int count = 0;
        int header = options ? 6 : 4;
        while (set.remaining() >= header) {
            int id = shortValue(set);
            int first = shortValue(set);
            int second = options ? shortValue(set) : 0;
            int scopes = options ? first / 4 : 0;
            int fields = options ? scopes + second / 4 : first;
            if (id < 256
                    || fields == 0
                    || fields > MAX_FIELDS
                    || set.remaining() < fields * 4
                    || (options && (scopes == 0 || first % 4 != 0 || second % 4 != 0))
                    || packet.size() >= maximumTemplates) {
                throw invalid();
            }
            int[] ids = new int[fields];
            int[] widths = new int[fields];
            int width = 0;
            for (int i = 0; i < fields; i++) {
                ids[i] = shortValue(set);
                widths[i] = shortValue(set);
                width += widths[i];
                if (widths[i] == 0
                        || widths[i] > CoapSettings.ABSOLUTE_BODY_LIMIT
                        || width > CoapSettings.ABSOLUTE_BODY_LIMIT) {
                    throw invalid();
                }
            }
            packet.put(exporter + id, new Template(ids, widths, width, scopes, now));
            count++;
        }
        return count;
    }

    private static JsonNode value(int field, boolean scope, byte[] bytes) throws IOException {
        if (!scope
                && ((bytes.length == 4 && (field == 8 || field == 12 || field == 15 || field == 18))
                        || (bytes.length == 16
                                && (field == 27 || field == 28 || field == 62 || field == 63)))) {
            return CoapPayload.JSON.getNodeFactory().textNode(address(bytes));
        }
        return bytes.length <= 8
                ? CoapPayload.JSON.getNodeFactory().numberNode(new BigInteger(1, bytes))
                : CoapPayload.JSON
                        .getNodeFactory()
                        .textNode(Base64.getEncoder().encodeToString(bytes));
    }

    private static String address(byte[] bytes) throws IOException {
        return InetAddress.getByAddress(bytes).getHostAddress();
    }

    private static void padding(ByteBuffer bytes) throws IOException {
        if (bytes.remaining() > 3) {
            throw invalid();
        }
        while (bytes.hasRemaining()) {
            if (bytes.get() != 0) {
                throw invalid();
            }
        }
    }

    private static int byteValue(ByteBuffer buffer) {
        return Byte.toUnsignedInt(buffer.get());
    }

    private static int shortValue(ByteBuffer buffer) {
        return Short.toUnsignedInt(buffer.getShort());
    }

    private static long intValue(ByteBuffer buffer) {
        return Integer.toUnsignedLong(buffer.getInt());
    }

    private static byte[] take(ByteBuffer buffer, int size) {
        byte[] output = new byte[size];
        buffer.get(output);
        return output;
    }

    private static IOException invalid() {
        return new IOException("invalid NetFlow v5/v9 payload");
    }

    /**
     * Keeps one data FlowSet within the bounded packet. Short records can resemble zero padding, so
     * the packet count must disambiguate each data set before records are decoded.
     */
    private static final class DataSet {
        private final int id;
        private final ByteBuffer bytes;
        private final Template template;
        private final int minimum;
        private final int maximum;

        private DataSet(int id, ByteBuffer bytes, Template template) throws IOException {
            this.id = id;
            this.bytes = bytes;
            this.template = template;
            int end = bytes.limit();
            maximum = bytes.remaining() / template.width;
            if (maximum == 0 || !zeroSuffix(bytes, end - maximum * template.width)) {
                throw invalid();
            }
            int possible = maximum;
            while (possible > 1 && zeroSuffix(bytes, end - (possible - 1) * template.width)) {
                possible--;
            }
            minimum = possible;
        }

        private ObjectNode record(long uptime, long seconds, long sequence, long source)
                throws IOException {
            ObjectNode record =
                    CoapPayload.JSON
                            .createObjectNode()
                            .put("version", 9)
                            .put("uptime", uptime)
                            .put("export_time", seconds)
                            .put("sequence", sequence)
                            .put("source_id", source)
                            .put("template_id", id)
                            .put("options", template.scopes > 0);
            ArrayNode fields = record.putArray("fields");
            for (int i = 0; i < template.ids.length; i++) {
                int fieldId = template.ids[i];
                byte[] valueBytes = take(bytes, template.widths[i]);
                boolean scope = i < template.scopes;
                ObjectNode field = fields.addObject().put("id", fieldId).put("scope", scope);
                field.set("value", value(fieldId, scope, valueBytes));
            }
            return record;
        }

        private static boolean zeroSuffix(ByteBuffer bytes, int padding) {
            if (padding > 3) {
                return false;
            }
            for (int i = bytes.limit() - padding; i < bytes.limit(); i++) {
                if (bytes.get(i) != 0) {
                    return false;
                }
            }
            return true;
        }
    }

    /** Stores field ordering, value widths and scope information until the template expires. */
    private static final class Template {
        private final int[] ids;
        private final int[] widths;
        private final int width;
        private final int scopes;
        private final long received;

        private Template(int[] ids, int[] widths, int width, int scopes, long received) {
            this.ids = ids;
            this.widths = widths;
            this.width = width;
            this.scopes = scopes;
            this.received = received;
        }
    }
}
