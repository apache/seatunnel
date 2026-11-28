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

package org.apache.seatunnel.connectors.seatunnel.opcua;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.eclipse.milo.opcua.stack.core.types.builtin.ByteString;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.DateTime;
import org.eclipse.milo.opcua.stack.core.types.builtin.StatusCode;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UByte;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UInteger;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.ULong;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UShort;
import org.eclipse.milo.opcua.stack.core.types.enumerated.NodeClass;

import java.lang.reflect.Array;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaFailure.problem;

/** Stable row schema and bounded conversion of the standard built-in UA value types. */
final class OpcUaRows {
    static final int MAX_BUFFER_BYTES = 16 * 1024 * 1024;
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String[] NAMES = {
        "kind",
        "node_id",
        "node_alias",
        "parent_id",
        "browse_name",
        "node_class",
        "depth",
        "value_type",
        "value_json",
        "quality",
        "status_code",
        "source_time",
        "source_picoseconds",
        "server_time",
        "server_picoseconds",
        "diagnostic"
    };
    private static final Map<Class<?>, String> TYPES =
            Map.ofEntries(
                    Map.entry(Boolean.class, "Boolean"),
                    Map.entry(boolean.class, "Boolean"),
                    Map.entry(Byte.class, "SByte"),
                    Map.entry(byte.class, "SByte"),
                    Map.entry(UByte.class, "Byte"),
                    Map.entry(Short.class, "Int16"),
                    Map.entry(short.class, "Int16"),
                    Map.entry(UShort.class, "UInt16"),
                    Map.entry(Integer.class, "Int32"),
                    Map.entry(int.class, "Int32"),
                    Map.entry(UInteger.class, "UInt32"),
                    Map.entry(Long.class, "Int64"),
                    Map.entry(long.class, "Int64"),
                    Map.entry(ULong.class, "UInt64"),
                    Map.entry(Float.class, "Float"),
                    Map.entry(float.class, "Float"),
                    Map.entry(Double.class, "Double"),
                    Map.entry(double.class, "Double"),
                    Map.entry(String.class, "String"),
                    Map.entry(DateTime.class, "DateTime"),
                    Map.entry(UUID.class, "Guid"),
                    Map.entry(ByteString.class, "ByteString"));

    private final OpcUaSettings settings;
    private final String tableId;

    OpcUaRows(OpcUaSettings settings, String tableId) {
        this.settings = settings;
        this.tableId = tableId;
    }

    static TableSchema schema() {
        TableSchema.Builder schema = TableSchema.builder();
        for (String name : NAMES) {
            SeaTunnelDataType<?> type = BasicType.STRING_TYPE;
            if ("status_code".equals(name)) {
                type = BasicType.LONG_TYPE;
            } else if ("depth".equals(name) || name.endsWith("_picoseconds")) {
                type = BasicType.INT_TYPE;
            }
            schema.column(PhysicalColumn.of(name, type, 0, !"kind".equals(name), null, null));
        }
        return schema.build();
    }

    SeaTunnelRow value(String id, String alias, DataValue data) throws OpcUaFailure {
        if (data == null) {
            throw problem("Read returned a missing DataValue");
        }
        StatusCode status = data.getStatusCode() == null ? StatusCode.GOOD : data.getStatusCode();
        Object value =
                status.isBad() || data.getValue() == null ? null : data.getValue().getValue();
        String type = value == null ? "Null" : typeName(value.getClass());
        String encoded = encode(value);
        return row(
                "VALUE",
                id,
                alias,
                null,
                null,
                null,
                null,
                type,
                encoded,
                status.isGood() ? "GOOD" : status.isBad() ? "BAD" : "UNCERTAIN",
                status.getValue(),
                date(data.getSourceTime()),
                small(data.getSourcePicoseconds()),
                date(data.getServerTime()),
                small(data.getServerPicoseconds()),
                null);
    }

    SeaTunnelRow node(String id, String parent, String name, NodeClass kind, int depth) {
        return row(
                "NODE",
                id,
                null,
                parent,
                name,
                kind.name(),
                depth,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null);
    }

    SeaTunnelRow gap(String reason) {
        return row(
                "GAP", null, null, null, null, null, null, null, null, null, null, null, null, null,
                null, reason);
    }

    static int weight(SeaTunnelRow row) {
        int bytes = 256;
        for (int i = 0; i < NAMES.length; i++) {
            Object field = row.getField(i);
            if (field instanceof String) {
                bytes += 2 * ((String) field).length();
            }
        }
        return bytes;
    }

    private SeaTunnelRow row(Object... fields) {
        SeaTunnelRow row = new SeaTunnelRow(fields);
        row.setTableId(tableId);
        return row;
    }

    private static String typeName(Class<?> value) throws OpcUaFailure {
        if (value.isArray()) {
            return typeName(value.getComponentType()) + "[]";
        }
        String type = TYPES.get(value);
        if (type == null) {
            throw problem("unsupported OPC UA value type");
        }
        return type;
    }

    private String encode(Object value) throws OpcUaFailure {
        Object exact = normalize(value, new int[] {settings.maxArrayLength}, 0);
        try {
            String result = JSON.writeValueAsString(exact);
            if (result.getBytes(StandardCharsets.UTF_8).length > settings.maxMessageSize) {
                throw problem("normalized OPC UA value exceeds max_message_size");
            }
            return result;
        } catch (JsonProcessingException e) {
            throw problem("could not encode OPC UA value");
        }
    }

    private Object normalize(Object value, int[] budget, int depth) throws OpcUaFailure {
        if (value == null) {
            return null;
        }
        if (value.getClass().isArray()) {
            int length = Array.getLength(value);
            if (depth >= 32 || length > budget[0]) {
                throw problem("OPC UA array exceeds its element or depth limit");
            }
            budget[0] -= length;
            List<Object> entries = new ArrayList<>(length);
            for (int i = 0; i < length; i++) {
                entries.add(normalize(Array.get(value, i), budget, depth + 1));
            }
            return entries;
        }
        // Only explicitly supported wrappers can supply business values.
        typeName(value.getClass());
        if (value instanceof ByteString) {
            byte[] bytes = ((ByteString) value).bytes();
            if (bytes == null) {
                return null;
            }
            checkSize(bytes.length);
            return Base64.getEncoder().encodeToString(bytes);
        }
        if (value instanceof String) {
            checkSize(((String) value).getBytes(StandardCharsets.UTF_8).length);
            return value;
        }
        if (value instanceof ULong) {
            return new BigInteger(value.toString());
        }
        if (value instanceof UInteger || value instanceof UShort || value instanceof UByte) {
            return ((Number) value).longValue();
        }
        if (value instanceof DateTime) {
            return date((DateTime) value);
        }
        if (value instanceof UUID) {
            return value.toString();
        }
        if ((value instanceof Float || value instanceof Double)
                && !Double.isFinite(((Number) value).doubleValue())) {
            return value.toString();
        }
        return value;
    }

    private void checkSize(int size) throws OpcUaFailure {
        if (size > settings.maxStringBytes) {
            throw problem("OPC UA string or ByteString exceeds max_string_bytes");
        }
    }

    private static Integer small(UShort number) {
        return number == null ? null : number.intValue();
    }

    private static String date(DateTime time) {
        if (time == null || time.isNull()) {
            return null;
        }
        long ticks = time.getUtcTime();
        long seconds = Math.floorDiv(ticks, 10000000L) - 11644473600L;
        return Instant.ofEpochSecond(seconds, Math.floorMod(ticks, 10000000L) * 100L).toString();
    }
}
