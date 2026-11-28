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
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.MappingIterator;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** Reads and writes newline-separated JSON records with a header and tagged field values. */
final class CoapRecordJson {
    private static final Set<String> TYPES =
            Set.of(
                    "MAP",
                    "LIST",
                    "LIST_MAP",
                    "BYTE_ARRAY",
                    "CHAR",
                    "STRING",
                    "BOOLEAN",
                    "BYTE",
                    "SHORT",
                    "INTEGER",
                    "LONG",
                    "FLOAT",
                    "DOUBLE",
                    "DECIMAL",
                    "DATE",
                    "TIME",
                    "DATETIME",
                    "ZONED_DATETIME");

    private CoapRecordJson() {}

    static CoapPayload.Decoder decoder(CoapSettings.Listener settings, CatalogTable table) {
        CoapParsedRows rows = new CoapParsedRows(settings, table);
        String headerField = settings.options.get(CoapOptions.RECORD_HEADER_FIELD);
        if (headerField != null
                && (!settings.typed
                        || headerField.isEmpty()
                        || table.getSeaTunnelRowType().indexOf(headerField, false) < 0)) {
            throw CoapSettings.invalid(
                    "record_header_field must name a field in the output schema");
        }
        return (bytes, peer) -> {
            List<SeaTunnelRow> result = new ArrayList<>();
            try (MappingIterator<JsonNode> records =
                    CoapPayload.JSON
                            .readerFor(JsonNode.class)
                            .readValues(CoapPayload.decodeText(bytes))) {
                while (records.hasNextValue()) {
                    if (result.size() == CoapSettings.MAX_ROWS) {
                        throw invalid();
                    }
                    JsonNode record = records.nextValue();
                    if (!record.path("header").isObject() || !record.has("value")) {
                        throw invalid();
                    }
                    JsonNode value = unpack(record.get("value"));
                    if (!settings.typed) {
                        ObjectNode envelope = CoapPayload.JSON.createObjectNode();
                        envelope.set("header", record.get("header"));
                        envelope.set("value", value);
                        value = envelope;
                    } else if (headerField != null) {
                        if (!value.isObject() || value.has(headerField)) {
                            throw invalid();
                        }
                        ((ObjectNode) value).set(headerField, record.get("header"));
                    }
                    result.add(rows.row(value));
                }
            }
            if (result.isEmpty()) {
                throw invalid();
            }
            return result;
        };
    }

    static CoapPayload.Encoder encoder(SeaTunnelRowType type, int maxBytes) {
        return row -> {
            CoapPayload.limitRow(row, maxBytes);
            ObjectNode result = CoapPayload.JSON.createObjectNode();
            String id = UUID.randomUUID().toString();
            result.putObject("header")
                    .put("stageCreator", "Apache SeaTunnel")
                    .put("sourceId", id)
                    .put("trackingId", id)
                    .put("stagesPath", "CoAP")
                    .putObject("values");
            result.set("value", pack(type, row));
            CoapPayload.LimitedOutput output = new CoapPayload.LimitedOutput(maxBytes);
            // Reserve the record separator within the same per-message byte limit.
            CoapPayload.JSON
                    .writer()
                    .without(
                            org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonGenerator
                                    .Feature.AUTO_CLOSE_TARGET)
                    .writeValue(output, result);
            output.write('\n');
            return output.bytes();
        };
    }

    private static JsonNode unpack(JsonNode field) throws IOException {
        if (field.isNull()) {
            return field;
        }
        JsonNode fieldType = field.get("type");
        JsonNode value = field.get("value");
        if (fieldType == null
                || !fieldType.isTextual()
                || !TYPES.contains(fieldType.textValue())
                || value == null) {
            throw invalid();
        }
        if (value.isNull()) {
            return value;
        }
        switch (fieldType.textValue()) {
            case "MAP":
                if (!value.isObject()) {
                    throw invalid();
                }
                ObjectNode map = CoapPayload.JSON.createObjectNode();
                for (Iterator<Map.Entry<String, JsonNode>> i = value.fields(); i.hasNext(); ) {
                    Map.Entry<String, JsonNode> item = i.next();
                    map.set(item.getKey(), unpack(item.getValue()));
                }
                return map;
            case "LIST":
            case "LIST_MAP":
                if (!value.isArray()) {
                    throw invalid();
                }
                return fieldType.textValue().equals("LIST")
                        ? unpackList(value)
                        : unpackOrderedMap(value);
            case "BYTE_ARRAY":
                if (!value.isTextual()) {
                    throw invalid();
                }
                value.binaryValue();
                return value;
            case "CHAR":
            case "STRING":
                if (!value.isTextual()) {
                    throw invalid();
                }
                return value;
            case "BOOLEAN":
                if (!value.isBoolean()) {
                    throw invalid();
                }
                return value;
            case "BYTE":
            case "SHORT":
            case "INTEGER":
            case "LONG":
            case "FLOAT":
            case "DOUBLE":
            case "DECIMAL":
                return number(fieldType.textValue(), value);
            case "DATE":
            case "TIME":
            case "DATETIME":
            case "ZONED_DATETIME":
                return date(fieldType.textValue(), value);
            default:
                throw invalid();
        }
    }

    private static JsonNode unpackList(JsonNode children) throws IOException {
        ArrayNode result = CoapPayload.JSON.createArrayNode();
        for (JsonNode child : children) {
            result.add(unpack(child));
        }
        return result;
    }

    private static JsonNode unpackOrderedMap(JsonNode children) throws IOException {
        ObjectNode result = CoapPayload.JSON.createObjectNode();
        for (JsonNode child : children) {
            String path = child.path("sqpath").asText();
            String name = lastSegment(path);
            if (result.has(name)) {
                throw invalid();
            }
            result.set(name, unpack(child));
        }
        return result;
    }

    private static String lastSegment(String path) throws IOException {
        boolean quoted = false;
        boolean escaped = false;
        int start = -1;
        StringBuilder name = new StringBuilder();
        for (int i = 0; i < path.length(); i++) {
            char ch = path.charAt(i);
            if (escaped) {
                name.append(ch);
                escaped = false;
            } else if (ch == '\\') {
                escaped = true;
            } else if (ch == '\'') {
                quoted = !quoted;
            } else if (ch == '/' && !quoted) {
                start = i;
                name.setLength(0);
            } else {
                name.append(ch);
            }
        }
        if (start < 0 || quoted || escaped || name.length() == 0) {
            throw invalid();
        }
        return name.toString();
    }

    private static JsonNode number(String type, JsonNode value) throws IOException {
        if ((!value.isNumber() && !value.isTextual()) || value.asText().length() > 1024) {
            throw invalid();
        }
        String text = value.asText();
        if (("FLOAT".equals(type) || "DOUBLE".equals(type))
                && ("NaN".equals(text) || "Infinity".equals(text) || "-Infinity".equals(text))) {
            return CoapPayload.JSON.getNodeFactory().textNode(text);
        }
        try {
            switch (type) {
                case "BYTE":
                    return CoapPayload.JSON.getNodeFactory().numberNode(Byte.parseByte(text));
                case "SHORT":
                    return CoapPayload.JSON.getNodeFactory().numberNode(Short.parseShort(text));
                case "INTEGER":
                    return CoapPayload.JSON.getNodeFactory().numberNode(Integer.parseInt(text));
                case "LONG":
                    return CoapPayload.JSON.getNodeFactory().numberNode(Long.parseLong(text));
                default:
                    return CoapPayload.JSON.getNodeFactory().numberNode(new BigDecimal(text));
            }
        } catch (NumberFormatException e) {
            throw invalid();
        }
    }

    private static JsonNode date(String type, JsonNode value) throws IOException {
        if (value.isTextual()) {
            return value;
        }
        if (!value.isIntegralNumber() || !value.canConvertToLong()) {
            throw invalid();
        }
        OffsetDateTime date = Instant.ofEpochMilli(value.longValue()).atOffset(ZoneOffset.UTC);
        String converted;
        switch (type) {
            case "DATE":
                converted = date.toLocalDate().toString();
                break;
            case "TIME":
                converted = date.toLocalTime().toString();
                break;
            case "ZONED_DATETIME":
                converted = date.toString();
                break;
            default:
                converted = date.toLocalDateTime().toString();
        }
        return CoapPayload.JSON.getNodeFactory().textNode(converted);
    }

    private static ObjectNode pack(SeaTunnelDataType<?> type, Object value) throws IOException {
        ObjectNode field = CoapPayload.JSON.createObjectNode();
        String name;
        JsonNode child = CoapPayload.JSON.getNodeFactory().nullNode();
        switch (type.getSqlType()) {
            case NULL:
                name = "STRING";
                break;
            case TINYINT:
                name = "BYTE";
                break;
            case SMALLINT:
                name = "SHORT";
                break;
            case INT:
                name = "INTEGER";
                break;
            case BIGINT:
                name = "LONG";
                break;
            case TIMESTAMP:
                name = "DATETIME";
                break;
            case TIMESTAMP_TZ:
                name = "ZONED_DATETIME";
                break;
            case BYTES:
                name = "BYTE_ARRAY";
                break;
            case ROW:
            case MAP:
                name = "MAP";
                break;
            case ARRAY:
                name = "LIST";
                break;
            default:
                name = type.getSqlType().name();
        }
        if (value != null) {
            child = packValue(type, value);
        }
        field.put("type", name);
        field.set("value", child);
        return field;
    }

    private static JsonNode packValue(SeaTunnelDataType<?> type, Object value) throws IOException {
        switch (type.getSqlType()) {
            case ROW:
                ObjectNode row = CoapPayload.JSON.createObjectNode();
                SeaTunnelRowType fields = (SeaTunnelRowType) type;
                for (int i = 0; i < fields.getTotalFields(); i++) {
                    row.set(
                            fields.getFieldName(i),
                            pack(fields.getFieldType(i), ((SeaTunnelRow) value).getField(i)));
                }
                return row;
            case MAP:
                ObjectNode map = CoapPayload.JSON.createObjectNode();
                for (Map.Entry<?, ?> child : ((Map<?, ?>) value).entrySet()) {
                    if (!(child.getKey() instanceof String)) {
                        throw invalid();
                    }
                    map.set(
                            (String) child.getKey(),
                            pack(((MapType<?, ?>) type).getValueType(), child.getValue()));
                }
                return map;
            case ARRAY:
                ArrayNode array = CoapPayload.JSON.createArrayNode();
                for (int i = 0; i < Array.getLength(value); i++) {
                    array.add(pack(((ArrayType<?, ?>) type).getElementType(), Array.get(value, i)));
                }
                return array;
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                return CoapPayload.JSON.getNodeFactory().textNode(value.toString());
            case DATE:
                return CoapPayload.JSON
                        .getNodeFactory()
                        .numberNode(
                                ((LocalDate) value)
                                        .atStartOfDay(ZoneOffset.UTC)
                                        .toInstant()
                                        .toEpochMilli());
            case TIME:
                return CoapPayload.JSON
                        .getNodeFactory()
                        .numberNode(((LocalTime) value).toNanoOfDay() / 1000000);
            case TIMESTAMP:
                return CoapPayload.JSON
                        .getNodeFactory()
                        .numberNode(
                                ((LocalDateTime) value).toInstant(ZoneOffset.UTC).toEpochMilli());
            case TIMESTAMP_TZ:
                return CoapPayload.JSON.getNodeFactory().textNode(value.toString());
            case BYTES:
            case STRING:
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case DECIMAL:
                return CoapPayload.JSON.valueToTree(value);
            default:
                throw invalid();
        }
    }

    private static IOException invalid() {
        return new IOException("invalid typed CoAP JSON record");
    }
}
