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

package org.apache.seatunnel.format.json.debezium;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonToken;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.MetadataUtil;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import static java.lang.String.format;

public class DebeziumJsonDeserializationSchema implements DeserializationSchema<SeaTunnelRow> {
    private static final long serialVersionUID = 1L;

    private static final String OP_KEY = "op";
    private static final String OP_READ = "r"; // snapshot read
    private static final String OP_CREATE = "c"; // insert
    private static final String OP_UPDATE = "u"; // update
    private static final String OP_DELETE = "d"; // delete
    public static final String DATA_PAYLOAD = "payload";
    private static final String DATA_BEFORE = "before";
    private static final String DATA_AFTER = "after";
    private static final String DATA_TS = "ts_ms";
    private static final String DATA_SCHEMA = "schema";
    private static final String SCHEMA_FIELD = "field";
    private static final String SCHEMA_FIELDS = "fields";
    private static final String SCHEMA_NAME = "name";

    private static final String REPLICA_IDENTITY_EXCEPTION =
            "The \"before\" field of %s operation is null, "
                    + "if you are using Debezium Postgres Connector, "
                    + "please check the Postgres table has been set REPLICA IDENTITY to FULL level.";

    public static final String FORMAT = "Debezium";

    private final SeaTunnelRowType rowType;

    private final JsonDeserializationSchema jsonDeserializer;

    private final DebeziumRowConverter debeziumRowConverter;

    private final boolean ignoreParseErrors;

    private final boolean debeziumEnabledSchema;

    private final TablePath tablePath;

    /** Cached {@link TablePath#toString()} to avoid rebuilding the table id on every row. */
    private final String tableId;

    public DebeziumJsonDeserializationSchema(CatalogTable catalogTable, boolean ignoreParseErrors) {
        this(catalogTable, ignoreParseErrors, false);
    }

    public DebeziumJsonDeserializationSchema(
            CatalogTable catalogTable, boolean ignoreParseErrors, boolean debeziumEnabledSchema) {
        this.rowType = catalogTable.getSeaTunnelRowType();
        this.ignoreParseErrors = ignoreParseErrors;
        this.jsonDeserializer =
                new JsonDeserializationSchema(catalogTable, false, ignoreParseErrors);
        this.debeziumRowConverter = new DebeziumRowConverter(rowType);
        this.debeziumEnabledSchema = debeziumEnabledSchema;
        this.tablePath = Optional.of(catalogTable).map(CatalogTable::getTablePath).orElse(null);
        this.tableId = tablePath != null ? tablePath.toString() : null;
    }

    @Override
    public SeaTunnelRow deserialize(byte[] message) throws IOException {
        throw new UnsupportedOperationException(
                "Please invoke DeserializationSchema#deserialize(byte[], Collector<SeaTunnelRow>) instead.");
    }

    @Override
    public void deserialize(byte[] message, Collector<SeaTunnelRow> out) {
        deserializeMessage(message, out, tablePath);
    }

    public void deserializeMessage(
            byte[] message, Collector<SeaTunnelRow> out, TablePath tablePath) {
        if (message == null || message.length == 0) {
            // skip tombstone messages
            return;
        }

        try {
            // Only schema-enabled messages need per-field logical type metadata. Keep the
            // schema-less hot path free of metadata maps and continue skipping unused objects.
            Map<String, Map<String, String>> fieldSchemaNames =
                    debeziumEnabledSchema ? new HashMap<>() : Collections.emptyMap();
            JsonNode payload = readDebeziumPayload(message, fieldSchemaNames);
            parsePayload(
                    out,
                    tablePath,
                    payload,
                    fieldSchemaNames.getOrDefault(DATA_BEFORE, Collections.emptyMap()),
                    fieldSchemaNames.getOrDefault(DATA_AFTER, Collections.emptyMap()));
        } catch (Exception e) {
            // a big try catch to protect the processing.
            if (!ignoreParseErrors) {
                throw CommonError.jsonOperationError(FORMAT, new String(message), e);
            }
        }
    }

    public void parsePayload(Collector<SeaTunnelRow> out, JsonNode payload) throws IOException {
        parsePayload(out, tablePath, payload);
    }

    void parsePayload(Collector<SeaTunnelRow> out, JsonNode root, JsonNode payload)
            throws IOException {
        parsePayload(out, tablePath, root, payload);
    }

    void parsePayload(
            Collector<SeaTunnelRow> out, TablePath tablePath, JsonNode root, JsonNode payload)
            throws IOException {
        parsePayload(
                out,
                tablePath,
                payload,
                extractFieldSchemaNames(root, DATA_BEFORE),
                extractFieldSchemaNames(root, DATA_AFTER));
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return this.rowType;
    }

    private void parsePayload(Collector<SeaTunnelRow> out, TablePath tablePath, JsonNode payload)
            throws IOException {
        parsePayload(out, tablePath, payload, Collections.emptyMap(), Collections.emptyMap());
    }

    private void parsePayload(
            Collector<SeaTunnelRow> out,
            TablePath tablePath,
            JsonNode payload,
            Map<String, String> beforeFieldSchemaNames,
            Map<String, String> afterFieldSchemaNames)
            throws IOException {
        String op = payload.get(OP_KEY).asText();
        JsonNode tsNode = payload.get(DATA_TS);
        String resolvedTableId = resolveTableId(tablePath);
        Long eventTime = tsNode != null ? tsNode.asLong() : null;

        switch (op) {
            case OP_CREATE:
            case OP_READ:
                SeaTunnelRow insert =
                        debeziumRowConverter.parse(payload.get(DATA_AFTER), afterFieldSchemaNames);
                insert.setRowKind(RowKind.INSERT);
                applyRowMeta(insert, resolvedTableId, eventTime);
                out.collect(insert);
                break;
            case OP_UPDATE:
                SeaTunnelRow before =
                        debeziumRowConverter.parse(
                                payload.get(DATA_BEFORE), beforeFieldSchemaNames);
                if (before == null) {
                    throw new IllegalStateException(
                            String.format(REPLICA_IDENTITY_EXCEPTION, "UPDATE"));
                }
                before.setRowKind(RowKind.UPDATE_BEFORE);
                applyRowMeta(before, resolvedTableId, eventTime);

                SeaTunnelRow after =
                        debeziumRowConverter.parse(payload.get(DATA_AFTER), afterFieldSchemaNames);
                after.setRowKind(RowKind.UPDATE_AFTER);
                applyRowMeta(after, resolvedTableId, eventTime);
                out.collect(before);
                out.collect(after);
                break;
            case OP_DELETE:
                SeaTunnelRow delete =
                        debeziumRowConverter.parse(
                                payload.get(DATA_BEFORE), beforeFieldSchemaNames);
                if (delete == null) {
                    throw new IllegalStateException(
                            String.format(REPLICA_IDENTITY_EXCEPTION, "DELETE"));
                }
                delete.setRowKind(RowKind.DELETE);
                applyRowMeta(delete, resolvedTableId, eventTime);
                out.collect(delete);
                break;
            default:
                throw new IllegalStateException(format("Unknown operation type '%s'.", op));
        }
    }

    /**
     * Reads immediate column logical types from the selected before/after envelope schema.
     *
     * <p>The converter uses these names before its numeric TIME fallback. This flat map does not
     * traverse nested ROW schemas; missing logical names retain the compatibility fallback.
     */
    static Map<String, String> extractFieldSchemaNames(JsonNode root, String envelopeField) {
        JsonNode schema = root == null ? null : root.get(DATA_SCHEMA);
        JsonNode fields = schema == null ? null : schema.get(SCHEMA_FIELDS);
        if (fields == null || !fields.isArray()) {
            return Collections.emptyMap();
        }
        for (JsonNode field : fields) {
            if (!envelopeField.equals(getText(field, SCHEMA_FIELD))) {
                continue;
            }
            JsonNode envelopeFields = field.get(SCHEMA_FIELDS);
            if (envelopeFields == null || !envelopeFields.isArray()) {
                return Collections.emptyMap();
            }
            Map<String, String> fieldSchemaNames = new HashMap<>();
            Iterator<JsonNode> iterator = envelopeFields.elements();
            while (iterator.hasNext()) {
                JsonNode envelopeFieldSchema = iterator.next();
                String fieldName = getText(envelopeFieldSchema, SCHEMA_FIELD);
                String schemaName = getText(envelopeFieldSchema, SCHEMA_NAME);
                if (fieldName != null && schemaName != null) {
                    fieldSchemaNames.put(fieldName, schemaName);
                }
            }
            return fieldSchemaNames;
        }
        return Collections.emptyMap();
    }

    private static String getText(JsonNode node, String fieldName) {
        JsonNode value = node == null ? null : node.get(fieldName);
        return value == null || value.isNull() ? null : value.asText();
    }

    /**
     * Resolves the table id string for the emitted row. Call sites normally pass {@link
     * #tablePath}; the identity check reuses the cached {@link #tableId}, while the fallback covers
     * the public {@link #deserializeMessage(byte[], Collector, TablePath)} contract if a different
     * path is supplied.
     */
    private String resolveTableId(TablePath tablePath) {
        if (tablePath == null) {
            return null;
        }
        if (tablePath == this.tablePath) {
            return tableId;
        }
        return tablePath.toString();
    }

    /** Applies table id and optional event-time metadata to a newly parsed row. */
    private static void applyRowMeta(SeaTunnelRow row, String tableId, Long eventTime) {
        if (tableId != null) {
            row.setTableId(tableId);
        }
        if (eventTime != null) {
            MetadataUtil.setEventTime(row, eventTime);
        }
    }

    /**
     * Parses a Debezium JSON envelope while skipping unused objects such as {@code source}, {@code
     * transaction}. Only CDC payload fields are materialized as JsonNodes. With schema enabled,
     * retain the immediate before/after field logical names without building the schema tree;
     * schema and payload may occur in either order in the envelope.
     */
    private JsonNode readDebeziumPayload(
            byte[] message, Map<String, Map<String, String>> fieldSchemaNames) throws IOException {
        ObjectMapper objectMapper = jsonDeserializer.getObjectMapper();
        try (JsonParser parser = objectMapper.getFactory().createParser(message)) {
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                throw new IOException("Expected START_OBJECT for Debezium JSON message");
            }
            if (debeziumEnabledSchema) {
                JsonNode payload = null;
                while (parser.nextToken() != JsonToken.END_OBJECT) {
                    String fieldName = parser.currentName();
                    parser.nextToken();
                    if (DATA_PAYLOAD.equals(fieldName)) {
                        payload = readPayloadFields(objectMapper, parser);
                    } else if (DATA_SCHEMA.equals(fieldName)) {
                        fieldSchemaNames.clear();
                        readEnvelopeFieldSchemaNames(parser, fieldSchemaNames);
                    } else {
                        parser.skipChildren();
                    }
                }
                if (payload == null) {
                    throw new IOException("Missing payload field in schema-included Debezium JSON");
                }
                return payload;
            }
            return readPayloadFields(objectMapper, parser);
        }
    }

    /**
     * Reads only the immediate before/after schema descriptors. Schema attributes and nested
     * metadata unrelated to TIME conversion remain streaming skips rather than JsonNode trees.
     */
    private static void readEnvelopeFieldSchemaNames(
            JsonParser parser, Map<String, Map<String, String>> fieldSchemaNames)
            throws IOException {
        if (parser.currentToken() != JsonToken.START_OBJECT) {
            parser.skipChildren();
            return;
        }
        while (parser.nextToken() != JsonToken.END_OBJECT) {
            String name = parser.currentName();
            parser.nextToken();
            if (SCHEMA_FIELDS.equals(name)) {
                fieldSchemaNames.clear();
                if (parser.currentToken() == JsonToken.START_ARRAY) {
                    while (parser.nextToken() != JsonToken.END_ARRAY) {
                        readEnvelopeFieldSchema(parser, fieldSchemaNames);
                    }
                } else {
                    parser.skipChildren();
                }
            } else {
                parser.skipChildren();
            }
        }
    }

    /**
     * Preserves field-order independence, including descriptors whose fields precede field. Schema
     * JSON property order must not determine which logical type names reach the converter.
     */
    private static void readEnvelopeFieldSchema(
            JsonParser parser, Map<String, Map<String, String>> fieldSchemaNames)
            throws IOException {
        if (parser.currentToken() != JsonToken.START_OBJECT) {
            parser.skipChildren();
            return;
        }
        String envelopeField = null;
        Map<String, String> logicalNames = Collections.emptyMap();
        while (parser.nextToken() != JsonToken.END_OBJECT) {
            String name = parser.currentName();
            parser.nextToken();
            if (SCHEMA_FIELD.equals(name)) {
                envelopeField = parser.getValueAsString();
                parser.skipChildren();
            } else if (SCHEMA_FIELDS.equals(name)) {
                logicalNames = readFieldSchemaNames(parser);
            } else {
                parser.skipChildren();
            }
        }
        if ((DATA_BEFORE.equals(envelopeField) || DATA_AFTER.equals(envelopeField))
                && !fieldSchemaNames.containsKey(envelopeField)) {
            fieldSchemaNames.put(envelopeField, logicalNames);
        }
    }

    /**
     * Extracts only immediate field/name string pairs, matching the dispatcher tree path. Nested
     * column-schema objects are skipped because the converter retains the existing flat contract.
     */
    private static Map<String, String> readFieldSchemaNames(JsonParser parser) throws IOException {
        if (parser.currentToken() != JsonToken.START_ARRAY) {
            parser.skipChildren();
            return Collections.emptyMap();
        }
        Map<String, String> logicalNames = new HashMap<>();
        while (parser.nextToken() != JsonToken.END_ARRAY) {
            if (parser.currentToken() != JsonToken.START_OBJECT) {
                parser.skipChildren();
                continue;
            }
            String fieldName = null;
            String schemaName = null;
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                String name = parser.currentName();
                parser.nextToken();
                if (SCHEMA_FIELD.equals(name)) {
                    fieldName = parser.getValueAsString();
                } else if (SCHEMA_NAME.equals(name)) {
                    schemaName = parser.getValueAsString();
                }
                parser.skipChildren();
            }
            if (fieldName != null && schemaName != null) {
                logicalNames.put(fieldName, schemaName);
            }
        }
        return logicalNames;
    }

    /**
     * Reads the current object value from {@code parser}, keeping only CDC payload fields and
     * skipping everything else (notably {@code source} and {@code transaction}).
     */
    private static JsonNode readPayloadFields(ObjectMapper objectMapper, JsonParser parser)
            throws IOException {
        if (parser.currentToken() != JsonToken.START_OBJECT) {
            throw new IOException("Expected START_OBJECT for Debezium payload");
        }
        ObjectNode payload = objectMapper.createObjectNode();
        while (parser.nextToken() != JsonToken.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();
            if (DATA_BEFORE.equals(fieldName)
                    || DATA_AFTER.equals(fieldName)
                    || OP_KEY.equals(fieldName)
                    || DATA_TS.equals(fieldName)) {
                payload.set(fieldName, objectMapper.readTree(parser));
            } else {
                parser.skipChildren();
            }
        }
        return payload;
    }
}
