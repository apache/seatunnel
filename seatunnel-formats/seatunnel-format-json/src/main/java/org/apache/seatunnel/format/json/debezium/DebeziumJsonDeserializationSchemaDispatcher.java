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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.utils.JsonUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

import static org.apache.seatunnel.format.json.debezium.DebeziumJsonDeserializationSchema.DATA_PAYLOAD;
import static org.apache.seatunnel.format.json.debezium.DebeziumJsonDeserializationSchema.FORMAT;

public class DebeziumJsonDeserializationSchemaDispatcher
        implements DeserializationSchema<SeaTunnelRow> {
    private static final long serialVersionUID = 1L;
    private static final Logger log =
            LoggerFactory.getLogger(DebeziumJsonDeserializationSchemaDispatcher.class);

    private final Map<TablePath, DebeziumJsonDeserializationSchema> tableDeserializationMap;
    private final boolean debeziumEnabledSchema;
    private boolean ignoreParseErrors;

    private static final String SOURCE = "source";
    private static final String TABLE = "table";
    private static final String SCHEMA = "schema";
    private static final String DATABASE = "db";
    private static final String CONNECTOR = "connector";

    public DebeziumJsonDeserializationSchemaDispatcher(
            Map<TablePath, DebeziumJsonDeserializationSchema> tableDeserializationMap,
            boolean ignoreParseErrors,
            boolean debeziumEnabledSchema) {
        this.tableDeserializationMap = tableDeserializationMap;
        this.debeziumEnabledSchema = debeziumEnabledSchema;
        this.ignoreParseErrors = ignoreParseErrors;
    }

    @Override
    public SeaTunnelRow deserialize(byte[] message) throws IOException {
        throw new UnsupportedOperationException(
                "Please invoke DeserializationSchema#deserialize(byte[], Collector<SeaTunnelRow>) instead.");
    }

    @Override
    public void deserialize(byte[] message, Collector<SeaTunnelRow> out) {
        if (message == null || message.length == 0) {
            // skip tombstone messages
            return;
        }

        try {
            JsonNode payload = getPayload(JsonUtils.readTree(message));
            JsonNode source = payload.get(SOURCE);
            String database = getNodeValue(source, DATABASE);
            String schema = getNodeValue(source, SCHEMA);
            String table = getNodeValue(source, TABLE);
            TablePath tablePath = TablePath.of(database, schema, table);
            if (tableDeserializationMap.containsKey(tablePath)) {
                tableDeserializationMap.get(tablePath).parsePayload(out, payload);
            } else {
                if (isConnectorCanWithOutDB(source.get(CONNECTOR))) {
                    tablePath = TablePath.of(null, schema, table);
                    if (tableDeserializationMap.containsKey(tablePath)) {
                        tableDeserializationMap.get(tablePath).parsePayload(out, payload);
                        return;
                    }
                }
                log.debug("Unsupported table path {}, just skip.", tablePath);
            }

        } catch (Exception e) {
            // a big try catch to protect the processing.
            if (!ignoreParseErrors) {
                throw CommonError.jsonOperationError(FORMAT, new String(message), e);
            }
        }
    }

    private static String getNodeValue(JsonNode source, String key) {
        return source.has(key) && !source.get(key).isNull() ? source.get(key).asText() : null;
    }

    private JsonNode getPayload(JsonNode jsonNode) {
        if (debeziumEnabledSchema) {
            return jsonNode.get(DATA_PAYLOAD);
        }
        return jsonNode;
    }

    private boolean isConnectorCanWithOutDB(JsonNode connectorNode) {
        if (connectorNode == null || connectorNode.isNull()) {
            return true;
        }
        String connector = connectorNode.asText().toLowerCase(Locale.ROOT);
        return connector.equals("oracle") || connector.equals("dameng");
    }

    @VisibleForTesting
    public Map<TablePath, DebeziumJsonDeserializationSchema> getTableDeserializationMap() {
        return tableDeserializationMap;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        throw new UnsupportedOperationException("Unreachable method.");
    }
}
