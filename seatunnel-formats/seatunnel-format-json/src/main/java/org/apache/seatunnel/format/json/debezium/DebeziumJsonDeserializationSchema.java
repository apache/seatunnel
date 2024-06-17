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

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import java.io.IOException;
import java.util.Optional;

import static java.lang.String.format;

public class DebeziumJsonDeserializationSchema implements DeserializationSchema<SeaTunnelRow> {
    private static final long serialVersionUID = 1L;

    private static final String OP_READ = "r"; // snapshot read
    private static final String OP_CREATE = "c"; // insert
    private static final String OP_UPDATE = "u"; // update
    private static final String OP_DELETE = "d"; // delete

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

    private CatalogTable catalogTable;

    public DebeziumJsonDeserializationSchema(CatalogTable catalogTable, boolean ignoreParseErrors) {
        this.catalogTable = catalogTable;
        this.rowType = catalogTable.getSeaTunnelRowType();
        this.ignoreParseErrors = ignoreParseErrors;
        this.jsonDeserializer =
                new JsonDeserializationSchema(catalogTable, false, ignoreParseErrors);
        this.debeziumRowConverter = new DebeziumRowConverter(rowType);
        this.debeziumEnabledSchema = false;
    }

    public DebeziumJsonDeserializationSchema(
            CatalogTable catalogTable, boolean ignoreParseErrors, boolean debeziumEnabledSchema) {
        this.catalogTable = catalogTable;
        this.rowType = catalogTable.getSeaTunnelRowType();
        this.ignoreParseErrors = ignoreParseErrors;
        this.jsonDeserializer =
                new JsonDeserializationSchema(catalogTable, false, ignoreParseErrors);
        this.debeziumRowConverter = new DebeziumRowConverter(rowType);
        this.debeziumEnabledSchema = debeziumEnabledSchema;
        this.catalogTable = catalogTable;
    }

    @Override
    public SeaTunnelRow deserialize(byte[] message) throws IOException {
        throw new UnsupportedOperationException(
                "Please invoke DeserializationSchema#deserialize(byte[], Collector<SeaTunnelRow>) instead.");
    }

    @Override
    public void deserialize(byte[] message, Collector<SeaTunnelRow> out) {
        TablePath tablePath =
                Optional.ofNullable(catalogTable).map(CatalogTable::getTablePath).orElse(null);
        deserializeMessage(message, out, tablePath);
    }

    private void deserializeMessage(
            byte[] message, Collector<SeaTunnelRow> out, TablePath tablePath) {
        if (message == null || message.length == 0) {
            // skip tombstone messages
            return;
        }

        try {
            JsonNode payload = getPayload(convertBytes(message));
            String op = payload.get("op").asText();

            switch (op) {
                case OP_CREATE:
                case OP_READ:
                    SeaTunnelRow insert = convertJsonNode(payload.get("after"));
                    insert.setRowKind(RowKind.INSERT);
                    if (tablePath != null) {
                        insert.setTableId(tablePath.toString());
                    }
                    out.collect(insert);
                    break;
                case OP_UPDATE:
                    SeaTunnelRow before = convertJsonNode(payload.get("before"));
                    if (before == null) {
                        throw new IllegalStateException(
                                String.format(REPLICA_IDENTITY_EXCEPTION, "UPDATE"));
                    }
                    before.setRowKind(RowKind.UPDATE_BEFORE);
                    if (tablePath != null) {
                        before.setTableId(tablePath.toString());
                    }
                    out.collect(before);

                    SeaTunnelRow after = convertJsonNode(payload.get("after"));
                    after.setRowKind(RowKind.UPDATE_AFTER);

                    if (tablePath != null) {
                        after.setTableId(tablePath.toString());
                    }
                    out.collect(after);
                    break;
                case OP_DELETE:
                    SeaTunnelRow delete = convertJsonNode(payload.get("before"));
                    if (delete == null) {
                        throw new IllegalStateException(
                                String.format(REPLICA_IDENTITY_EXCEPTION, "UPDATE"));
                    }
                    delete.setRowKind(RowKind.DELETE);
                    if (tablePath != null) {
                        delete.setTableId(tablePath.toString());
                    }
                    out.collect(delete);
                    break;
                default:
                    throw new IllegalStateException(format("Unknown operation type '%s'.", op));
            }
        } catch (RuntimeException e) {
            // a big try catch to protect the processing.
            if (!ignoreParseErrors) {
                throw CommonError.jsonOperationError(FORMAT, new String(message), e);
            }
        }
    }

    private JsonNode getPayload(JsonNode jsonNode) {
        if (debeziumEnabledSchema) {
            return jsonNode.get("payload");
        }
        return jsonNode;
    }

    private JsonNode convertBytes(byte[] message) {
        try {
            return jsonDeserializer.deserializeToJsonNode(message);
        } catch (IOException t) {
            throw CommonError.jsonOperationError(FORMAT, new String(message), t);
        }
    }

    private SeaTunnelRow convertJsonNode(JsonNode root) {
        return debeziumRowConverter.serializeValue(root);
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return this.rowType;
    }

    private static SeaTunnelRowType createJsonRowType(SeaTunnelRowType databaseSchema) {
        return databaseSchema;
    }
}
