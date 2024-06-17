/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.format.json.ogg;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import lombok.NonNull;

import java.io.IOException;
import java.util.Optional;
import java.util.regex.Pattern;

public class OggJsonDeserializationSchema implements DeserializationSchema<SeaTunnelRow> {

    private static final long serialVersionUID = 1L;

    private static final String FORMAT = "Ogg";

    private static final String FIELD_TYPE = "op_type";

    private static final String FIELD_DATABASE_TABLE = "table";

    private static final String DATA_BEFORE = "before"; // BEFORE

    private static final String DATA_AFTER = "after"; // AFTER

    private static final String OP_INSERT = "I"; // INSERT

    private static final String OP_UPDATE = "U"; // UPDATE

    private static final String OP_DELETE = "D"; // DELETE

    private static final String REPLICA_IDENTITY_EXCEPTION =
            "The \"before\" field of %s operation message is null, "
                    + "if you are using Ogg Postgres Connector, "
                    + "please check the Postgres table has been set REPLICA IDENTITY to FULL level.";

    private final String database;

    private final String table;

    /** Names of fields. */
    private final String[] fieldNames;

    /** Field number. */
    private final int fieldCount;

    private final boolean ignoreParseErrors;

    /** Pattern of the specific database. */
    private final Pattern databasePattern;

    /** Pattern of the specific table. */
    private final Pattern tablePattern;

    private final JsonDeserializationSchema jsonDeserializer;

    private final SeaTunnelRowType seaTunnelRowType;

    private final CatalogTable catalogTable;

    public OggJsonDeserializationSchema(
            @NonNull CatalogTable catalogTable,
            String database,
            String table,
            boolean ignoreParseErrors) {
        this.catalogTable = catalogTable;
        this.seaTunnelRowType = catalogTable.getSeaTunnelRowType();
        this.jsonDeserializer =
                new JsonDeserializationSchema(catalogTable, false, ignoreParseErrors);
        this.database = database;
        this.table = table;
        this.fieldNames = seaTunnelRowType.getFieldNames();
        this.fieldCount = seaTunnelRowType.getTotalFields();
        this.ignoreParseErrors = ignoreParseErrors;
        this.databasePattern = database == null ? null : Pattern.compile(database);
        this.tablePattern = table == null ? null : Pattern.compile(table);
    }

    @Override
    public SeaTunnelRow deserialize(byte[] message) throws IOException {
        throw new UnsupportedOperationException(
                "Please invoke DeserializationSchema#deserialize(byte[], Collector<SeaTunnelRow>) instead.");
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return this.seaTunnelRowType;
    }

    public void deserializeMessage(
            byte[] message, Collector<SeaTunnelRow> out, TablePath tablePath) {

        if (message == null || message.length == 0) {
            // skip tombstone messages
            return;
        }

        ObjectNode jsonNode;
        try {
            jsonNode = convertBytes(message);
        } catch (RuntimeException e) {
            if (!ignoreParseErrors) {
                throw e;
            } else {
                return;
            }
        }

        try {
            if (database != null
                    && !databasePattern
                            .matcher(jsonNode.get(FIELD_DATABASE_TABLE).asText().split("\\.")[0])
                            .matches()) {
                return;
            }
            if (table != null
                    && !tablePattern
                            .matcher(jsonNode.get(FIELD_DATABASE_TABLE).asText().split("\\.")[1])
                            .matches()) {
                return;
            }

            String op = jsonNode.get(FIELD_TYPE).asText().trim();

            switch (op) {
                case OP_INSERT:
                    // Gets the data for the INSERT operation
                    JsonNode dataInsert = jsonNode.get(DATA_AFTER);
                    SeaTunnelRow row = convertJsonNode(dataInsert);
                    if (tablePath != null) {
                        row.setTableId(tablePath.toString());
                    }
                    out.collect(row);
                    break;
                case OP_UPDATE:
                    JsonNode dataBefore = jsonNode.get(DATA_BEFORE);
                    // Modify Operation Data cannot be empty before modification
                    if (dataBefore == null || dataBefore.isNull()) {
                        throw new IllegalStateException(
                                String.format(REPLICA_IDENTITY_EXCEPTION, "UPDATE"));
                    }
                    JsonNode dataAfter = jsonNode.get(DATA_AFTER);
                    // Gets the data for the UPDATE BEFORE operation
                    SeaTunnelRow before = convertJsonNode(dataBefore);
                    // Gets the data for the UPDATE AFTER operation
                    SeaTunnelRow after = convertJsonNode(dataAfter);
                    before.setRowKind(RowKind.UPDATE_BEFORE);
                    if (tablePath != null) {
                        before.setTableId(tablePath.toString());
                    }
                    out.collect(before);

                    after.setRowKind(RowKind.UPDATE_AFTER);
                    if (tablePath != null) {
                        after.setTableId(tablePath.toString());
                    }
                    out.collect(after);
                    break;
                case OP_DELETE:
                    JsonNode dataBeforeDel = jsonNode.get(DATA_BEFORE);
                    if (dataBeforeDel == null || dataBeforeDel.isNull()) {
                        throw new IllegalStateException(
                                String.format(REPLICA_IDENTITY_EXCEPTION, "DELETE"));
                    }
                    // Gets the data for the DELETE BEFORE operation
                    SeaTunnelRow beforeDelete = convertJsonNode(dataBeforeDel);
                    if (beforeDelete == null) {
                        throw new IllegalStateException(
                                String.format(REPLICA_IDENTITY_EXCEPTION, "DELETE"));
                    }
                    beforeDelete.setRowKind(RowKind.DELETE);
                    if (tablePath != null) {
                        beforeDelete.setTableId(tablePath.toString());
                    }
                    out.collect(beforeDelete);
                    break;
                default:
                    throw new IllegalStateException(
                            String.format("Unknown operation type '%s'.", op));
            }

        } catch (RuntimeException e) {
            if (!ignoreParseErrors) {
                throw CommonError.jsonOperationError(FORMAT, jsonNode.toString(), e);
            }
        }
    }

    private ObjectNode convertBytes(byte[] message) throws SeaTunnelRuntimeException {
        try {
            return (ObjectNode) jsonDeserializer.deserializeToJsonNode(message);
        } catch (Throwable t) {
            throw CommonError.jsonOperationError(FORMAT, new String(message), t);
        }
    }

    @Override
    public void deserialize(byte[] message, Collector<SeaTunnelRow> out) {
        TablePath tablePath =
                Optional.ofNullable(catalogTable).map(CatalogTable::getTablePath).orElse(null);
        deserializeMessage(message, out, tablePath);
    }

    private SeaTunnelRow convertJsonNode(JsonNode root) {
        return jsonDeserializer.convertToRowData(root);
    }

    private static SeaTunnelRowType createJsonRowType(SeaTunnelRowType physicalDataType) {
        // Ogg JSON contains other information, e.g. "ts", "sql", but we don't need them
        return physicalDataType;
    }

    // ------------------------------------------------------------------------------------------
    // Builder
    // ------------------------------------------------------------------------------------------

    /** Creates A builder for building a {@link OggJsonDeserializationSchema}. */
    public static Builder builder(CatalogTable catalogTable) {
        return new Builder(catalogTable);
    }

    public static class Builder {

        private boolean ignoreParseErrors = false;

        private String database = null;

        private String table = null;

        private CatalogTable catalogTable;

        public Builder(CatalogTable catalogTable) {
            this.catalogTable = catalogTable;
        }

        public Builder setDatabase(String database) {
            this.database = database;
            return this;
        }

        public Builder setTable(String table) {
            this.table = table;
            return this;
        }

        public Builder setIgnoreParseErrors(boolean ignoreParseErrors) {
            this.ignoreParseErrors = ignoreParseErrors;
            return this;
        }

        public OggJsonDeserializationSchema build() {
            return new OggJsonDeserializationSchema(
                    catalogTable, database, table, ignoreParseErrors);
        }
    }
}
