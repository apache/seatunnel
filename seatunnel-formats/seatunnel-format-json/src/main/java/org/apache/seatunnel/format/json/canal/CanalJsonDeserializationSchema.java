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

package org.apache.seatunnel.format.json.canal;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
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

import static java.lang.String.format;

public class CanalJsonDeserializationSchema implements DeserializationSchema<SeaTunnelRow> {
    private static final long serialVersionUID = 1L;

    private static final String FORMAT = "Canal";

    private static final String FIELD_OLD = "old";

    private static final String FIELD_DATA = "data";

    private static final String FIELD_TYPE = "type";

    private static final String FIELD_DATABASE = "database";

    private static final String FIELD_TABLE = "table";

    private static final String OP_INSERT = "INSERT";

    private static final String OP_UPDATE = "UPDATE";

    private static final String OP_DELETE = "DELETE";

    private static final String OP_CREATE = "CREATE";

    private static final String OP_QUERY = "QUERY";

    private static final String OP_ALTER = "ALTER";

    private final String database;

    private final String table;

    /** Names of fields. */
    private final String[] fieldNames;

    /** Number of fields. */
    private final int fieldCount;

    private final boolean ignoreParseErrors;

    /** Pattern of the specific database. */
    private final Pattern databasePattern;

    /** Pattern of the specific table. */
    private final Pattern tablePattern;

    private final JsonDeserializationSchema jsonDeserializer;

    private final SeaTunnelRowType seaTunnelRowType;
    private final CatalogTable catalogTable;

    public CanalJsonDeserializationSchema(
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

    public void deserialize(ObjectNode jsonNode, Collector<SeaTunnelRow> out) throws IOException {
        TablePath tablePath =
                Optional.ofNullable(catalogTable).map(CatalogTable::getTablePath).orElse(null);

        try {
            if (database != null
                    && !databasePattern.matcher(jsonNode.get(FIELD_DATABASE).asText()).matches()) {
                return;
            }
            if (table != null
                    && !tablePattern.matcher(jsonNode.get(FIELD_TABLE).asText()).matches()) {
                return;
            }

            JsonNode dataNode = jsonNode.get(FIELD_DATA);
            String op = jsonNode.get(FIELD_TYPE).asText();
            // When a null value is encountered, an exception needs to be thrown for easy sensing
            if (dataNode == null || dataNode.isNull()) {
                // We'll skip the query or create or alter event data
                if (OP_QUERY.equals(op) || OP_CREATE.equals(op) || OP_ALTER.equals(op)) {
                    return;
                }
                throw new IllegalStateException(
                        format("Null data value '%s' Cannot send downstream", jsonNode));
            }

            switch (op) {
                case OP_INSERT:
                    for (int i = 0; i < dataNode.size(); i++) {
                        SeaTunnelRow row = convertJsonNode(dataNode.get(i));
                        if (tablePath != null && !tablePath.toString().isEmpty()) {
                            row.setTableId(tablePath.toString());
                        }
                        out.collect(row);
                    }
                    break;
                case OP_UPDATE:
                    final ArrayNode oldNode = (ArrayNode) jsonNode.get(FIELD_OLD);
                    for (int i = 0; i < dataNode.size(); i++) {
                        SeaTunnelRow after = convertJsonNode(dataNode.get(i));
                        SeaTunnelRow before = convertJsonNode(oldNode.get(i));
                        for (int f = 0; f < fieldCount; f++) {
                            if (before.isNullAt(f) && oldNode.findValue(fieldNames[f]) == null) {
                                // fields in "old" (before) means the fields are changed
                                // fields not in "old" (before) means the fields are not changed
                                // so we just copy the not changed fields into before
                                before.setField(f, after.getField(f));
                            }
                        }
                        before.setRowKind(RowKind.UPDATE_BEFORE);
                        if (tablePath != null && !tablePath.toString().isEmpty()) {
                            before.setTableId(tablePath.toString());
                        }
                        after.setRowKind(RowKind.UPDATE_AFTER);
                        if (tablePath != null && !tablePath.toString().isEmpty()) {
                            after.setTableId(tablePath.toString());
                        }
                        out.collect(before);
                        out.collect(after);
                    }
                    break;
                case OP_DELETE:
                    for (int i = 0; i < dataNode.size(); i++) {
                        SeaTunnelRow row = convertJsonNode(dataNode.get(i));
                        row.setRowKind(RowKind.DELETE);
                        if (tablePath != null && !tablePath.toString().isEmpty()) {
                            row.setTableId(tablePath.toString());
                        }
                        out.collect(row);
                    }
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
        if (message == null || message.length == 0) {
            return null;
        }

        try {
            return (ObjectNode) jsonDeserializer.deserializeToJsonNode(message);
        } catch (Throwable t) {
            if (!ignoreParseErrors) {
                throw CommonError.jsonOperationError(FORMAT, new String(message), t);
            }
            return null;
        }
    }

    @Override
    public void deserialize(byte[] message, Collector<SeaTunnelRow> out) throws IOException {
        ObjectNode jsonNodes = convertBytes(message);
        if (jsonNodes != null) {
            deserialize(convertBytes(message), out);
        }
    }

    private SeaTunnelRow convertJsonNode(JsonNode root) {
        return jsonDeserializer.convertToRowData(root);
    }

    private static SeaTunnelRowType createJsonRowType(SeaTunnelRowType physicalDataType) {
        // Canal JSON contains other information, e.g. "ts", "sql", but we don't need them
        return physicalDataType;
    }

    // ------------------------------------------------------------------------------------------
    // Builder
    // ------------------------------------------------------------------------------------------

    /** Creates A builder for building a {@link CanalJsonDeserializationSchema}. */
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

        public Builder setCatalogTable(CatalogTable catalogTable) {
            this.catalogTable = catalogTable;
            return this;
        }

        public CanalJsonDeserializationSchema build() {
            return new CanalJsonDeserializationSchema(
                    catalogTable, database, table, ignoreParseErrors);
        }
    }
}
