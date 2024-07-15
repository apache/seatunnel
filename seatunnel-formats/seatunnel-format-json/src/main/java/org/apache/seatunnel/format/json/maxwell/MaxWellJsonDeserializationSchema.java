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

package org.apache.seatunnel.format.json.maxwell;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;
import org.apache.seatunnel.format.json.exception.SeaTunnelJsonFormatException;

import java.io.IOException;
import java.util.regex.Pattern;

import static java.lang.String.format;

public class MaxWellJsonDeserializationSchema implements DeserializationSchema<SeaTunnelRow> {

    private static final long serialVersionUID = 1L;

    private static final String FIELD_OLD = "old";

    private static final String FIELD_DATA = "data";

    private static final String FIELD_TYPE = "type";

    private static final String OP_INSERT = "insert";

    private static final String OP_UPDATE = "update";

    private static final String OP_DELETE = "delete";

    private static final String FIELD_DATABASE = "database";

    private static final String FIELD_TABLE = "table";

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

    private final CatalogTable catalogTable;
    private final SeaTunnelRowType seaTunnelRowType;

    public MaxWellJsonDeserializationSchema(
            CatalogTable catalogTable, String database, String table, boolean ignoreParseErrors) {
        this.catalogTable = catalogTable;
        this.seaTunnelRowType = catalogTable.getSeaTunnelRowType();
        this.jsonDeserializer =
                new JsonDeserializationSchema(false, ignoreParseErrors, seaTunnelRowType);
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
        throw new UnsupportedOperationException();
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return this.seaTunnelRowType;
    }

    @Override
    public void deserialize(byte[] message, Collector<SeaTunnelRow> out) {
        if (message == null) {
            return;
        }
        ObjectNode jsonNode = (ObjectNode) convertBytes(message);
        if (database != null
                && !databasePattern.matcher(jsonNode.get(FIELD_DATABASE).asText()).matches()) {
            return;
        }
        if (table != null && !tablePattern.matcher(jsonNode.get(FIELD_TABLE).asText()).matches()) {
            return;
        }
        JsonNode dataNode = jsonNode.get(FIELD_DATA);
        String type = jsonNode.get(FIELD_TYPE).asText();
        if (OP_INSERT.equals(type)) {
            SeaTunnelRow rowInsert = convertJsonNode(dataNode);
            rowInsert.setRowKind(RowKind.INSERT);
            out.collect(rowInsert);
        } else if (OP_UPDATE.equals(type)) {
            SeaTunnelRow rowAfter = convertJsonNode(dataNode);
            JsonNode oldNode = jsonNode.get(FIELD_OLD);
            SeaTunnelRow rowBefore = convertJsonNode(oldNode);
            for (int f = 0; f < fieldCount; f++) {
                assert rowBefore != null;
                if (rowBefore.isNullAt(f) && oldNode.findValue(fieldNames[f]) == null) {
                    // fields in "old" (before) means the fields are changed
                    // fields not in "old" (before) means the fields are not changed
                    // so we just copy the not changed fields into before
                    assert rowAfter != null;
                    rowBefore.setField(f, rowAfter.getField(f));
                }
            }
            assert rowBefore != null;
            rowBefore.setRowKind(RowKind.UPDATE_BEFORE);
            assert rowAfter != null;
            rowAfter.setRowKind(RowKind.UPDATE_AFTER);
            out.collect(rowBefore);
            out.collect(rowAfter);
        } else if (OP_DELETE.equals(type)) {
            SeaTunnelRow rowDelete = convertJsonNode(dataNode);
            rowDelete.setRowKind(RowKind.DELETE);
            out.collect(rowDelete);
        } else {
            if (!ignoreParseErrors) {
                throw new SeaTunnelJsonFormatException(
                        CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        format(
                                "Unknown \"type\" value \"%s\". The MaxWell JSON message is '%s'",
                                type, new String(message)));
            }
        }
    }

    private JsonNode convertBytes(byte[] message) {
        try {
            return jsonDeserializer.deserializeToJsonNode(message);
        } catch (Exception t) {
            if (ignoreParseErrors) {
                return null;
            }
            throw new SeaTunnelJsonFormatException(
                    CommonErrorCode.CONVERT_TO_CONNECTOR_TYPE_ERROR_SIMPLE,
                    String.format("Failed to deserialize JSON '%s'.", new String(message)),
                    t);
        }
    }

    private SeaTunnelRow convertJsonNode(JsonNode root) {
        return jsonDeserializer.convertToRowData(root);
    }

    private static SeaTunnelRowType createJsonRowType(SeaTunnelRowType physicalDataType) {
        // MaxWell JSON contains other information, e.g. "ts", "sql", but we don't need them
        return physicalDataType;
    }

    // ------------------------------------------------------------------------------------------
    // Builder
    // ------------------------------------------------------------------------------------------

    /** Creates A builder for building a {@link MaxWellJsonDeserializationSchema}. */
    public static Builder builder(CatalogTable catalogTable) {
        return new Builder(catalogTable);
    }

    public static class Builder {

        private boolean ignoreParseErrors = false;

        private String database = null;

        private String table = null;

        private final CatalogTable catalogTable;

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

        public MaxWellJsonDeserializationSchema build() {
            return new MaxWellJsonDeserializationSchema(
                    catalogTable, database, table, ignoreParseErrors);
        }
    }
}
