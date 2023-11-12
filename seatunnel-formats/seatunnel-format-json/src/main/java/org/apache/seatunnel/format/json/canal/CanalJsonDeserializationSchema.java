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
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;
import org.apache.seatunnel.format.json.exception.SeaTunnelJsonFormatException;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import static java.lang.String.format;

public class CanalJsonDeserializationSchema implements DeserializationSchema<SeaTunnelRow> {

    private static final long serialVersionUID = 1L;

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

    private String database;

    private String table;

    /** Names of fields. */
    private final String[] fieldNames;

    /** Number of fields. */
    private final int fieldCount;

    private boolean ignoreParseErrors;

    /** Pattern of the specific database. */
    private final Pattern databasePattern;

    /** Pattern of the specific table. */
    private final Pattern tablePattern;

    private final JsonDeserializationSchema jsonDeserializer;

    private final SeaTunnelRowType physicalRowType;

    public CanalJsonDeserializationSchema(
            SeaTunnelRowType physicalRowType,
            String database,
            String table,
            boolean ignoreParseErrors) {
        this.physicalRowType = physicalRowType;
        final SeaTunnelRowType jsonRowType = createJsonRowType(physicalRowType);
        this.jsonDeserializer =
                new JsonDeserializationSchema(false, ignoreParseErrors, jsonRowType);
        this.database = database;
        this.table = table;
        this.fieldNames = physicalRowType.getFieldNames();
        this.fieldCount = physicalRowType.getTotalFields();
        this.ignoreParseErrors = ignoreParseErrors;
        this.databasePattern = database == null ? null : Pattern.compile(database);
        this.tablePattern = table == null ? null : Pattern.compile(table);
    }

    @Override
    public List<SeaTunnelRow> deserialize(byte[] message) throws IOException {
        if (message == null) {
            return Collections.emptyList();
        }
        ObjectNode jsonNode = (ObjectNode) convertBytes(message);
        Preconditions.checkNotNull(jsonNode);
        return deserialize(jsonNode);
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return this.physicalRowType;
    }

    public List<SeaTunnelRow> deserialize(ObjectNode jsonNode) {
        if (database != null
                && !databasePattern.matcher(jsonNode.get(FIELD_DATABASE).asText()).matches()) {
            return Collections.emptyList();
        }
        if (table != null && !tablePattern.matcher(jsonNode.get(FIELD_TABLE).asText()).matches()) {
            return Collections.emptyList();
        }
        JsonNode dataNode = jsonNode.get(FIELD_DATA);
        String type = jsonNode.get(FIELD_TYPE).asText();
        // When a null value is encountered, an exception needs to be thrown for easy sensing
        if (dataNode == null || dataNode.isNull()) {
            // We'll skip the query or create or alter event data
            if (OP_QUERY.equals(type) || OP_CREATE.equals(type) || OP_ALTER.equals(type)) {
                return Collections.emptyList();
            }
            throw new SeaTunnelJsonFormatException(
                    CommonErrorCodeDeprecated.JSON_OPERATION_FAILED,
                    format("Null data value \"%s\" Cannot send downstream", jsonNode));
        }
        if (OP_INSERT.equals(type)) {
            List<SeaTunnelRow> seaTunnelRows = new ArrayList<>();
            for (int i = 0; i < dataNode.size(); i++) {
                SeaTunnelRow row = convertJsonNode(dataNode.get(i));
                seaTunnelRows.add(row);
            }
            return seaTunnelRows;
        } else if (OP_UPDATE.equals(type)) {
            final ArrayNode oldNode = (ArrayNode) jsonNode.get(FIELD_OLD);
            List<SeaTunnelRow> seaTunnelRows = new ArrayList<>();
            for (int i = 0; i < dataNode.size(); i++) {
                SeaTunnelRow after = convertJsonNode(dataNode.get(i));
                SeaTunnelRow before = convertJsonNode(oldNode.get(i));
                for (int f = 0; f < fieldCount; f++) {
                    assert before != null;
                    if (before.isNullAt(f) && oldNode.findValue(fieldNames[f]) == null) {
                        // fields in "old" (before) means the fields are changed
                        // fields not in "old" (before) means the fields are not changed
                        // so we just copy the not changed fields into before
                        assert after != null;
                        before.setField(f, after.getField(f));
                    }
                }
                assert before != null;
                before.setRowKind(RowKind.UPDATE_BEFORE);
                assert after != null;
                after.setRowKind(RowKind.UPDATE_AFTER);
                seaTunnelRows.add(before);
                seaTunnelRows.add(after);
            }
            return seaTunnelRows;
        } else if (OP_DELETE.equals(type)) {
            List<SeaTunnelRow> seaTunnelRows = new ArrayList<>();
            for (int i = 0; i < dataNode.size(); i++) {
                SeaTunnelRow row = convertJsonNode(dataNode.get(i));
                assert row != null;
                row.setRowKind(RowKind.DELETE);
                seaTunnelRows.add(row);
            }
            return seaTunnelRows;
        } else {
            if (!ignoreParseErrors) {
                throw new SeaTunnelJsonFormatException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                        format(
                                "Unknown \"type\" value \"%s\". The Canal JSON message is '%s'",
                                type, jsonNode.asText()));
            }
        }
        return Collections.emptyList();
    }

    private JsonNode convertBytes(byte[] message) {
        try {
            return jsonDeserializer.deserializeToJsonNode(message);
        } catch (Exception t) {
            if (ignoreParseErrors) {
                return null;
            }
            throw new SeaTunnelJsonFormatException(
                    CommonErrorCodeDeprecated.JSON_OPERATION_FAILED,
                    String.format("Failed to deserialize JSON '%s'.", new String(message)),
                    t);
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
    public static Builder builder(SeaTunnelRowType physicalDataType) {
        return new Builder(physicalDataType);
    }

    public static class Builder {

        private boolean ignoreParseErrors = false;

        private String database = null;

        private String table = null;

        private final SeaTunnelRowType physicalDataType;

        public Builder(SeaTunnelRowType physicalDataType) {
            this.physicalDataType = physicalDataType;
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

        public CanalJsonDeserializationSchema build() {
            return new CanalJsonDeserializationSchema(
                    physicalDataType, database, table, ignoreParseErrors);
        }
    }
}
