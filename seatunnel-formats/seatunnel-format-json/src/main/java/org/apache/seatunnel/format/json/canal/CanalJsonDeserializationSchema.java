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
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import java.io.IOException;
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
    public SeaTunnelRow deserialize(byte[] message) throws IOException {
        throw new UnsupportedOperationException(
                "Please invoke DeserializationSchema#deserialize(byte[], Collector<SeaTunnelRow>) instead.");
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return this.physicalRowType;
    }

    public void deserialize(ObjectNode jsonNode, Collector<SeaTunnelRow> out) throws IOException {
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
            String type = jsonNode.get(FIELD_TYPE).asText();
            // When a null value is encountered, an exception needs to be thrown for easy sensing
            if (dataNode == null || dataNode.isNull()) {
                // We'll skip the query or create or alter event data
                if (OP_QUERY.equals(type) || OP_CREATE.equals(type) || OP_ALTER.equals(type)) {
                    return;
                }
                throw new IllegalStateException(
                        format("Null data value '%s' Cannot send downstream", jsonNode));
            }
            if (OP_INSERT.equals(type)) {
                for (int i = 0; i < dataNode.size(); i++) {
                    SeaTunnelRow row = convertJsonNode(dataNode.get(i));
                    out.collect(row);
                }
            } else if (OP_UPDATE.equals(type)) {
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
                    after.setRowKind(RowKind.UPDATE_AFTER);
                    out.collect(before);
                    out.collect(after);
                }
            } else if (OP_DELETE.equals(type)) {
                for (int i = 0; i < dataNode.size(); i++) {
                    SeaTunnelRow row = convertJsonNode(dataNode.get(i));
                    row.setRowKind(RowKind.DELETE);
                    out.collect(row);
                }
            } else {
                throw new IllegalStateException(format("Unknown operation type '%s'.", type));
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
    public void deserialize(byte[] message, Collector<SeaTunnelRow> out) throws IOException {
        if (message == null) {
            return;
        }

        ObjectNode jsonNode;
        try {
            jsonNode = convertBytes(message);
        } catch (SeaTunnelRuntimeException cause) {
            if (!ignoreParseErrors) {
                throw cause;
            } else {
                return;
            }
        }

        deserialize(jsonNode, out);
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
