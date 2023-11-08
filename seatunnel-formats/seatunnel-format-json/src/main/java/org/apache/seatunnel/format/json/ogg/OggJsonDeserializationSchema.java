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

public class OggJsonDeserializationSchema implements DeserializationSchema<SeaTunnelRow> {

    private static final long serialVersionUID = 1L;

    private static final String FIELD_TYPE = "op_type";

    private static final String FIELD_DATABASE_TABLE = "table";

    private static final String DATA_BEFORE = "before"; // BEFORE

    private static final String DATA_AFTER = "after"; // AFTER

    private static final String OP_INSERT = "I"; // INSERT

    private static final String OP_UPDATE = "U"; // UPDATE

    private static final String OP_DELETE = "D"; // DELETE

    private static final String REPLICA_IDENTITY_EXCEPTION =
            "The \"before\" field of %s message is null, "
                    + "if you are using Ogg Postgres Connector, "
                    + "please check the Postgres table has been set REPLICA IDENTITY to FULL level.";

    private String database;

    private String table;

    /** Names of fields. */
    private final String[] fieldNames;

    /** Field number. */
    private final int fieldCount;

    private boolean ignoreParseErrors;

    /** Pattern of the specific database. */
    private final Pattern databasePattern;

    /** Pattern of the specific table. */
    private final Pattern tablePattern;

    private final JsonDeserializationSchema jsonDeserializer;

    private final SeaTunnelRowType physicalRowType;

    public OggJsonDeserializationSchema(
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
        throw new SeaTunnelJsonFormatException(
                CommonErrorCode.JSON_OPERATION_FAILED,
                String.format("Failed to deserialize JSON '%s'.", new String(message)));
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return this.physicalRowType;
    }

    public void deserialize(byte[] message, Collector<SeaTunnelRow> out) {
        if (message == null || message.length == 0) {
            // skip tombstone messages
            return;
        }
        ObjectNode jsonNode = (ObjectNode) convertBytes(message);
        assert jsonNode != null;

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
        if (OP_INSERT.equals(op)) {
            // Gets the data for the INSERT operation
            JsonNode dataBefore = jsonNode.get(DATA_AFTER);
            SeaTunnelRow row = convertJsonNode(dataBefore);
            out.collect(row);
        } else if (OP_UPDATE.equals(op)) {
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
            assert before != null;
            before.setRowKind(RowKind.UPDATE_BEFORE);
            out.collect(before);
            assert after != null;
            after.setRowKind(RowKind.UPDATE_AFTER);
            out.collect(after);

        } else if (OP_DELETE.equals(op)) {
            JsonNode dataBefore = jsonNode.get(DATA_BEFORE);
            if (dataBefore == null || dataBefore.isNull()) {
                throw new IllegalStateException(
                        String.format(REPLICA_IDENTITY_EXCEPTION, "DELETE"));
            }
            // Gets the data for the DELETE BEFORE operation
            SeaTunnelRow before = convertJsonNode(dataBefore);
            if (before == null) {
                throw new SeaTunnelJsonFormatException(
                        CommonErrorCode.JSON_OPERATION_FAILED,
                        format(
                                "The data %s the %s cannot be null \"%s\" ",
                                "BEFORE", "DELETE", new String(message)));
            }
            before.setRowKind(RowKind.DELETE);
            out.collect(before);
        } else {
            if (!ignoreParseErrors) {
                throw new SeaTunnelJsonFormatException(
                        CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        format(
                                "Unknown \"type\" value \"%s\". The Canal JSON message is '%s'",
                                op, new String(message)));
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
                    CommonErrorCode.JSON_OPERATION_FAILED,
                    String.format("Failed to deserialize JSON '%s'.", new String(message)),
                    t);
        }
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

        public OggJsonDeserializationSchema build() {
            return new OggJsonDeserializationSchema(
                    physicalDataType, database, table, ignoreParseErrors);
        }
    }
}
