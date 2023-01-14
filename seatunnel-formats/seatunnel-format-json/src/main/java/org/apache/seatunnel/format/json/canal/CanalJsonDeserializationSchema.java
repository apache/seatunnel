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

import static org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE;
import static java.lang.String.format;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.regex.Pattern;

public class CanalJsonDeserializationSchema implements DeserializationSchema<SeaTunnelRow> {

    private static final long serialVersionUID = 1L;

    private static final String FIELD_OLD = "old";

    private static final String OP_INSERT = "INSERT";

    private static final String OP_UPDATE = "UPDATE";

    private static final String OP_DELETE = "DELETE";

    private static final String OP_CREATE = "CREATE";

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

    public CanalJsonDeserializationSchema(SeaTunnelRowType physicalRowType,
                                          String database,
                                          String table,
                                          boolean ignoreParseErrors
                                          ) {
        final SeaTunnelRowType jsonRowType = createJsonRowType(physicalRowType);
        this.jsonDeserializer = new JsonDeserializationSchema(
            false,
            ignoreParseErrors,
            jsonRowType
        );
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
        return null;
    }

    @Override
    public void deserialize(byte[] message, Collector<SeaTunnelRow> out) throws IOException {
        if (message == null || message.length == 0) {
            return;
        }
        try {
            final JsonNode root = jsonDeserializer.deserializeToJsonNode(message);
            if (database != null) {
                if (!databasePattern
                    .matcher(root.get("database").asText())
                    .matches()) {
                    return;
                }
            }
            if (table != null) {
                if (!tablePattern
                    .matcher(root.get("table").asText())
                    .matches()) {
                    return;
                }
            }
            SeaTunnelRow row = jsonDeserializer.convertToRowData(root);
            SeaTunnelRow data = (SeaTunnelRow) row.getField(0);
            String type = row.getField(2).toString();
            if (OP_INSERT.equals(type)) {
                out.collect(row);
            } else if (OP_UPDATE.equals(type)) {
                SeaTunnelRow after = (SeaTunnelRow) row.getField(0);
                SeaTunnelRow before = (SeaTunnelRow) row.getField(1);
                final JsonNode oldField = root.get(FIELD_OLD);
                for (int f = 0; f < fieldCount; f++) {
                    if (before.isNullAt(f) && oldField.findValue(fieldNames[f]) == null) {
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
            } else if (OP_DELETE.equals(type)) {
                // "data" field is an array of row, contains deleted rows
                data.setRowKind(RowKind.DELETE);
                out.collect(data);
            } else if (OP_CREATE.equals(type)) {
                // "data" field is null and "type" is "CREATE" which means
                // this is a DDL change event, and we should skip it.
                return;
            } else {
                if (!ignoreParseErrors) {
                    throw new IOException(
                        format(
                            "Unknown \"type\" value \"%s\". The Canal JSON message is '%s'",
                            type, new String(message)));
                }
            }

        } catch (Throwable t) {
            // a big try catch to protect the processing.
            if (!ignoreParseErrors) {
                throw new IOException(
                    format("Corrupt Canal JSON message '%s'.", new String(message)), t);
            }
        }
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return null;
    }

    private static SeaTunnelRowType createJsonRowType(
        SeaTunnelRowType physicalDataType) {
        // Canal JSON contains other information, e.g. "ts", "sql", but we don't need them
        SeaTunnelRowType root =
            new SeaTunnelRowType(new String[]{"data", "old", "type", "database", "table"}, new SeaTunnelDataType[]{physicalDataType, physicalDataType, STRING_TYPE, STRING_TYPE, STRING_TYPE});
        return root;
    }

    // ------------------------------------------------------------------------------------------
    // Builder
    // ------------------------------------------------------------------------------------------

    /** Creates A builder for building a {@link CanalJsonDeserializationSchema}. */
    public static Builder builder(
        SeaTunnelRowType physicalDataType) {
        return new Builder(physicalDataType);
    }

    public static class Builder{

        private boolean ignoreParseErrors = false;

        private String database = null;

        private String table = null;

        private final SeaTunnelRowType physicalDataType;

        public Builder(SeaTunnelRowType physicalDataType){
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

        public CanalJsonDeserializationSchema build(){
            return new CanalJsonDeserializationSchema(
                physicalDataType,
                database,
                table,
                ignoreParseErrors
                );
        }
    }
}
