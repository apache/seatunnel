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
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;
import org.apache.seatunnel.format.json.exception.SeaTunnelJsonFormatException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

public class CanalJsonDeserializationSchema implements DeserializationSchema<SeaTunnelRow> {

    private static final long serialVersionUID = 1L;

    private static final String FIELD_OLD = "old";

    private static final String FIELD_DATA = "data";

    private static final String FIELD_TYPE = "type";

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

    private final SeaTunnelRowType physicalRowType;

    public CanalJsonDeserializationSchema(SeaTunnelRowType physicalRowType,
                                          String database,
                                          String table,
                                          boolean ignoreParseErrors
                                          ) {
        this.physicalRowType = physicalRowType;
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
        if (message == null) {
            return null;
        }
        return convertJsonNode(convertBytes(message));
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return this.physicalRowType;
    }

    public void deserialize(byte[] message, Collector<SeaTunnelRow> out) throws IOException {
        if (message == null) {
            return;
        }
        List<JsonNode> roots = convertBytesToList(message);
        Objects.requireNonNull(roots).forEach(root->{
            SeaTunnelRow row = convertJsonNode(root);

            if (!Optional.ofNullable(row).isPresent()){
                return;
            }
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
            } else {
                if (!ignoreParseErrors) {
                    throw new SeaTunnelJsonFormatException(CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        format(
                            "Unknown \"type\" value \"%s\". The Canal JSON message is '%s'",
                            type, new String(message)));
                }
            }
        });
    }

    private List<JsonNode> convertBytesToList(byte[] message) {
        try {
            ObjectNode jsonNode = (ObjectNode)convertBytes(message);
            assert jsonNode != null;
            JsonNode data = jsonNode.get(FIELD_DATA);
            JsonNode old = null;
            if (OP_UPDATE.equals(jsonNode.get(FIELD_TYPE).asText())){
                old = jsonNode.get(FIELD_OLD);
            }
            List<JsonNode> resultList = Lists.newArrayList();
            for (int i = 0; i < data.size(); i++) {
                JsonNode jsonNode1 = data.get(i);
                if (OP_UPDATE.equals(jsonNode.get(FIELD_TYPE).asText())){
                    JsonNode jsonNode2 = Objects.requireNonNull(old).get(i);
                    jsonNode.replace(FIELD_OLD, jsonNode2);
                }
                jsonNode.replace(FIELD_DATA, jsonNode1);
                resultList.add(jsonNode);
            }
            return resultList;
        } catch (Throwable t) {
            if (ignoreParseErrors) {
                return null;
            }
            throw new SeaTunnelJsonFormatException(CommonErrorCode.JSON_OPERATION_FAILED,
                String.format("Failed to deserialize JSON '%s'.", new String(message)), t);
        }
    }

    private JsonNode convertBytes(byte[] message) {
        try {
            return jsonDeserializer.deserializeToJsonNode(message);
        } catch (Throwable t) {
            if (ignoreParseErrors) {
                return null;
            }
            throw new SeaTunnelJsonFormatException(CommonErrorCode.JSON_OPERATION_FAILED,
                String.format("Failed to deserialize JSON '%s'.", new String(message)), t);
        }
    }

    private SeaTunnelRow convertJsonNode(JsonNode root) {
        if (database != null) {
            if (!databasePattern
                .matcher(root.get("database").asText())
                .matches()) {
                return null;
            }
        }
        if (table != null) {
            if (!tablePattern
                .matcher(root.get("table").asText())
                .matches()) {
                return null;
            }
        }
        return jsonDeserializer.convertToRowData(root);
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
