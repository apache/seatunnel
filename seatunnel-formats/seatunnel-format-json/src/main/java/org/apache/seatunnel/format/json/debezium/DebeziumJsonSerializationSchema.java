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

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.format.json.JsonSerializationSchema;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import static org.apache.seatunnel.api.table.type.BasicType.LONG_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE;
import static org.apache.seatunnel.api.table.type.CommonOptions.EVENT_TIME;
import static org.apache.seatunnel.format.json.debezium.DebeziumJsonFormatOptions.GENERATE_ROW_SIZE;

public class DebeziumJsonSerializationSchema implements SerializationSchema {
    private static final long serialVersionUID = 1L;

    private static final String OP_INSERT = "c"; // insert
    private static final String OP_DELETE = "d"; // delete
    private static final String OP_UPDATE = "u"; // update
    public static final String FORMAT = "Debezium";

    private final JsonSerializationSchema jsonSerializer;

    private transient SeaTunnelRow genericRow;

    boolean mergeUpdateEventFlag;
    SeaTunnelRow cacheUpdateBeforeRow;

    public DebeziumJsonSerializationSchema(SeaTunnelRowType rowType) {
        this.jsonSerializer = new JsonSerializationSchema(createJsonRowType(rowType));
        this.genericRow = new SeaTunnelRow(GENERATE_ROW_SIZE);
        this.mergeUpdateEventFlag = false;
    }

    public DebeziumJsonSerializationSchema(
            SeaTunnelRowType rowType, Charset charset, boolean mergeUpdateEventFlag) {
        this.jsonSerializer = new JsonSerializationSchema(createJsonRowType(rowType), charset);
        this.genericRow = new SeaTunnelRow(GENERATE_ROW_SIZE);
        this.mergeUpdateEventFlag = mergeUpdateEventFlag;
    }

    @Override
    public byte[] serialize(SeaTunnelRow row) {
        try {
            Map<String, String> source = new HashMap<>();
            if (!StringUtils.isEmpty(row.getTableId())) {
                source.put("schema", TablePath.of(row.getTableId()).getSchemaName());
                source.put("database", TablePath.of(row.getTableId()).getDatabaseName());
                source.put("table", TablePath.of(row.getTableId()).getTableName());
            }
            switch (row.getRowKind()) {
                case INSERT:
                case UPDATE_AFTER:
                    if (mergeUpdateEventFlag && row.getRowKind().equals(RowKind.UPDATE_AFTER)) {
                        genericRow.setField(0, cacheUpdateBeforeRow);
                        genericRow.setField(2, OP_UPDATE);
                    } else {
                        genericRow.setField(0, null);
                        genericRow.setField(2, OP_INSERT);
                    }
                    genericRow.setField(1, row);
                    genericRow.setField(3, source);

                    if (row.getOptions() != null
                            && row.getOptions().containsKey(EVENT_TIME.getName())) {
                        genericRow.setField(4, row.getOptions().get(EVENT_TIME.getName()));
                    } else {
                        genericRow.setField(4, null);
                    }
                    return jsonSerializer.serialize(genericRow);
                case UPDATE_BEFORE:
                    if (mergeUpdateEventFlag) {
                        cacheUpdateBeforeRow = row;
                        return null;
                    }
                case DELETE:
                    genericRow.setField(0, row);
                    genericRow.setField(1, null);
                    genericRow.setField(2, OP_DELETE);
                    genericRow.setField(3, source);
                    if (row.getOptions() != null
                            && row.getOptions().containsKey(EVENT_TIME.getName())) {
                        genericRow.setField(4, row.getOptions().get(EVENT_TIME.getName()));
                    }
                    return jsonSerializer.serialize(genericRow);
                default:
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Unsupported operation '%s' for row kind.", row.getRowKind()));
            }
        } catch (Throwable t) {
            throw CommonError.jsonOperationError(FORMAT, row.toString(), t);
        }
    }

    private static SeaTunnelRowType createJsonRowType(SeaTunnelRowType databaseSchema) {
        return new SeaTunnelRowType(
                new String[] {"before", "after", "op", "source", "ts_ms"},
                new SeaTunnelDataType[] {
                    databaseSchema,
                    databaseSchema,
                    STRING_TYPE,
                    new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE),
                    LONG_TYPE
                });
    }
}
