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

import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.format.json.JsonSerializationSchema;
import org.apache.seatunnel.format.json.exception.SeaTunnelJsonFormatException;

import static org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE;
import static org.apache.seatunnel.format.json.debezium.DebeziumJsonFormatOptions.GENERATE_ROW_SIZE;

public class DebeziumJsonSerializationSchema implements SerializationSchema {
    private static final long serialVersionUID = 1L;

    private static final String OP_INSERT = "c"; // insert
    private static final String OP_DELETE = "d"; // delete

    private final JsonSerializationSchema jsonSerializer;

    private transient SeaTunnelRow genericRow;

    public DebeziumJsonSerializationSchema(SeaTunnelRowType rowType) {
        this.jsonSerializer = new JsonSerializationSchema(createJsonRowType(rowType));
        this.genericRow = new SeaTunnelRow(GENERATE_ROW_SIZE);
    }

    @Override
    public byte[] serialize(SeaTunnelRow row) {
        try {
            switch (row.getRowKind()) {
                case INSERT:
                case UPDATE_AFTER:
                    genericRow.setField(0, null);
                    genericRow.setField(1, row);
                    genericRow.setField(2, OP_INSERT);
                    return jsonSerializer.serialize(genericRow);
                case UPDATE_BEFORE:
                case DELETE:
                    genericRow.setField(0, row);
                    genericRow.setField(1, null);
                    genericRow.setField(2, OP_DELETE);
                    return jsonSerializer.serialize(genericRow);
                default:
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Unsupported operation '%s' for row kind.", row.getRowKind()));
            }
        } catch (Throwable t) {
            throw new SeaTunnelJsonFormatException(
                    CommonErrorCode.JSON_OPERATION_FAILED,
                    String.format("Could not serialize row %s.", row),
                    t);
        }
    }

    private static SeaTunnelRowType createJsonRowType(SeaTunnelRowType databaseSchema) {
        return new SeaTunnelRowType(
                new String[] {"before", "after", "op"},
                new SeaTunnelDataType[] {databaseSchema, databaseSchema, STRING_TYPE});
    }
}
