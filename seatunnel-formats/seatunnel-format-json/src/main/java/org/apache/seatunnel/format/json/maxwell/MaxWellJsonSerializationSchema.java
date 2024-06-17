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

import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.format.json.JsonSerializationSchema;
import org.apache.seatunnel.format.json.exception.SeaTunnelJsonFormatException;

import static org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE;

public class MaxWellJsonSerializationSchema implements SerializationSchema {

    private static final long serialVersionUID = 1L;

    private static final String OP_INSERT = "INSERT";
    private static final String OP_DELETE = "DELETE";

    public static final String FORMAT = "MAXWELL";

    private transient SeaTunnelRow reuse;

    private final JsonSerializationSchema jsonSerializer;

    public MaxWellJsonSerializationSchema(SeaTunnelRowType rowType) {
        this.jsonSerializer = new JsonSerializationSchema(createJsonRowType(rowType));
        this.reuse = new SeaTunnelRow(2);
    }

    @Override
    public byte[] serialize(SeaTunnelRow row) {
        try {
            String opType = rowKind2String(row.getRowKind());
            reuse.setField(0, row);
            reuse.setField(1, opType);
            return jsonSerializer.serialize(reuse);
        } catch (Throwable t) {
            throw CommonError.jsonOperationError(FORMAT, row.toString(), t);
        }
    }

    private String rowKind2String(RowKind rowKind) {
        switch (rowKind) {
            case INSERT:
            case UPDATE_AFTER:
                return OP_INSERT;
            case UPDATE_BEFORE:
            case DELETE:
                return OP_DELETE;
            default:
                throw new SeaTunnelJsonFormatException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                        String.format("Unsupported operation %s for row kind.", rowKind));
        }
    }

    private static SeaTunnelRowType createJsonRowType(SeaTunnelRowType databaseSchema) {
        // MaxWell JSON contains other information, e.g. "database", "ts"
        // but we don't need them
        // and we don't need "old" , because can not support UPDATE_BEFORE,UPDATE_AFTER
        return new SeaTunnelRowType(
                new String[] {"data", "type"},
                new SeaTunnelDataType[] {databaseSchema, STRING_TYPE});
    }
}
