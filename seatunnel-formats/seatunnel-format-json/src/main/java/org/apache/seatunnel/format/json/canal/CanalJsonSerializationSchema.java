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

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.format.json.JsonSerializationSchema;
import org.apache.seatunnel.format.json.exception.SeaTunnelJsonFormatException;

import java.nio.charset.Charset;

import static org.apache.seatunnel.api.table.type.BasicType.LONG_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE;
import static org.apache.seatunnel.api.table.type.CommonOptions.EVENT_TIME;

public class CanalJsonSerializationSchema implements SerializationSchema {

    private static final long serialVersionUID = 1L;

    private static final String FORMAT = "Canal";

    private static final String OP_INSERT = "INSERT";
    private static final String OP_DELETE = "DELETE";
    private static final String OP_UPDATE = "UPDATE";

    private transient SeaTunnelRow reuse;

    private final JsonSerializationSchema jsonSerializer;

    boolean mergeUpdateEventFlag;
    SeaTunnelRow cacheUpdateBeforeRow;

    public CanalJsonSerializationSchema(SeaTunnelRowType rowType) {
        this.jsonSerializer = new JsonSerializationSchema(createJsonRowType(rowType));
        this.reuse = new SeaTunnelRow(6);
        mergeUpdateEventFlag = false;
    }

    public CanalJsonSerializationSchema(
            SeaTunnelRowType rowType, Charset charset, boolean mergeUpdateEventFlag) {
        this.jsonSerializer = new JsonSerializationSchema(createJsonRowType(rowType), charset);
        this.reuse = new SeaTunnelRow(6);
        this.mergeUpdateEventFlag = mergeUpdateEventFlag;
    }

    @Override
    public byte[] serialize(SeaTunnelRow row) {
        try {
            if (mergeUpdateEventFlag && row.getRowKind() == RowKind.UPDATE_BEFORE) {
                cacheUpdateBeforeRow = row;
                return null;
            }

            if (mergeUpdateEventFlag && row.getRowKind() == RowKind.UPDATE_AFTER) {
                reuse.setField(0, new SeaTunnelRow[] {cacheUpdateBeforeRow});
            } else {
                reuse.setField(0, null);
            }

            reuse.setField(1, new SeaTunnelRow[] {row});
            reuse.setField(2, rowKind2String(row.getRowKind()));

            if (!StringUtils.isEmpty(row.getTableId())) {
                reuse.setField(3, TablePath.of(row.getTableId()).getDatabaseName());
                reuse.setField(4, TablePath.of(row.getTableId()).getTableName());
            }

            if (row.getOptions() != null && row.getOptions().containsKey(EVENT_TIME.getName())) {
                reuse.setField(5, row.getOptions().get(EVENT_TIME.getName()));
            }

            return jsonSerializer.serialize(reuse);
        } catch (Throwable t) {
            throw CommonError.jsonOperationError(FORMAT, row.toString(), t);
        }
    }

    private String rowKind2String(RowKind rowKind) {
        switch (rowKind) {
            case INSERT:
            case UPDATE_AFTER:
                if (mergeUpdateEventFlag && rowKind.equals(RowKind.UPDATE_AFTER)) {
                    return OP_UPDATE;
                }
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
        return new SeaTunnelRowType(
                new String[] {"old", "data", "type", "database", "table", "ts"},
                new SeaTunnelDataType[] {
                    new ArrayType<>(SeaTunnelRowType[].class, databaseSchema),
                    new ArrayType<>(SeaTunnelRowType[].class, databaseSchema),
                    STRING_TYPE,
                    STRING_TYPE,
                    STRING_TYPE,
                    LONG_TYPE
                });
    }
}
