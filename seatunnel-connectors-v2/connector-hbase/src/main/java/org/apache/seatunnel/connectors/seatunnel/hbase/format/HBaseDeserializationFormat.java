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

package org.apache.seatunnel.connectors.seatunnel.hbase.format;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.hbase.exception.HbaseConnectorException;

import org.apache.hadoop.hbase.util.Bytes;

import java.nio.charset.Charset;

public class HBaseDeserializationFormat {

    public SeaTunnelRow deserialize(byte[][] rowCell, SeaTunnelRowType seaTunnelRowType) {
        SeaTunnelRow row = new SeaTunnelRow(seaTunnelRowType.getTotalFields());
        for (int i = 0; i < row.getArity(); i++) {
            SeaTunnelDataType<?> fieldType = seaTunnelRowType.getFieldType(i);
            row.setField(i, deserializeValue(fieldType, rowCell[i]));
        }
        return row;
    }

    private Object deserializeValue(SeaTunnelDataType<?> typeInfo, byte[] cell) {
        if (cell == null) {
            return null;
        }

        switch (typeInfo.getSqlType()) {
            case TINYINT:
            case SMALLINT:
            case INT:
                return Integer.valueOf(Bytes.toString(cell));
            case BOOLEAN:
                return Boolean.valueOf(Bytes.toString(cell));
            case BIGINT:
                return Long.valueOf(Bytes.toString(cell));
            case FLOAT:
            case DECIMAL:
                return Double.valueOf(Bytes.toString(cell));
            case BYTES:
                return cell;
            case DATE:
            case TIME:
            case TIMESTAMP:
            case STRING:
                return new String(cell, Charset.defaultCharset());
            default:
                throw new HbaseConnectorException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                        "Unsupported data type " + typeInfo.getSqlType());
        }
    }
}
