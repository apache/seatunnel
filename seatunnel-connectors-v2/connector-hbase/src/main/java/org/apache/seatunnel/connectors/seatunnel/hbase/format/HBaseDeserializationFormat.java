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
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.DateUtils;
import org.apache.seatunnel.common.utils.TimeUtils;
import org.apache.seatunnel.connectors.seatunnel.hbase.exception.HbaseConnectorException;

import org.apache.hadoop.hbase.util.Bytes;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

import static org.apache.seatunnel.common.utils.DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS;

public class HBaseDeserializationFormat {

    private final DateUtils.Formatter dateFormat = DateUtils.Formatter.YYYY_MM_DD;
    private final DateTimeUtils.Formatter datetimeFormat = YYYY_MM_DD_HH_MM_SS;
    private final TimeUtils.Formatter timeFormat = TimeUtils.Formatter.HH_MM_SS;

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
                return cell[0];
            case SMALLINT:
                return (short) ((cell[0] & 0xFF) << 8 | (cell[1] & 0xFF));
            case INT:
                return Bytes.toInt(cell);
            case BOOLEAN:
                return Bytes.toBoolean(cell);
            case BIGINT:
                return Bytes.toLong(cell);
            case FLOAT:
            case DECIMAL:
                return Bytes.toFloat(cell);
            case DOUBLE:
                return Bytes.toDouble(cell);
            case BYTES:
                return cell;
            case DATE:
                return LocalDate.parse(
                        Bytes.toString(cell), DateTimeFormatter.ofPattern(dateFormat.getValue()));
            case TIME:
                return LocalTime.parse(
                        Bytes.toString(cell), DateTimeFormatter.ofPattern(timeFormat.getValue()));
            case TIMESTAMP:
                return LocalDateTime.parse(
                        Bytes.toString(cell),
                        DateTimeFormatter.ofPattern(datetimeFormat.getValue()));
            case STRING:
                return Bytes.toString(cell);
            default:
                throw new HbaseConnectorException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                        "Unsupported data type " + typeInfo.getSqlType());
        }
    }
}
