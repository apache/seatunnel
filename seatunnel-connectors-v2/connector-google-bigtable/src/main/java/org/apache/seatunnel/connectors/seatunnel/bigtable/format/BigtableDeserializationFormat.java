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

package org.apache.seatunnel.connectors.seatunnel.bigtable.format;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.DateUtils;
import org.apache.seatunnel.common.utils.TimeUtils;
import org.apache.seatunnel.connectors.seatunnel.bigtable.exception.BigtableConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.bigtable.exception.BigtableConnectorException;

import com.google.protobuf.ByteString;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

/**
 * Deserializes raw {@link ByteString} cell values returned by Bigtable into typed {@link
 * SeaTunnelRow} fields.
 *
 * <p>Numeric types are stored as big-endian binary; string-like types (DATE, TIME, TIMESTAMP,
 * DECIMAL, STRING) are stored as UTF-8 text, matching the encoding used by {@code
 * BigtableSinkWriter}.
 */
public class BigtableDeserializationFormat {

    private final DateUtils.Formatter dateFormat = DateUtils.Formatter.YYYY_MM_DD;
    private final DateTimeUtils.Formatter datetimeFormat =
            DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS;
    private final TimeUtils.Formatter timeFormat = TimeUtils.Formatter.HH_MM_SS;

    /**
     * Deserializes an array of raw cell bytes into a {@link SeaTunnelRow}.
     *
     * @param rawCells one entry per field in {@code rowType}; may be {@code null} for absent cells
     * @param rowType the target row schema
     * @return the deserialized row
     */
    public SeaTunnelRow deserialize(ByteString[] rawCells, SeaTunnelRowType rowType) {
        SeaTunnelRow row = new SeaTunnelRow(rowType.getTotalFields());
        for (int i = 0; i < rowType.getTotalFields(); i++) {
            SeaTunnelDataType<?> fieldType = rowType.getFieldType(i);
            row.setField(i, deserializeCell(fieldType, rawCells[i]));
        }
        return row;
    }

    private Object deserializeCell(SeaTunnelDataType<?> fieldType, ByteString cell) {
        if (cell == null || cell.isEmpty()) {
            return null;
        }
        byte[] bytes = cell.toByteArray();
        switch (fieldType.getSqlType()) {
            case TINYINT:
                return bytes[0];
            case SMALLINT:
                return (short) ((bytes[0] & 0xFF) << 8 | (bytes[1] & 0xFF));
            case INT:
                return ByteBuffer.wrap(bytes).getInt();
            case BIGINT:
                return ByteBuffer.wrap(bytes).getLong();
            case FLOAT:
                return ByteBuffer.wrap(bytes).getFloat();
            case DOUBLE:
                return ByteBuffer.wrap(bytes).getDouble();
            case BOOLEAN:
                return bytes[0] != 0;
            case BYTES:
                return bytes;
            case DECIMAL:
                String decStr = new String(bytes, StandardCharsets.UTF_8);
                try {
                    return new BigDecimal(decStr);
                } catch (NumberFormatException e) {
                    return new BigDecimal(ByteBuffer.wrap(bytes).getFloat());
                }
            case DATE:
                return LocalDate.parse(
                        new String(bytes, StandardCharsets.UTF_8),
                        DateTimeFormatter.ofPattern(dateFormat.getValue()));
            case TIME:
                return LocalTime.parse(
                        new String(bytes, StandardCharsets.UTF_8),
                        DateTimeFormatter.ofPattern(timeFormat.getValue()));
            case TIMESTAMP:
                return LocalDateTime.parse(
                        new String(bytes, StandardCharsets.UTF_8),
                        DateTimeFormatter.ofPattern(datetimeFormat.getValue()));
            case STRING:
                return new String(bytes, StandardCharsets.UTF_8);
            default:
                throw new BigtableConnectorException(
                        BigtableConnectorErrorCode.READ_FAILED,
                        "Unsupported data type: " + fieldType.getSqlType());
        }
    }
}
