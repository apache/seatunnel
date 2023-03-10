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

package org.apache.seatunnel.connectors.seatunnel.fts.utils;

import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.fts.exception.FlinkTableStoreConnectorException;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;

import java.time.LocalDate;

/** The converter for converting {@link RowData} and {@link SeaTunnelRow} */
public class RowConverter {

    private RowConverter() {}

    public static SeaTunnelRow convert(RowData rowData, SeaTunnelRowType seaTunnelRowType) {
        Object[] objects = new Object[seaTunnelRowType.getTotalFields()];
        for (int i = 0; i < objects.length; i++) {
            SeaTunnelDataType<?> fieldType = seaTunnelRowType.getFieldType(i);
            switch (fieldType.getSqlType()) {
                case TINYINT:
                    objects[i] = rowData.getByte(i);
                    break;
                case SMALLINT:
                    objects[i] = rowData.getShort(i);
                    break;
                case INT:
                    objects[i] = rowData.getInt(i);
                    break;
                case BIGINT:
                    objects[i] = rowData.getLong(i);
                    break;
                case FLOAT:
                    objects[i] = rowData.getFloat(i);
                    break;
                case DOUBLE:
                    objects[i] = rowData.getDouble(i);
                    break;
                case DECIMAL:
                    SeaTunnelDataType<?> decimalType = seaTunnelRowType.getFieldType(i);
                    objects[i] =
                            rowData.getDecimal(
                                    i,
                                    ((DecimalType) decimalType).getPrecision(),
                                    ((DecimalType) decimalType).getScale());
                    break;
                case STRING:
                    objects[i] = rowData.getString(i).toString();
                    break;
                case BOOLEAN:
                    objects[i] = rowData.getBoolean(i);
                    break;
                case BYTES:
                    objects[i] = rowData.getBinary(i);
                    break;
                case DATE:
                    objects[i] = LocalDate.ofEpochDay(rowData.getInt(i));
                    break;
                case TIMESTAMP:
                    // Now SeaTunnel not supported assigned the timezone for timestamp,
                    // so we use the default precision 6
                    TimestampData timestamp = rowData.getTimestamp(i, 6);
                    objects[i] = timestamp.toLocalDateTime();
                    break;
                case ARRAY:
                    SeaTunnelDataType<?> arrayType = seaTunnelRowType.getFieldType(i);
                    ArrayData array = rowData.getArray(i);
                    switch (arrayType.getSqlType()) {
                        case STRING:
                            String[] strings = new String[array.size()];
                            for (int j = 0; j < strings.length; j++) {
                                strings[j] = array.getString(j).toString();
                            }
                            objects[i] = strings;
                            break;
                        case BOOLEAN:
                            Boolean[] booleans = new Boolean[array.size()];
                            for (int j = 0; j < booleans.length; j++) {
                                booleans[j] = array.getBoolean(j);
                            }
                            objects[i] = booleans;
                            break;
                        case TINYINT:
                            Byte[] bytes = new Byte[array.size()];
                            for (int j = 0; j < bytes.length; j++) {
                                bytes[j] = array.getByte(j);
                            }
                            objects[i] = bytes;
                            break;
                        case SMALLINT:
                            Short[] shorts = new Short[array.size()];
                            for (int j = 0; j < shorts.length; j++) {
                                shorts[j] = array.getShort(j);
                            }
                            objects[i] = shorts;
                            break;
                        case INT:
                            Integer[] integers = new Integer[array.size()];
                            for (int j = 0; j < integers.length; j++) {
                                integers[j] = array.getInt(j);
                            }
                            objects[i] = integers;
                            break;
                        case BIGINT:
                            Long[] longs = new Long[array.size()];
                            for (int j = 0; j < longs.length; j++) {
                                longs[j] = array.getLong(j);
                            }
                            objects[i] = longs;
                            break;
                        case FLOAT:
                            Float[] floats = new Float[array.size()];
                            for (int j = 0; j < floats.length; j++) {
                                floats[j] = array.getFloat(j);
                            }
                            objects[i] = floats;
                            break;
                        case DOUBLE:
                            Double[] doubles = new Double[array.size()];
                            for (int j = 0; j < doubles.length; j++) {
                                doubles[j] = array.getDouble(j);
                            }
                            objects[i] = doubles;
                            break;
                        default:
                            String errorMsg =
                                    String.format(
                                            "Array type not support this genericType [%s]",
                                            arrayType);
                            throw new FlinkTableStoreConnectorException(
                                    CommonErrorCode.UNSUPPORTED_DATA_TYPE, errorMsg);
                    }
                    break;
                case MAP:
                    SeaTunnelDataType<?> mapType = seaTunnelRowType.getFieldType(i);
                    MapData map = rowData.getMap(i);
                    break;
                case ROW:
                    SeaTunnelDataType<?> rowType = seaTunnelRowType.getFieldType(i);
                    RowData row = rowData.getRow(i, ((SeaTunnelRowType) rowType).getTotalFields());
                    break;
                default:
                    throw new FlinkTableStoreConnectorException(
                            CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                            "SeaTunnel does not support this type");
            }
        }
        return new SeaTunnelRow(objects);
    }

    public static RowData convert(SeaTunnelRow seaTunnelRow, SeaTunnelRowType seaTunnelRowType) {
        // TODO implementation
        return null;
    }
}
