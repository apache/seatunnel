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

package org.apache.seatunnel.translation.flink.serialization;

import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.translation.serialization.RowConverter;

import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * The row converter between {@link Row} and {@link SeaTunnelRow}, used to convert or reconvert
 * between flink row and seatunnel row
 */
public class FlinkRowConverter extends RowConverter<Row> {

    public FlinkRowConverter(SeaTunnelDataType<?> dataType) {
        super(dataType);
    }

    @Override
    public Row convert(SeaTunnelRow seaTunnelRow) throws IOException {
        validate(seaTunnelRow);
        return (Row) convert(seaTunnelRow, dataType);
    }

    private static Object convert(Object field, SeaTunnelDataType<?> dataType) {
        if (field == null) {
            return null;
        }
        SqlType sqlType = dataType.getSqlType();
        switch (sqlType) {
            case ROW:
                SeaTunnelRow seaTunnelRow = (SeaTunnelRow) field;
                SeaTunnelRowType rowType = (SeaTunnelRowType) dataType;
                int arity = rowType.getTotalFields();
                Row engineRow = new Row(arity);
                for (int i = 0; i < arity; i++) {
                    engineRow.setField(
                            i, convert(seaTunnelRow.getField(i), rowType.getFieldType(i)));
                }
                engineRow.setKind(RowKind.fromByteValue(seaTunnelRow.getRowKind().toByteValue()));
                return engineRow;
            case MAP:
                return convertMap(
                        (Map<?, ?>) field, (MapType<?, ?>) dataType, FlinkRowConverter::convert);
            default:
                return field;
        }
    }

    private static Object convertMap(
            Map<?, ?> mapData,
            MapType<?, ?> mapType,
            BiFunction<Object, SeaTunnelDataType<?>, Object> convertFunction) {
        if (mapData == null || mapData.size() == 0) {
            return mapData;
        }
        switch (mapType.getValueType().getSqlType()) {
            case MAP:
            case ROW:
                Map<Object, Object> newMap = new HashMap<>(mapData.size());
                mapData.forEach(
                        (key, value) -> {
                            SeaTunnelDataType<?> valueType = mapType.getValueType();
                            newMap.put(key, convertFunction.apply(value, valueType));
                        });
                return newMap;
            default:
                return mapData;
        }
    }

    @Override
    public SeaTunnelRow reconvert(Row engineRow) throws IOException {
        return (SeaTunnelRow) reconvert(engineRow, dataType);
    }

    private static Object reconvert(Object field, SeaTunnelDataType<?> dataType) {
        if (field == null) {
            return null;
        }
        SqlType sqlType = dataType.getSqlType();
        switch (sqlType) {
            case ROW:
                Row engineRow = (Row) field;
                SeaTunnelRowType rowType = (SeaTunnelRowType) dataType;
                int arity = rowType.getTotalFields();
                SeaTunnelRow seaTunnelRow = new SeaTunnelRow(arity);
                for (int i = 0; i < arity; i++) {
                    seaTunnelRow.setField(
                            i, reconvert(engineRow.getField(i), rowType.getFieldType(i)));
                }
                seaTunnelRow.setRowKind(
                        org.apache.seatunnel.api.table.type.RowKind.fromByteValue(
                                engineRow.getKind().toByteValue()));
                return seaTunnelRow;
            case MAP:
                return convertMap(
                        (Map<?, ?>) field, (MapType<?, ?>) dataType, FlinkRowConverter::reconvert);
            default:
                return field;
        }
    }
}
