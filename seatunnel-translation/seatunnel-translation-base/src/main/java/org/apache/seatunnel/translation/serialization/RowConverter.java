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

package org.apache.seatunnel.translation.serialization;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Conversion between {@link SeaTunnelRow} & engine's row.
 *
 * @param <T> engine row
 */
@Slf4j
public abstract class RowConverter<T> implements Serializable {
    protected final SeaTunnelDataType<?> dataType;

    public RowConverter(SeaTunnelDataType<?> dataType) {
        this.dataType = dataType;
    }

    public void validate(SeaTunnelRow seaTunnelRow) throws IOException {
        if (!(dataType instanceof SeaTunnelRowType)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The data type don't support validation: %s. ",
                            dataType.getClass().getSimpleName()));
        }
        SeaTunnelDataType<?>[] fieldTypes = ((SeaTunnelRowType) dataType).getFieldTypes();
        List<String> errors = new ArrayList<>();
        Object field;
        SeaTunnelDataType<?> fieldType;
        for (int i = 0; i < fieldTypes.length; i++) {
            field = seaTunnelRow.getField(i);
            fieldType = fieldTypes[i];
            if (!validate(field, fieldType)) {
                errors.add(
                        String.format(
                                "The SQL type '%s' don't support '%s', the class of the expected data type is '%s'.",
                                fieldType.getSqlType(),
                                field.getClass(),
                                fieldType.getTypeClass()));
            }
        }
        if (!errors.isEmpty()) {
            throw new UnsupportedOperationException(String.join(",", errors));
        }
    }

    protected boolean validate(Object field, SeaTunnelDataType<?> dataType) {
        if (field == null || dataType.getSqlType() == SqlType.NULL) {
            return true;
        }
        SqlType sqlType = dataType.getSqlType();
        switch (sqlType) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case DATE:
            case TIME:
            case TIMESTAMP:
            case TIMESTAMP_TZ:
            case FLOAT:
            case DOUBLE:
            case STRING:
            case DECIMAL:
            case BYTES:
                boolean isEq = (dataType.getTypeClass() == field.getClass());
                if (!isEq) {
                    log.error(
                            String.format(
                                    "dateType.getTypeClass is %s, but field.getClass is %s",
                                    dataType.getTypeClass(), field.getClass()));
                }
                return isEq;
            case ARRAY:
                if (!(field instanceof Object[])) {
                    return false;
                }
                ArrayType<?, ?> arrayType = (ArrayType<?, ?>) dataType;
                Object[] arrayField = (Object[]) field;
                if (arrayField.length == 0) {
                    return true;
                } else {
                    return validate(arrayField[0], arrayType.getElementType());
                }
            case MAP:
                if (!(field instanceof Map)) {
                    log.error(
                            String.format(
                                    "field type is %s, not instanceof java.util.Map",
                                    field.getClass()));
                    return false;
                }
                MapType<?, ?> mapType = (MapType<?, ?>) dataType;
                Map<?, ?> mapField = (Map<?, ?>) field;
                if (mapField.isEmpty()) {
                    return true;
                } else {
                    Map.Entry<?, ?> entry = mapField.entrySet().stream().findFirst().get();
                    Object key = entry.getKey();
                    if (key instanceof scala.Some) {
                        key = ((scala.Some<?>) key).get();
                    }
                    Object value = entry.getValue();
                    if (value instanceof scala.Some) {
                        value = ((scala.Some<?>) value).get();
                    }
                    return validate(key, mapType.getKeyType())
                            && validate(value, mapType.getValueType());
                }
            case ROW:
                if (!(field instanceof SeaTunnelRow)) {
                    return false;
                }
                SeaTunnelDataType<?>[] fieldTypes = ((SeaTunnelRowType) dataType).getFieldTypes();
                SeaTunnelRow seaTunnelRow = (SeaTunnelRow) field;
                for (int i = 0; i < fieldTypes.length; i++) {
                    if (!validate(seaTunnelRow.getField(i), fieldTypes[i])) {
                        return false;
                    }
                }
                return true;
            default:
                return false;
        }
    }

    /**
     * Convert {@link SeaTunnelRow} to engine's row.
     *
     * @throws IOException Thrown, if the conversion fails.
     */
    public abstract T convert(SeaTunnelRow seaTunnelRow) throws IOException;

    /**
     * Convert engine's row to {@link SeaTunnelRow}.
     *
     * @throws IOException Thrown, if the conversion fails.
     */
    public abstract SeaTunnelRow reconvert(T engineRow) throws IOException;
}
