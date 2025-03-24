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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.catalog;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeConverter;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseType;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AutoService(TypeConverter.class)
public class ClickhouseTypeConverter
        implements BasicTypeConverter<BasicTypeDefine<ClickhouseType>> {
    public static final ClickhouseTypeConverter INSTANCE = new ClickhouseTypeConverter();
    public static final Integer MAX_DATETIME_SCALE = 9;
    public static final String IDENTIFIER = "Clickhouse";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Column convert(BasicTypeDefine<ClickhouseType> typeDefine) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public BasicTypeDefine<ClickhouseType> reconvert(Column column) {
        BasicTypeDefine.BasicTypeDefineBuilder builder =
                BasicTypeDefine.builder()
                        .name(column.getName())
                        .nullable(column.isNullable())
                        .comment(column.getComment())
                        .defaultValue(column.getDefaultValue());

        switch (column.getDataType().getSqlType()) {
            case BOOLEAN:
                builder.columnType(ClickhouseType.BOOLEAN);
                builder.dataType(ClickhouseType.BOOLEAN);
                break;
            case TINYINT:
                builder.columnType(ClickhouseType.TINYINT);
                builder.dataType(ClickhouseType.TINYINT);
                break;
            case SMALLINT:
                builder.columnType(ClickhouseType.SMALLINT);
                builder.dataType(ClickhouseType.SMALLINT);
                break;
            case INT:
                builder.columnType(ClickhouseType.INT);
                builder.dataType(ClickhouseType.INT);
                break;
            case BIGINT:
                builder.columnType(ClickhouseType.BIGINT);
                builder.dataType(ClickhouseType.BIGINT);
                break;
            case FLOAT:
                builder.columnType(ClickhouseType.FLOAT);
                builder.dataType(ClickhouseType.FLOAT);
                break;
            case DOUBLE:
                builder.columnType(ClickhouseType.DOUBLE);
                builder.dataType(ClickhouseType.DOUBLE);
                break;
            case DATE:
                builder.columnType(ClickhouseType.DATE);
                builder.dataType(ClickhouseType.DATE);
                break;
            case TIME:
            case STRING:
                builder.columnType(ClickhouseType.STRING);
                builder.dataType(ClickhouseType.STRING);
                break;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) column.getDataType();
                builder.columnType(
                        String.format(
                                "%s(%s, %s)",
                                ClickhouseType.DECIMAL,
                                decimalType.getPrecision(),
                                decimalType.getScale()));
                builder.dataType(ClickhouseType.DECIMAL);
                break;
            case TIMESTAMP:
                if (column.getScale() != null
                        && column.getScale() > 0
                        && column.getScale() <= MAX_DATETIME_SCALE) {
                    builder.columnType(
                            String.format("%s(%s)", ClickhouseType.DateTime64, column.getScale()));
                    builder.scale(column.getScale());
                } else {
                    builder.columnType(String.format("%s(%s)", ClickhouseType.DateTime64, 0));
                    builder.scale(0);
                }
                builder.dataType(ClickhouseType.DateTime64);
                break;
            case MAP:
                MapType dataType = (MapType) column.getDataType();
                SeaTunnelDataType keyType = dataType.getKeyType();
                SeaTunnelDataType valueType = dataType.getValueType();
                Column keyColumn =
                        PhysicalColumn.of(
                                column.getName() + ".key",
                                (SeaTunnelDataType<?>) keyType,
                                (Long) null,
                                true,
                                null,
                                null);
                String keyColumnType = reconvert(keyColumn).getColumnType();
                Column valueColumn =
                        PhysicalColumn.of(
                                column.getName() + ".value",
                                (SeaTunnelDataType<?>) valueType,
                                (Long) null,
                                true,
                                null,
                                null);
                String valueColumnType = reconvert(valueColumn).getColumnType();

                builder.dataType(ClickhouseType.MAP);
                builder.columnType(
                        String.format(
                                "%s(%s, %s)", ClickhouseType.MAP, keyColumnType, valueColumnType));
                break;
            case ARRAY:
                SeaTunnelDataType<?> arrayDataType = column.getDataType();
                SeaTunnelDataType elementType = null;
                if (arrayDataType instanceof ArrayType) {
                    ArrayType arrayType = (ArrayType) arrayDataType;
                    elementType = arrayType.getElementType();
                }

                Column arrayKeyColumn =
                        PhysicalColumn.of(
                                column.getName() + ".key",
                                (SeaTunnelDataType<?>) elementType,
                                (Long) null,
                                true,
                                null,
                                null);
                String arrayKeyColumnType = reconvert(arrayKeyColumn).getColumnType();
                builder.dataType(ClickhouseType.ARRAY);
                builder.columnType(
                        String.format("%s(%s)", ClickhouseType.ARRAY, arrayKeyColumnType));
                break;
            default:
                throw CommonError.convertToConnectorTypeError(
                        IDENTIFIER, column.getDataType().getSqlType().name(), column.getName());
        }
        return builder.build();
    }
}
